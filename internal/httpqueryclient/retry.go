package httpqueryclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbanalytics/internal/leakcheck"
)

// retryableRequestOptions holds the common configuration for a retryable HTTP request.
type retryableRequestOptions struct {
	method      string
	path        string
	body        []byte
	header      http.Header
	authHandler func(req *http.Request)
	maxRetries  uint32

	// statement and payload are used for query-specific retry logic (timeout recalculation).
	// They can be left empty for non-query requests.
	statement      string
	payload        map[string]interface{}
	serverDeadline time.Time
}

// retryState tracks state across retry iterations.
type retryState struct {
	lastCode    uint32
	lastMessage string
	lastRootErr error
	retries     uint32
	uniqueID    string
	backoff     backoffCalculator
	addrs       []string
	body        []byte
}

// retryAction represents what the response handler wants to do.
type retryAction int

const (
	retryActionReturn retryAction = iota
	retryActionRetry
)

// retryableResponseHandler is called when a successful HTTP response is received (no transport error).
// It decides whether to return a result, return an error, or retry.
type retryableResponseHandler[T any] func(resp *http.Response, state *retryState) (*T, retryAction, error)

// doWithRetries performs an HTTP request with retry logic including exponential backoff,
// DNS resolution, and connection error handling.
func doWithRetries[T any](
	ctx context.Context,
	c *Client,
	opts *retryableRequestOptions,
	handler retryableResponseHandler[T],
) (*T, error) {
	addrs, err := c.resolver.LookupHost(ctx, c.host)
	if err != nil {
		return nil, newAnalyticsError(fmt.Errorf("failed to lookup host: %w", err), opts.statement, c.host, 0, 0)
	}

	state := &retryState{
		lastCode:    0,
		lastMessage: "",
		lastRootErr: nil,
		retries:     0,
		uniqueID:    uuid.NewString(),
		backoff:     analyticsExponentialBackoffWithJitter(100*time.Millisecond, 1*time.Minute, 2),
		addrs:       addrs,
		body:        opts.body,
	}

	for {
		// We use > here as this check is at the top of the loop, so we want to allow the nth retry to be made.
		if state.retries > opts.maxRetries {
			// This could be a query error or a platform error, either way we don't wrap it in an analytics error.
			return nil, state.lastRootErr
		}

		idx := rand.Intn(len(state.addrs)) //nolint:gosec
		addr := state.addrs[idx]

		reqURI := fmt.Sprintf("%s://%s:%d%s", c.scheme, addr, c.port, opts.path)

		var connectDoneErr error

		trace := &httptrace.ClientTrace{ //nolint:exhaustruct
			ConnectDone: func(_, _ string, err error) {
				connectDoneErr = err
			},
		}

		var reqBody io.Reader
		if state.body != nil {
			reqBody = io.NopCloser(newResettableReader(state.body))
		}

		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), opts.method, reqURI, reqBody)
		if err != nil {
			return nil, newObfuscateErrorWrapper("failed to create http request", err)
		}

		req.Host = c.host

		if opts.header != nil {
			req.Header = opts.header
		}

		if opts.authHandler != nil {
			opts.authHandler(req)
		}

		c.logger.Trace("Sending request %s to %s", state.uniqueID, reqURI)

		resp, err := c.innerClient.Do(req)
		if err != nil {
			c.logger.Trace("Received HTTP Response for ID=%s, errored: %v", state.uniqueID, err)

			// We don't want to bail out on connection errors as they may be because of dial timeout.
			if connectDoneErr == nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil, newAnalyticsError(err, opts.statement, c.host, 0, state.retries).
						withLastDetail(state.lastCode, state.lastMessage)
				}
			}

			newBody, notRetriableErr := c.handleMaybeRetry(ctx, state.uniqueID, opts.serverDeadline,
				state.backoff, state.retries, opts.payload)
			if notRetriableErr != nil {
				return nil, newAnalyticsError(notRetriableErr, opts.statement, c.host, 0, state.retries).
					withLastDetail(state.lastCode, state.lastMessage)
			}

			if connectDoneErr == nil {
				state.lastRootErr = newObfuscateErrorWrapper("failed to send request", err)
			} else {
				state.lastRootErr = connectDoneErr
			}

			if newBody != nil {
				state.body = newBody
			}

			state.retries++

			continue
		}

		c.logger.Trace("Received HTTP Response for ID=%s, status=%d", state.uniqueID, resp.StatusCode)

		resp = leakcheck.WrapHTTPResponse(resp) //nolint:bodyclose

		result, action, handlerErr := handler(resp, state)
		if action == retryActionRetry {
			newBody, retryErr := c.handleMaybeRetry(ctx, state.uniqueID, opts.serverDeadline,
				state.backoff, state.retries, opts.payload)
			if retryErr != nil {
				// The handler error takes priority when the retry is denied.
				if handlerErr != nil {
					return nil, handlerErr
				}

				return nil, newAnalyticsError(retryErr, opts.statement, c.host, resp.StatusCode, state.retries).
					withLastDetail(state.lastCode, state.lastMessage)
			}

			if newBody != nil {
				state.body = newBody
			}

			state.retries++

			continue
		}

		return result, handlerErr
	}
}

// newResettableReader creates a new reader from a byte slice. This is useful so that
// the request body can be re-read on retries.
func newResettableReader(data []byte) io.Reader {
	return &resettableReader{data: data, offset: 0}
}

type resettableReader struct {
	data   []byte
	offset int
}

func (r *resettableReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}

	n = copy(p, r.data[r.offset:])
	r.offset += n

	return n, nil
}

// handleMaybeRetry checks whether a retry should be performed and sleeps for the backoff duration.
// Note in the interest of keeping this signature sane, we return a raw base error here.
func (c *Client) handleMaybeRetry(ctx context.Context, reqID string, serverDeadline time.Time, calc backoffCalculator,
	retries uint32, payload map[string]interface{}) ([]byte, error) {
	b := calc(retries)
	ctxDeadline, _ := ctx.Deadline()

	var body []byte

	if !ctxDeadline.IsZero() {
		now := time.Now()
		if now.Add(b).After(ctxDeadline) {
			return nil, ErrContextDeadlineWouldBeExceeded
		}
	}

	if !serverDeadline.IsZero() {
		serverTimeout := serverDeadline.Sub(time.Now().Add(b))

		if serverTimeout < 0 {
			return nil, ErrTimeout
		}

		payload["timeout"] = serverTimeout.String()

		payloadBody, err := json.Marshal(payload)
		if err != nil {
			return nil, newObfuscateErrorWrapper("failed to marshal payload after updating timeout", err)
		}

		body = payloadBody
	}

	c.logger.Trace("Retrying request %s in %s, retries: %d", reqID, b, retries)

	select {
	case <-ctx.Done():
		return nil, ctx.Err() // nolint:wrapcheck
	case <-time.After(b):
	}

	return body, nil
}

type backoffCalculator func(retryAttempts uint32) time.Duration

func analyticsExponentialBackoffWithJitter(min, max time.Duration, backoffFactor float64) backoffCalculator { //nolint:revive
	var minBackoff float64 = 1000000 // 1 Millisecond

	var maxBackoff float64 = 500000000 // 500 Milliseconds

	var factor float64 = 2

	if min > 0 {
		minBackoff = float64(min)
	}

	if max > 0 {
		maxBackoff = float64(max)
	}

	if backoffFactor > 0 {
		factor = backoffFactor
	}

	return func(retryAttempts uint32) time.Duration {
		backoff := minBackoff * (math.Pow(factor, float64(retryAttempts)))

		backoff = rand.Float64() * (backoff) // #nosec G404

		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		if backoff < minBackoff {
			backoff = minBackoff
		}

		return time.Duration(backoff)
	}
}
