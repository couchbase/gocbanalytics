package httpqueryclient

import (
	"bytes"
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

// Query executes a query.
func (c *Client) Query(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	statement := getMapValueString(opts.Payload, "statement", "")

	body, err := json.Marshal(opts.Payload)
	if err != nil {
		return nil, newObfuscateErrorWrapper("failed to marshal query payload", err)
	}

	header := make(http.Header)
	header.Set("Content-Type", "application/json")

	ctxDeadline, _ := ctx.Deadline()

	var serverDeadline time.Time

	st, ok := opts.Payload["timeout"]
	if ok {
		timeout, err := time.ParseDuration(st.(string))
		if err != nil {
			return nil, newObfuscateErrorWrapper("failed to parse server timeout", err)
		}

		serverDeadline = time.Now().Add(timeout)
	}

	var lastCode uint32

	var lastMessage string

	var lastRootErr error

	var retries uint32

	uniqueID := uuid.NewString()

	backoff := analyticsExponentialBackoffWithJitter(100*time.Millisecond, 1*time.Minute, 2)

	addrs, err := c.resolver.LookupHost(ctx, c.host)
	if err != nil {
		return nil, newAnalyticsError(fmt.Errorf("failed to lookup host: %w", err), statement, c.host, 0)
	}

	for {
		if len(addrs) == 0 {
			return nil, newAnalyticsError(lastRootErr, statement, c.host, 0).withLastDetail(lastCode, lastMessage)
		}

		idx := rand.Intn(len(addrs))
		addr := addrs[idx]

		reqURI := fmt.Sprintf("%s://%s:%d/api/v1/request", c.scheme, addr, c.port)

		var connectDoneErr error

		trace := &httptrace.ClientTrace{ //nolint:exhaustruct
			ConnectDone: func(_, _ string, err error) {
				connectDoneErr = err
			},
		}

		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), "POST", reqURI, io.NopCloser(bytes.NewReader(body)))
		if err != nil {
			return nil, newObfuscateErrorWrapper("failed to create http request", err)
		}

		req.Host = c.host
		req.Header = header

		username, password := opts.CredentialProvider()
		req.SetBasicAuth(username, password)

		c.logger.Trace("Sending request %s to %s", uniqueID, reqURI)

		resp, err := c.innerClient.Do(req)
		if err != nil {
			c.logger.Trace("Received HTTP Response for ID=%s, errored: %v", uniqueID, err)

			// We don't want to bail out on connection errors as they may be because of dial timeout.
			if connectDoneErr == nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil, newAnalyticsError(err, statement, c.host, 0)
				}
			}

			newBody, notRetriableErr := handleMaybeRetryAnalytics(ctxDeadline, serverDeadline, backoff, retries, opts.Payload)
			if notRetriableErr != nil {
				return nil, newAnalyticsError(notRetriableErr, statement, c.host, 0).withLastDetail(lastCode, lastMessage)
			}

			addrs = append(addrs[:idx], addrs[idx+1:]...)

			if connectDoneErr == nil {
				lastRootErr = newObfuscateErrorWrapper("failed to send request", err)
			} else {
				lastRootErr = connectDoneErr
			}

			body = newBody
			retries++

			continue
		}

		c.logger.Trace("Received HTTP Response for ID=%s, status=%d", uniqueID, resp.StatusCode)

		resp = leakcheck.WrapHTTPResponse(resp) // nolint: bodyclose
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			respBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return nil, newAnalyticsError(newObfuscateErrorWrapper("failed to read response body", readErr), statement,
					c.host, resp.StatusCode)
			}

			cErr := parseAnalyticsErrorResponse(respBody, statement, c.host, resp.StatusCode, lastCode, lastMessage)
			if cErr != nil {
				first, retriable := isAnalyticsErrorRetriable(cErr)
				if !retriable {
					return nil, cErr
				}

				lastRootErr = cErr

				if first != nil {
					lastCode = first.Code
					lastMessage = first.Message
				}

				newBody, err := handleMaybeRetryAnalytics(ctxDeadline, serverDeadline, backoff, retries, opts.Payload)
				if err != nil {
					return nil, newAnalyticsError(err, statement, c.host, resp.StatusCode).
						withErrors(cErr.Errors).
						withErrorText(string(respBody)).
						withLastDetail(lastCode, lastMessage)
				}

				body = newBody
				retries++

				continue
			}

			return nil, newAnalyticsError(
				errors.New("query returned non-200 status code but no errors in body"), //nolint:err113
				statement,
				c.host,
				resp.StatusCode).
				withErrorText(string(respBody)).
				withLastDetail(lastCode, lastMessage)
		}

		streamer, err := newQueryStreamer(resp.Body, c.logger, "results")
		if err != nil {
			respBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				c.logger.Debug("Failed to read response body: %v", readErr)
			}

			return nil, newAnalyticsError(newObfuscateErrorWrapper("failed to parse success response body", readErr),
				statement,
				c.host,
				resp.StatusCode).
				withErrorText(string(respBody)).
				withLastDetail(lastCode, lastMessage)
		}

		peeked := streamer.NextRow()
		if peeked == nil {
			err := streamer.Err()
			if err != nil {
				return nil, newAnalyticsError(err,
					statement,
					c.host,
					resp.StatusCode)
			}

			meta, metaErr := streamer.MetaData()
			if metaErr != nil {
				return nil, newAnalyticsError(metaErr,
					statement,
					c.host,
					resp.StatusCode)
			}

			cErr := parseAnalyticsErrorResponse(meta, statement, c.host, resp.StatusCode, lastCode, lastMessage)
			if cErr != nil {
				first, retriable := isAnalyticsErrorRetriable(cErr)
				if !retriable {
					return nil, cErr
				}

				lastRootErr = cErr

				if first != nil {
					lastCode = first.Code
					lastMessage = first.Message
				}

				newBody, err := handleMaybeRetryAnalytics(ctxDeadline, serverDeadline, backoff, retries, opts.Payload)
				if err != nil {
					return nil, newAnalyticsError(err, statement, c.host, resp.StatusCode).
						withErrors(cErr.Errors).
						withErrorText(string(meta)).
						withLastDetail(lastCode, lastMessage)
				}

				body = newBody
				retries++

				continue
			}
		}

		return &QueryRowReader{
			streamer:   streamer,
			statement:  statement,
			endpoint:   c.host,
			statusCode: resp.StatusCode,
			peeked:     peeked,
		}, nil
	}
}

func parseAnalyticsErrorResponse(respBody []byte, statement, endpoint string, statusCode int, lastCode uint32, lastMsg string) *QueryError {
	if statusCode == 401 {
		return newAnalyticsError(ErrInvalidCredential, statement, endpoint, statusCode)
	}

	var rawRespParse jsonAnalyticsErrorResponse

	parseErr := json.Unmarshal(respBody, &rawRespParse)
	if parseErr != nil {
		return newAnalyticsError(newObfuscateErrorWrapper("failed to parse response errors", parseErr), statement, endpoint, statusCode).
			withLastDetail(lastCode, lastMsg).
			withErrorText(string(respBody))
	}

	if len(rawRespParse.Errors) == 0 {
		if statusCode == 503 {
			return newAnalyticsError(ErrServiceUnavailable, statement, endpoint, statusCode)
		}

		return nil
	}

	var respParse []jsonAnalyticsError

	parseErr = json.Unmarshal(rawRespParse.Errors, &respParse)
	if parseErr != nil {
		return newAnalyticsError(newObfuscateErrorWrapper("failed to parse response errors", parseErr), statement, endpoint, statusCode).
			withLastDetail(lastCode, lastMsg).
			withErrorText(string(respBody))
	}

	if len(respParse) == 0 {
		return nil
	}

	errDescs := make([]ErrorDesc, len(respParse))
	for i, jsonErr := range respParse {
		errDescs[i] = ErrorDesc{
			Code:    jsonErr.Code,
			Message: jsonErr.Msg,
			Retry:   jsonErr.Retry,
		}
	}

	return newAnalyticsError(ErrAnalytics, statement, endpoint, statusCode).
		withLastDetail(lastCode, lastMsg).
		withErrorText(string(respBody)).
		withErrors(errDescs)
}

func isAnalyticsErrorRetriable(cErr *QueryError) (*ErrorDesc, bool) {
	if errors.Is(cErr, ErrServiceUnavailable) {
		return nil, true
	}

	// If there are no errors then we shouldn't retry.
	if len(cErr.Errors) == 0 {
		return nil, false
	}

	var first *ErrorDesc

	allRetriable := true

	for _, err := range cErr.Errors {
		if !err.Retry {
			allRetriable = false

			if first == nil {
				first = &ErrorDesc{
					Code:    err.Code,
					Message: err.Message,
					Retry:   false,
				}
			}
		}
	}

	if !allRetriable {
		return nil, false
	}

	if first == nil {
		first = &cErr.Errors[0]
	}

	return first, true
}

// Note in the interest of keeping this signature sane, we return a raw base error here.
func handleMaybeRetryAnalytics(ctxDeadline time.Time, serverDeadline time.Time, calc backoffCalculator,
	retries uint32, payload map[string]interface{}) ([]byte, error) {
	b := calc(retries)

	var body []byte

	if !ctxDeadline.IsZero() {
		if time.Now().Add(b).After(ctxDeadline.Add(-b)) {
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

	time.Sleep(b)

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
