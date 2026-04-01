package httpqueryclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ErrQueryNotFound is returned when a handle or result is not found.
var ErrQueryNotFound = errors.New("not found")

// handleResponse holds the parsed response from a handle request.
type handleResponse struct {
	statusCode int
	body       []byte
}

type handleRequestOptions struct {
	method      string
	path        string
	body        []byte
	contentType string
	authHandler func(req *http.Request)
	maxRetries  uint32
}

func (c *Client) handleResponseHandler(resp *http.Response, state *retryState) (*handleResponse, retryAction, error) {
	respBody, readErr := io.ReadAll(resp.Body)

	closeErr := resp.Body.Close()
	if closeErr != nil {
		c.logger.Debug("Failed to close response body: %v", closeErr)
	}

	if readErr != nil {
		return nil, retryActionReturn, newObfuscateErrorWrapper("failed to read response body", readErr)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return &handleResponse{
			statusCode: resp.StatusCode,
			body:       respBody,
		}, retryActionReturn, nil
	}

	cErr := parseAnalyticsErrorResponse(respBody, "", c.host, resp.StatusCode, state.lastCode, state.lastMessage, state.retries)
	if cErr != nil {
		first, retriable := isAnalyticsErrorRetriable(cErr)
		if !retriable {
			return nil, retryActionReturn, cErr
		}

		state.lastRootErr = cErr

		if first != nil {
			state.lastCode = first.Code
			state.lastMessage = first.Message
		}

		// Return the enriched error in case retry is denied.
		return nil, retryActionRetry, newAnalyticsError(cErr.InnerError, "", c.host, resp.StatusCode, state.retries).
			withErrors(cErr.Errors).
			withErrorText(string(respBody)).
			withLastDetail(state.lastCode, state.lastMessage)
	}

	return nil, retryActionReturn, newAnalyticsError(ErrAnalytics, "", c.host, resp.StatusCode, state.retries).
		withErrorText(string(respBody))
}

func (c *Client) doHandleRequest(ctx context.Context, opts handleRequestOptions) (*handleResponse, error) {
	header := make(http.Header)
	if opts.contentType != "" {
		header.Set("Content-Type", opts.contentType)
	}

	reqOpts := &retryableRequestOptions{
		method:         opts.method,
		path:           opts.path,
		body:           opts.body,
		header:         header,
		authHandler:    opts.authHandler,
		maxRetries:     opts.maxRetries,
		statement:      "",
		payload:        nil,
		serverDeadline: time.Time{},
	}

	return doWithRetries(ctx, c, reqOpts, c.handleResponseHandler)
}

// FetchHandleStatus fetches the status of a query handle.
func (c *Client) FetchHandleStatus(ctx context.Context, handle string,
	authHandler func(req *http.Request), maxRetries uint32) ([]byte, error) {
	resp, err := c.doHandleRequest(ctx, handleRequestOptions{
		method:      "GET",
		path:        handle,
		authHandler: authHandler,
		contentType: "",
		body:        nil,
		maxRetries:  maxRetries,
	})
	if err != nil {
		return nil, maybeQueryNotFoundError(err)
	}

	return resp.body, nil
}

// DiscardHandleResults discards the results for a query handle.
func (c *Client) DiscardHandleResults(ctx context.Context, handle string,
	authHandler func(req *http.Request), maxRetries uint32) error {
	_, err := c.doHandleRequest(ctx, handleRequestOptions{
		method:      "DELETE",
		path:        handle,
		authHandler: authHandler,
		contentType: "",
		body:        nil,
		maxRetries:  maxRetries,
	})
	if err != nil {
		if isHTTP404Error(err) {
			return nil
		}
	}

	return err
}

// CancelHandle cancels an active query handle.
func (c *Client) CancelHandle(ctx context.Context, requestID string,
	authHandler func(req *http.Request), maxRetries uint32) error {
	form := url.Values{}
	form.Set("request_id", requestID)

	_, err := c.doHandleRequest(ctx, handleRequestOptions{
		method:      "DELETE",
		path:        "/api/v1/active_requests/",
		body:        []byte(form.Encode()),
		contentType: "application/x-www-form-urlencoded",
		authHandler: authHandler,
		maxRetries:  maxRetries,
	})
	if err != nil {
		if isHTTP404Error(err) {
			return nil
		}
	}

	return err
}

// StreamHandleResults streams the results for a query handle.
func (c *Client) StreamHandleResults(ctx context.Context, handle string,
	authHandler func(req *http.Request), maxRetries uint32) (*QueryRowReader, error) {
	reqOpts := &retryableRequestOptions{
		method:         "GET",
		path:           handle,
		authHandler:    authHandler,
		maxRetries:     maxRetries,
		body:           nil,
		header:         nil,
		statement:      "",
		payload:        nil,
		serverDeadline: time.Time{},
	}

	return doWithRetries(ctx, c, reqOpts, func(resp *http.Response, state *retryState) (*QueryRowReader, retryAction, error) {
		return c.handleStreamHandleResponse(resp, state)
	})
}

func (c *Client) handleStreamHandleResponse(resp *http.Response, state *retryState) (*QueryRowReader, retryAction, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)

		closeErr := resp.Body.Close()
		if closeErr != nil {
			c.logger.Debug("Failed to close response body: %v", closeErr)
		}

		if readErr != nil {
			return nil, retryActionReturn, newObfuscateErrorWrapper("failed to read response body", readErr)
		}

		cErr := parseAnalyticsErrorResponse(respBody, "", c.host, resp.StatusCode, state.lastCode, state.lastMessage, state.retries)
		if cErr != nil {
			first, retriable := isAnalyticsErrorRetriable(cErr)
			if !retriable {
				return nil, retryActionReturn, cErr
			}

			state.lastRootErr = cErr

			if first != nil {
				state.lastCode = first.Code
				state.lastMessage = first.Message
			}

			return nil, retryActionRetry, newAnalyticsError(cErr.InnerError, "", c.host, resp.StatusCode, state.retries).
				withErrors(cErr.Errors).
				withErrorText(string(respBody)).
				withLastDetail(state.lastCode, state.lastMessage)
		}

		if resp.StatusCode == 404 {
			return nil, retryActionReturn, newAnalyticsError(ErrQueryNotFound, "", c.host, resp.StatusCode, state.retries).
				withErrorText(string(respBody))
		}

		return nil, retryActionReturn, newAnalyticsError(ErrAnalytics, "", c.host, resp.StatusCode, state.retries).
			withErrorText(string(respBody))
	}

	streamer, err := newQueryStreamer(resp.Body, c.logger, "results")
	if err != nil {
		resp.Body.Close()

		return nil, retryActionReturn, newObfuscateErrorWrapper("failed to parse response body", err)
	}

	return &QueryRowReader{
		streamer:   streamer,
		statement:  "",
		endpoint:   c.host,
		statusCode: resp.StatusCode,
		peeked:     nil,
	}, retryActionReturn, nil
}

func maybeQueryNotFoundError(err error) error {
	var qErr *QueryError
	if errors.As(err, &qErr) && qErr.HTTPResponseCode == 404 {
		return newAnalyticsError(ErrQueryNotFound, qErr.Statement, qErr.Endpoint, qErr.HTTPResponseCode, qErr.Retries).
			withErrorText(qErr.ErrorText)
	}

	return err
}
