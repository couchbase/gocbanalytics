package httpqueryclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
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

	var serverDeadline time.Time

	st, ok := opts.Payload["timeout"]
	if ok {
		timeout, err := time.ParseDuration(st.(string))
		if err != nil {
			return nil, newObfuscateErrorWrapper("failed to parse server timeout", err)
		}

		serverDeadline = time.Now().Add(timeout)
	}

	reqOpts := &retryableRequestOptions{
		method:         "POST",
		path:           "/api/v1/request",
		body:           body,
		header:         header,
		authHandler:    opts.AuthHandler,
		maxRetries:     opts.MaxRetries,
		statement:      statement,
		payload:        opts.Payload,
		serverDeadline: serverDeadline,
	}

	return doWithRetries(ctx, c, reqOpts, func(resp *http.Response, state *retryState) (*QueryRowReader, retryAction, error) {
		return c.handleQueryResponse(resp, state, statement)
	})
}

func (c *Client) handleQueryResponse(resp *http.Response, state *retryState, statement string) (*QueryRowReader, retryAction, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)

		closeErr := resp.Body.Close()
		if closeErr != nil {
			c.logger.Debug("Failed to close response body: %v", closeErr)
		}

		if readErr != nil {
			return nil, retryActionReturn, newAnalyticsError(newObfuscateErrorWrapper("failed to read response body", readErr), statement,
				c.host, resp.StatusCode, state.retries).
				withErrorText(string(respBody))
		}

		cErr := parseAnalyticsErrorResponse(respBody, statement, c.host, resp.StatusCode, state.lastCode, state.lastMessage, state.retries)
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
			return nil, retryActionRetry, newAnalyticsError(cErr.InnerError, statement, c.host, resp.StatusCode, state.retries).
				withErrors(cErr.Errors).
				withErrorText(string(respBody)).
				withLastDetail(state.lastCode, state.lastMessage)
		}

		return nil, retryActionReturn, newAnalyticsError(
			errors.New("query returned non-200 status code but no errors in body"), //nolint:err113
			statement,
			c.host,
			resp.StatusCode,
			state.retries).
			withErrorText(string(respBody)).
			withLastDetail(state.lastCode, state.lastMessage)
	}

	streamer, err := newQueryStreamer(resp.Body, c.logger, "results")
	if err != nil {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			c.logger.Debug("Failed to read response body: %v", readErr)
		}

		closeErr := resp.Body.Close()
		if closeErr != nil {
			c.logger.Debug("Failed to close response body: %v", closeErr)
		}

		return nil, retryActionReturn, newAnalyticsError(newObfuscateErrorWrapper("failed to parse success response body", err),
			statement,
			c.host,
			resp.StatusCode,
			state.retries).
			withErrorText(string(respBody)).
			withLastDetail(state.lastCode, state.lastMessage)
	}

	peeked := streamer.NextRow()
	if peeked == nil {
		err := streamer.Err()
		if err != nil {
			return nil, retryActionReturn, newAnalyticsError(err,
				statement,
				c.host,
				resp.StatusCode,
				state.retries).
				withLastDetail(state.lastCode, state.lastMessage)
		}

		meta, metaErr := streamer.MetaData()
		if metaErr != nil {
			return nil, retryActionReturn, newAnalyticsError(metaErr,
				statement,
				c.host,
				resp.StatusCode,
				state.retries).
				withErrorText(string(meta)).
				withLastDetail(state.lastCode, state.lastMessage)
		}

		cErr := parseAnalyticsErrorResponse(meta, statement, c.host, resp.StatusCode, state.lastCode, state.lastMessage, state.retries)
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
			return nil, retryActionRetry, newAnalyticsError(cErr.InnerError, statement, c.host, resp.StatusCode, state.retries).
				withErrors(cErr.Errors).
				withErrorText(string(meta)).
				withLastDetail(state.lastCode, state.lastMessage)
		}
	}

	return &QueryRowReader{
		streamer:   streamer,
		statement:  statement,
		endpoint:   c.host,
		statusCode: resp.StatusCode,
		peeked:     peeked,
	}, retryActionReturn, nil
}

func parseAnalyticsErrorResponse(respBody []byte, statement, endpoint string, statusCode int, lastCode uint32, lastMsg string, retries uint32) *QueryError {
	if statusCode == 401 {
		return newAnalyticsError(ErrInvalidCredential, statement, endpoint, statusCode, retries)
	}

	var rawRespParse jsonAnalyticsErrorResponse

	parseErr := json.Unmarshal(respBody, &rawRespParse)
	if parseErr != nil {
		return newAnalyticsError(
			newObfuscateErrorWrapper("failed to parse response errors", parseErr),
			statement,
			endpoint,
			statusCode,
			retries,
		).
			withLastDetail(lastCode, lastMsg).
			withErrorText(string(respBody))
	}

	if len(rawRespParse.Errors) == 0 {
		if statusCode == 503 {
			return newAnalyticsError(ErrServiceUnavailable, statement, endpoint, statusCode, retries)
		}

		return nil
	}

	var respParse []jsonAnalyticsError

	parseErr = json.Unmarshal(rawRespParse.Errors, &respParse)
	if parseErr != nil {
		return newAnalyticsError(
			newObfuscateErrorWrapper("failed to parse response errors", parseErr),
			statement,
			endpoint,
			statusCode,
			retries,
		).
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

	return newAnalyticsError(ErrAnalytics, statement, endpoint, statusCode, retries).
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
