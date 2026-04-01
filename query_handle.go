package cbanalytics

import (
	"context"
)

// QueryHandle represents an asynchronous query handle that can be used to check status,
// retrieve results, cancel, or discard a deferred query.
type QueryHandle struct {
	handle    string
	requestID string
	provider  queryHandleProvider
}

// FetchResultHandle fetches the current status of the deferred query from the server.
func (qh *QueryHandle) FetchResultHandle(ctx context.Context) (*QueryResultHandle, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	return qh.provider.fetchHandleResult(ctx, qh.handle)
}

// Cancel cancels the deferred query on the server.
func (qh *QueryHandle) Cancel(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	return qh.provider.cancelHandle(ctx, qh.requestID)
}

// QueryResultHandle provides access to the results of a completed deferred query.
type QueryResultHandle struct {
	handle      string
	provider    queryHandleProvider
	unmarshaler Unmarshaler
}

// FetchResults streams all results from the completed deferred query.
func (qhr *QueryResultHandle) FetchResults(ctx context.Context, opts ...*FetchResultsOptions) (*QueryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	unmarshaler := qhr.unmarshaler

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		if opt.Unmarshaler != nil {
			unmarshaler = opt.Unmarshaler
		}
	}

	return qhr.provider.streamHandleResults(ctx, qhr.handle, unmarshaler)
}

// DiscardResults discards the results of the deferred query on the server.
func (qhr *QueryResultHandle) DiscardResults(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	return qhr.provider.discardHandleResults(ctx, qhr.handle)
}
