package cbanalytics

import (
	"context"
	"fmt"
)

// QueryHandle represents an asynchronous query handle that can be used to check status,
// retrieve results, cancel, or discard a deferred query.
type QueryHandle struct {
	handle    string
	requestID string
	provider  queryHandleProvider
}

// FetchStatus fetches the current status of the deferred query from the server.
func (qh *QueryHandle) FetchStatus(ctx context.Context) (*QueryStatus, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	return qh.provider.fetchHandleStatus(ctx, qh.handle)
}

// QueryStatus represents the status of a deferred query.
type QueryStatus struct {
	status       string
	metrics      string
	resultHandle *QueryResultHandle
}

// ResultsReady returns true if the query results are ready to be fetched.
func (qs *QueryStatus) ResultsReady() bool {
	return qs.resultHandle != nil
}

// ResultHandle returns the QueryResultHandle for accessing results.
// ResultHandle should only be called when ResultsReady returns true.
func (qs *QueryStatus) ResultHandle() (*QueryResultHandle, error) {
	if qs.resultHandle == nil {
		return nil, newAnalyticsError(ErrAnalytics, "", "", 0, 0).
			withMessage("ResultHandle should only be called when ResultsReady returns true")
	}

	return qs.resultHandle, nil
}

func (qs *QueryStatus) String() string {
	return fmt.Sprintf("status: %s, metrics: %s", qs.status, qs.metrics)
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
