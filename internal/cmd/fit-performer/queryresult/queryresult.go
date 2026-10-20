package queryresult

import (
	"context"

	cbanalytics "github.com/couchbase/gocbanalytics"
)

type Queryable interface {
	ExecuteQuery(ctx context.Context, statement string, opts ...*cbanalytics.QueryOptions) (*cbanalytics.QueryResult, error)
}

type queryResultOrError struct {
	err    error
	result *cbanalytics.QueryResult
}

type QueryResult struct {
	queryable Queryable

	awaitResult chan *queryResultOrError

	result *cbanalytics.QueryResult

	cancel context.CancelFunc
}

func NewQueryResult(cancel context.CancelFunc, queryable Queryable) *QueryResult {
	return &QueryResult{
		queryable:   queryable,
		awaitResult: make(chan *queryResultOrError, 1),
		result:      nil,
		cancel:      cancel,
	}
}

func NewQueryResultFromResult(result *cbanalytics.QueryResult, cancel context.CancelFunc) *QueryResult {
	if cancel == nil {
		cancel = func() {}
	}

	return &QueryResult{
		queryable:   nil,
		awaitResult: nil,
		result:      result,
		cancel:      cancel,
	}
}

func (q *QueryResult) ExecuteQuery(ctx context.Context, statement string, opts ...*cbanalytics.QueryOptions) {
	res, err := q.queryable.ExecuteQuery(ctx, statement, opts...)
	q.awaitResult <- &queryResultOrError{
		err:    err,
		result: res,
	}
}

func (q *QueryResult) AwaitResult() error {
	if q.result != nil {
		return nil
	}

	res := <-q.awaitResult
	if res.err != nil {
		return res.err
	}

	q.awaitResult = nil
	q.result = res.result

	return nil
}

func (q *QueryResult) Cancel() {
	q.cancel()
}

func (q *QueryResult) Metadata() (*cbanalytics.QueryMetadata, error) {
	return q.result.MetaData()
}

func (q *QueryResult) NextRow() *cbanalytics.QueryResultRow {
	return q.result.NextRow()
}

func (q *QueryResult) HasResult() bool {
	return q.result != nil
}

func (q *QueryResult) Err() error {
	return q.result.Err()
}
