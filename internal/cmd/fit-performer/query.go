package main

import (
	"context"
	"fmt"
	"time"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/serialization"

	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/fiterror"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/options"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/metadata"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/query"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/result"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/queryresult"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/unmarshal"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *AnalyticsPerformer) ExecuteQuery(_ctx context.Context, req *query.ExecuteQueryRequest) (*query.ExecuteQueryResponse, error) {
	var ctx context.Context

	var cancel context.CancelFunc

	var opts *cbanalytics.QueryOptions

	if req.Options != nil {
		if req.Options.Timeout == nil {
			ctx, cancel = context.WithCancel(context.Background())
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), req.Options.Timeout.AsDuration())
		}

		var err error

		opts, err = options.ConvertQueryProtoOptionsToSDK(req.Options)
		if err != nil {
			cancel()

			return nil, err
		}
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

	var queryable queryresult.Queryable

	switch l := req.Level.(type) {
	case *query.ExecuteQueryRequest_ClusterLevel:
		cluster, ok := c.conns.Get(l.ClusterLevel.ClusterId)
		if !ok {
			cancel()

			return nil, fmt.Errorf("cluster not found: %s", l.ClusterLevel.ClusterId)
		}

		queryable = cluster
	case *query.ExecuteQueryRequest_ScopeLevel:
		cluster, ok := c.conns.Get(l.ScopeLevel.ClusterId)
		if !ok {
			cancel()

			return nil, fmt.Errorf("cluster not found: %s", l.ScopeLevel.ClusterId)
		}

		queryable = cluster.Database(l.ScopeLevel.DatabaseName).Scope(l.ScopeLevel.ScopeName)
	}

	queryHandle := uuid.NewString()
	queryResult := queryresult.NewQueryResult(cancel, queryable)
	c.queries.Set(queryHandle, queryResult)

	start := time.Now()

	go queryResult.ExecuteQuery(ctx, req.Statement, opts)

	return &query.ExecuteQueryResponse{
		QueryHandle: queryHandle,
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) QueryResult(_ctx context.Context, req *query.QueryResultRequest) (*result.EmptyResultOrFailureResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	err := res.AwaitResult()
	if err != nil {
		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) QueryRow(ctx context.Context, req *query.QueryRowRequest) (*query.QueryRowResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	if !res.HasResult() {
		return nil, fmt.Errorf("query result has not been awaited: %s", req.QueryHandle)
	}

	row := res.NextRow()

	if row == nil {
		if err := res.Err(); err != nil {
			return &query.QueryRowResponse{
				Result: &query.QueryRowResponse_RowLevelFailure{
					RowLevelFailure: fiterror.ParseSDKError(err),
				},
				Metadata: &metadata.ResponseMetadata{
					ElapsedNanos: time.Since(start).Nanoseconds(),
					Initiated:    timestamppb.New(start),
				},
			}, nil
		}

		return &query.QueryRowResponse{
			Result: &query.QueryRowResponse_Success{
				Success: &query.QueryRowResponse_Result{
					Row:         nil,
					EndOfStream: true,
				},
			},
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	var contentWas *serialization.ContentWas

	if req.ContentAs != nil {
		var err error

		contentWas, err = unmarshal.ParseContentAs(req.ContentAs, row)
		if err != nil {
			return &query.QueryRowResponse{
				Result: &query.QueryRowResponse_RowLevelFailure{
					RowLevelFailure: fiterror.ParseSDKError(err),
				},
				Metadata: &metadata.ResponseMetadata{
					ElapsedNanos: time.Since(start).Nanoseconds(),
					Initiated:    timestamppb.New(start),
				},
			}, nil
		}
	}

	return &query.QueryRowResponse{
		Result: &query.QueryRowResponse_Success{
			Success: &query.QueryRowResponse_Result{
				Row: &query.QueryRowResponse_Row{
					RowContent: contentWas,
				},
				EndOfStream: false,
			},
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) QueryCancel(ctx context.Context, req *query.QueryCancelRequest) (*result.EmptyResultOrFailureResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	res.Cancel()

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) QueryMetadata(ctx context.Context, req *query.QueryMetadataRequest) (*query.QueryResultMetadataResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	if !res.HasResult() {
		return nil, fmt.Errorf("query result has not been awaited: %s", req.QueryHandle)
	}

	meta, err := res.Metadata()
	if err != nil {
		return &query.QueryResultMetadataResponse{
			Result: &query.QueryResultMetadataResponse_Failure{
				Failure: fiterror.ParseSDKError(err),
			},
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	return &query.QueryResultMetadataResponse{
		Result: &query.QueryResultMetadataResponse_Success{
			Success: ParseMetadata(meta),
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) CloseQueryResult(ctx context.Context, req *query.CloseQueryResultRequest) (*result.EmptyResultOrFailureResponse, error) {
	c.queries.Delete(req.QueryHandle)
	c.queryHandles.Delete(req.QueryHandle)
	c.statusHandles.Delete(req.QueryHandle)
	c.resultHandles.Delete(req.QueryHandle)

	start := time.Now()

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) CloseAllQueryResults(_ctx context.Context, _req *query.CloseAllQueryResultsRequest) (*result.EmptyResultOrFailureResponse, error) {
	start := time.Now()

	c.queries.Clear()
	c.queryHandles.Clear()
	c.statusHandles.Clear()
	c.resultHandles.Clear()

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) StartQuery(_ctx context.Context, req *query.StartQueryRequest) (*query.StartQueryResponse, error) {
	ctx := context.Background()

	if req.Options != nil && req.Options.Timeout != nil {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, req.Options.Timeout.AsDuration())

		defer cancel()
	}

	var startOpts *cbanalytics.StartQueryOptions

	if req.Options != nil {
		var err error

		startOpts, err = options.ConvertStartQueryProtoOptionsToSDK(req.Options)
		if err != nil {
			return nil, err
		}
	}

	var startQueryable startQueryable

	var handleTimeout time.Duration

	switch l := req.Level.(type) {
	case *query.StartQueryRequest_ClusterLevel:
		cluster, ok := c.conns.Get(l.ClusterLevel.ClusterId)
		if !ok {
			return nil, fmt.Errorf("cluster not found: %s", l.ClusterLevel.ClusterId)
		}

		startQueryable = cluster
		handleTimeout = cluster.HandleTimeout()
	case *query.StartQueryRequest_ScopeLevel:
		cluster, ok := c.conns.Get(l.ScopeLevel.ClusterId)
		if !ok {
			return nil, fmt.Errorf("cluster not found: %s", l.ScopeLevel.ClusterId)
		}

		startQueryable = cluster.Database(l.ScopeLevel.DatabaseName).Scope(l.ScopeLevel.ScopeName)
		handleTimeout = cluster.HandleTimeout()
	}

	handle, err := startQueryable.StartQuery(ctx, req.Statement, startOpts)
	if err != nil {
		return &query.StartQueryResponse{
			Result: &query.StartQueryResponse_Failure{
				Failure: fiterror.ParseSDKError(err),
			},
		}, nil
	}

	queryHandle := uuid.NewString()
	c.queryHandles.Set(queryHandle, &queryHandleResource{
		handle:        handle,
		handleTimeout: handleTimeout,
	})

	return &query.StartQueryResponse{
		Result: &query.StartQueryResponse_QueryHandle{
			QueryHandle: queryHandle,
		},
	}, nil
}

func (c *AnalyticsPerformer) AsyncFetchStatus(_ctx context.Context, req *query.AsyncFetchStatusRequest) (*query.AsyncFetchStatusResponse, error) {
	handleResource, ok := c.queryHandles.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	ctx, cancel := contextWithHandleTimeout(handleResource.handleTimeout)
	defer cancel()

	status, err := handleResource.handle.FetchStatus(ctx)
	if err != nil {
		return &query.AsyncFetchStatusResponse{
			Result: &query.AsyncFetchStatusResponse_Failure{
				Failure: fiterror.ParseSDKError(err),
			},
		}, nil
	}

	c.statusHandles.Set(req.QueryHandle, &queryStatusResource{
		status:        status,
		handleTimeout: handleResource.handleTimeout,
	})

	return &query.AsyncFetchStatusResponse{
		Result: &query.AsyncFetchStatusResponse_QueryStatus{
			QueryStatus: &query.AsyncFetchStatusResponse_QueryStatusResult{
				ResultsReady: status.ResultsReady(),
				ToString:     status.String(),
			},
		},
	}, nil
}

func (c *AnalyticsPerformer) AsyncQueryStatusResultHandle(ctx context.Context, req *query.AsyncQueryStatusResultHandleRequest) (*result.EmptyResultOrFailureResponse, error) {
	statusResource, ok := c.statusHandles.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query status not found: %s", req.QueryHandle)
	}

	start := time.Now()

	handle, err := statusResource.status.ResultHandle()
	if err != nil {
		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	c.resultHandles.Set(req.QueryHandle, &resultHandleResource{
		handle:        handle,
		handleTimeout: statusResource.handleTimeout,
	})

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) AsyncCancelHandle(_ctx context.Context, req *query.AsyncCancelHandleRequest) (*result.EmptyResultOrFailureResponse, error) {
	handleResource, ok := c.queryHandles.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	ctx, cancel := contextWithHandleTimeout(handleResource.handleTimeout)
	defer cancel()

	err := handleResource.handle.Cancel(ctx)
	if err != nil {
		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) AsyncFetchResults(_ctx context.Context, req *query.AsyncFetchResultsRequest) (*result.EmptyResultOrFailureResponse, error) {
	resultHandleResource, ok := c.resultHandles.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("result handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	ctx, cancel := contextWithHandleTimeout(resultHandleResource.handleTimeout)

	var fetchOpts *cbanalytics.FetchResultsOptions

	if req.Options != nil && req.Options.Deserializer != nil {
		u, err := unmarshal.NewUnmarshaler(req.Options.Deserializer)
		if err != nil {
			return nil, err
		}

		fetchOpts = cbanalytics.NewFetchResultOptions().SetUnmarshaler(u)
	}

	queryResult, err := resultHandleResource.handle.FetchResults(ctx, fetchOpts)
	if err != nil {
		cancel()

		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	// Store the QueryResult so it can be iterated via QueryRow/QueryMetadata
	qr := queryresult.NewQueryResultFromResult(queryResult, cancel)
	c.queries.Set(req.QueryHandle, qr)

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *AnalyticsPerformer) AsyncDiscardResults(_ctx context.Context, req *query.AsyncDiscardResultsRequest) (*result.EmptyResultOrFailureResponse, error) {
	resultHandleResource, ok := c.resultHandles.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("result handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	ctx, cancel := contextWithHandleTimeout(resultHandleResource.handleTimeout)
	defer cancel()

	err := resultHandleResource.handle.DiscardResults(ctx)
	if err != nil {
		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

type startQueryable interface {
	StartQuery(ctx context.Context, statement string, opts ...*cbanalytics.StartQueryOptions) (*cbanalytics.QueryHandle, error)
}

func contextWithHandleTimeout(handleTimeout time.Duration) (context.Context, context.CancelFunc) {
	if handleTimeout > 0 {
		return context.WithTimeout(context.Background(), handleTimeout)
	}

	return context.WithCancel(context.Background())
}
