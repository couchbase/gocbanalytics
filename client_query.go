package cbcolumnar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10"
)

type queryClient interface {
	Query(ctx context.Context, statement string, opts *QueryOptions) (*QueryResult, error)
}

type gocbcoreQueryClientNamespace struct {
	Database string
	Scope    string
}
type gocbcoreQueryClient struct {
	agent               *gocbcore.ColumnarAgent
	defaultQueryTimeout time.Duration
	defaultUnmarshaler  Unmarshaler
	namespace           *gocbcoreQueryClientNamespace
}

func newGocbcoreQueryClient(agent *gocbcore.ColumnarAgent, defaultQueryTimeout time.Duration,
	defaultUnmarshaler Unmarshaler, namespace *gocbcoreQueryClientNamespace) *gocbcoreQueryClient {
	return &gocbcoreQueryClient{
		agent:               agent,
		defaultQueryTimeout: defaultQueryTimeout,
		defaultUnmarshaler:  defaultUnmarshaler,
		namespace:           namespace,
	}
}

func (c *gocbcoreQueryClient) Query(ctx context.Context, statement string, opts *QueryOptions) (*QueryResult, error) {
	coreOpts, err := c.translateQueryOptions(ctx, statement, opts)
	if err != nil {
		return nil, err
	}

	if c.namespace != nil {
		coreOpts.Payload["query_context"] = fmt.Sprintf("default:`%s`.`%s`", c.namespace.Database, c.namespace.Scope)
	}

	coreOpts.Payload["client_context_id"] = uuid.NewString()

	res, err := c.agent.Query(ctx, *coreOpts)
	if err != nil {
		return nil, translateGocbcoreError(err)
	}

	unmarshaler := opts.Unmarshaler
	if unmarshaler == nil {
		unmarshaler = c.defaultUnmarshaler
	}

	return &QueryResult{
		reader:      c.newRowReader(res),
		unmarshaler: unmarshaler,
	}, nil
}

func (c *gocbcoreQueryClient) translateQueryOptions(ctx context.Context, statement string, opts *QueryOptions) (*gocbcore.ColumnarQueryOptions, error) {
	var priority *int

	if opts.Priority != nil && *opts.Priority {
		minus1 := -1
		priority = &minus1
	}

	execOpts := make(map[string]interface{})
	if opts.PositionalParameters != nil {
		execOpts["args"] = opts.PositionalParameters
	}

	if opts.NamedParameters != nil {
		for key, value := range opts.NamedParameters {
			if !strings.HasPrefix(key, "$") {
				key = "$" + key
			}

			execOpts[key] = value
		}
	}

	if opts.Raw != nil {
		for k, v := range opts.Raw {
			execOpts[k] = v
		}
	}

	if opts.ScanConsistency != nil {
		switch {
		case *opts.ScanConsistency == QueryScanConsistencyNotBounded:
			execOpts["scan_consistency"] = "not_bounded"
		case *opts.ScanConsistency == QueryScanConsistencyRequestPlus:
			execOpts["scan_consistency"] = "request_plus"
		default:
			return nil, invalidArgumentError{
				ArgumentName: "ScanConsistency",
				Reason:       "unknown value",
			}
		}
	}

	if opts.ReadOnly != nil {
		execOpts["readonly"] = *opts.ReadOnly
	}

	deadline, ok := ctx.Deadline()
	if ok {
		execOpts["timeout"] = (time.Until(deadline) + 5*time.Second).String()
	} else {
		execOpts["timeout"] = c.defaultQueryTimeout.String()
	}

	execOpts["statement"] = statement

	return &gocbcore.ColumnarQueryOptions{
		Payload:      execOpts,
		Priority:     priority,
		User:         "",
		TraceContext: nil,
	}, nil
}

type gocbcoreRowReader struct {
	reader *gocbcore.ColumnarRowReader
}

func (c *gocbcoreQueryClient) newRowReader(result *gocbcore.ColumnarRowReader) *gocbcoreRowReader {
	return &gocbcoreRowReader{
		reader: result,
	}
}

func (c *gocbcoreRowReader) NextRow() []byte {
	return c.reader.NextRow()
}

func (c *gocbcoreRowReader) MetaData() (*QueryMetadata, error) {
	metaBytes, err := c.reader.MetaData()
	if err != nil {
		return nil, translateGocbcoreError(err)
	}

	var jsonResp jsonAnalyticsResponse

	err = json.Unmarshal(metaBytes, &jsonResp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %s", err) // nolint: err113, errorlint
	}

	meta := &QueryMetadata{
		RequestID: "",
		Metrics: QueryMetrics{
			ElapsedTime:      0,
			ExecutionTime:    0,
			ResultCount:      0,
			ResultSize:       0,
			ProcessedObjects: 0,
		},
		Warnings: nil,
	}
	meta.fromData(jsonResp)

	return meta, nil
}

func (c *gocbcoreRowReader) Close() error {
	err := c.reader.Close()
	if err != nil {
		return translateGocbcoreError(err)
	}

	return nil
}

func (c *gocbcoreRowReader) Err() error {
	err := c.reader.Err()
	if err != nil {
		return translateGocbcoreError(err)
	}

	return nil
}

func translateGocbcoreError(err error) error {
	var coreErr *gocbcore.ColumnarError
	if !errors.As(err, &coreErr) {
		return err
	}

	if coreErr.HTTPResponseCode == 401 || errors.Is(err, gocbcore.ErrAuthenticationFailure) {
		return newColumnarError(coreErr.Statement, coreErr.Endpoint, coreErr.HTTPResponseCode).
			withMessage(coreErr.InnerError.Error()).
			withCause(ErrInvalidCredential)
	}

	if len(coreErr.Errors) > 0 {
		var firstNonRetriableErr *columnarErrorDesc

		descs := make([]columnarErrorDesc, len(coreErr.Errors))
		for i, desc := range coreErr.Errors {
			descs[i] = columnarErrorDesc{
				Code:    desc.Code,
				Message: desc.Message,
			}

			if firstNonRetriableErr == nil && !desc.Retry {
				firstNonRetriableErr = &descs[i]
			}
		}

		var code int

		var msg string

		if firstNonRetriableErr == nil {
			code = int(coreErr.Errors[0].Code)
			msg = coreErr.Errors[0].Message
		} else {
			code = int(firstNonRetriableErr.Code)
			msg = firstNonRetriableErr.Message
		}

		if code == 20000 {
			return newColumnarError(coreErr.Statement, coreErr.Endpoint, coreErr.HTTPResponseCode).
				withErrors(descs).
				withCause(ErrInvalidCredential)
		}

		if code == 21002 {
			return newColumnarError(coreErr.Statement, coreErr.Endpoint, coreErr.HTTPResponseCode).
				withErrors(descs).
				withCause(ErrTimeout)
		}

		qErr := newQueryError(coreErr.Statement, coreErr.Endpoint, coreErr.HTTPResponseCode, code, msg).
			withErrors(descs)

		switch {
		case errors.Is(coreErr.InnerError, gocbcore.ErrTimeout):
			qErr.cause.cause = ErrTimeout
		case errors.Is(coreErr.InnerError, context.Canceled):
			qErr.cause.cause = context.Canceled
		case errors.Is(coreErr.InnerError, context.DeadlineExceeded):
			qErr.cause.cause = context.DeadlineExceeded
		}

		return qErr
	}

	baseErr := newColumnarError(coreErr.Statement, coreErr.Endpoint, coreErr.HTTPResponseCode).
		withMessage(coreErr.InnerError.Error())

	switch {
	case errors.Is(coreErr.InnerError, gocbcore.ErrTimeout):
		baseErr.cause = ErrTimeout
		if coreErr.WasNotDispatched {
			baseErr.message = "operation not sent to server, as timeout would be exceeded"
		}
	case errors.Is(coreErr.InnerError, context.Canceled):
		baseErr.cause = context.Canceled
		if coreErr.WasNotDispatched {
			baseErr.message = "operation not sent to server, as context was cancelled"
		}
	case errors.Is(coreErr.InnerError, context.DeadlineExceeded):
		baseErr.cause = context.DeadlineExceeded
		if coreErr.WasNotDispatched {
			baseErr.message = "operation not sent to server, as context deadline would be exceeded"
		}
	case errors.Is(coreErr.InnerError, gocbcore.ErrAuthenticationFailure):
		baseErr.cause = ErrInvalidCredential
	default:
		baseErr.cause = errors.New(err.Error()) // nolint: err113
	}

	return baseErr
}
