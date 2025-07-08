package cbanalytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbanalytics/internal/httpqueryclient"
)

type queryClient interface {
	Query(ctx context.Context, statement string, opts *QueryOptions) (*QueryResult, error)
}

type queryClientNamespace struct {
	Database string
	Scope    string
}
type httpQueryClient struct {
	credential                Credential
	client                    *httpqueryclient.Client
	defaultServerQueryTimeout time.Duration
	defaultUnmarshaler        Unmarshaler
	namespace                 *queryClientNamespace
	logger                    Logger
}

type httpQueryClientConfig struct {
	Credential                Credential
	Client                    *httpqueryclient.Client
	DefaultServerQueryTimeout time.Duration
	DefaultUnmarshaler        Unmarshaler
	Namespace                 *queryClientNamespace
	Logger                    Logger
}

func newHTTPQueryClient(cfg httpQueryClientConfig) *httpQueryClient {
	return &httpQueryClient{
		credential:                cfg.Credential,
		client:                    cfg.Client,
		defaultServerQueryTimeout: cfg.DefaultServerQueryTimeout,
		defaultUnmarshaler:        cfg.DefaultUnmarshaler,
		namespace:                 cfg.Namespace,
		logger:                    cfg.Logger,
	}
}

func (c *httpQueryClient) Query(ctx context.Context, statement string, opts *QueryOptions) (*QueryResult, error) {
	clientOpts, err := c.translateQueryOptions(ctx, statement, opts)
	if err != nil {
		return nil, err
	}

	if c.namespace != nil {
		clientOpts.Payload["query_context"] = fmt.Sprintf("default:`%s`.`%s`", c.namespace.Database, c.namespace.Scope)
	}

	clientContextID := opts.ClientContextID
	if clientContextID == nil {
		id := uuid.NewString()
		clientContextID = &id
	}

	clientOpts.Payload["client_context_id"] = clientContextID

	res, err := c.client.Query(ctx, clientOpts)
	if err != nil {
		return nil, translateClientError(err)
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

func (c *httpQueryClient) translateQueryOptions(ctx context.Context, statement string, opts *QueryOptions) (*httpqueryclient.QueryOptions, error) {
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
		execOpts["timeout"] = c.defaultServerQueryTimeout.String()
	}

	execOpts["statement"] = statement

	var credentialProvider func() (string, string)
	switch credential := c.credential.(type) {
	case *BasicAuthCredential:
		credentialProvider = func() (string, string) {
			return credential.UserPassPair.Username, credential.UserPassPair.Password
		}
	case *DynamicBasicAuthCredential:
		credentialProvider = func() (string, string) {
			userPassPair := credential.Credentials()

			return userPassPair.Username, userPassPair.Password
		}
	}

	return &httpqueryclient.QueryOptions{
		Payload:            execOpts,
		CredentialProvider: credentialProvider,
	}, nil
}

type clientRowReader struct {
	reader *httpqueryclient.QueryRowReader
}

func (c *httpQueryClient) newRowReader(result *httpqueryclient.QueryRowReader) *clientRowReader {
	return &clientRowReader{
		reader: result,
	}
}

func (c *clientRowReader) NextRow() []byte {
	return c.reader.NextRow()
}

func (c *clientRowReader) MetaData() (*QueryMetadata, error) {
	metaBytes, err := c.reader.MetaData()
	if err != nil {
		return nil, translateClientError(err)
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

func (c *clientRowReader) Close() error {
	err := c.reader.Close()
	if err != nil {
		return translateClientError(err)
	}

	return nil
}

func (c *clientRowReader) Err() error {
	err := c.reader.Err()
	if err != nil {
		return translateClientError(err)
	}

	return nil
}

func translateClientError(err error) error {
	var clientErr *httpqueryclient.QueryError
	if !errors.As(err, &clientErr) {
		return err
	}

	if len(clientErr.Errors) == 0 {
		baseErr := err

		switch {
		case errors.Is(err, httpqueryclient.ErrInvalidCredential):
			baseErr = ErrInvalidCredential
		case errors.Is(err, httpqueryclient.ErrServiceUnavailable):
			baseErr = ErrServiceUnavailable
		case errors.Is(err, httpqueryclient.ErrTimeout):
			baseErr = ErrTimeout
		case errors.Is(err, context.Canceled):
			baseErr = context.Canceled
		case errors.Is(err, context.DeadlineExceeded):
			baseErr = context.DeadlineExceeded
		}

		return newAnalyticsError(baseErr, clientErr.Statement, clientErr.Endpoint, clientErr.HTTPResponseCode).
			withMessage(clientErr.InnerError.Error())
	}

	var firstNonRetriableErr *analyticsErrorDesc

	descs := make([]analyticsErrorDesc, len(clientErr.Errors))
	for i, desc := range clientErr.Errors {
		descs[i] = analyticsErrorDesc{
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
		code = int(clientErr.Errors[0].Code)
		msg = clientErr.Errors[0].Message
	} else {
		code = int(firstNonRetriableErr.Code)
		msg = firstNonRetriableErr.Message
	}

	switch code {
	case 20000:
		return newQueryError(ErrInvalidCredential, clientErr.Statement, clientErr.Endpoint, clientErr.HTTPResponseCode, code, msg).
			withErrors(descs)
	case 21002:
		return newQueryError(ErrTimeout, clientErr.Statement, clientErr.Endpoint, clientErr.HTTPResponseCode, code, msg).
			withErrors(descs)
	case 23000:
		return newQueryError(ErrServiceUnavailable, clientErr.Statement, clientErr.Endpoint, clientErr.HTTPResponseCode, code, msg).
			withErrors(descs)
	}

	qErr := newQueryError(nil, clientErr.Statement, clientErr.Endpoint, clientErr.HTTPResponseCode, code, msg).
		withErrors(descs)

	switch {
	case errors.Is(clientErr.InnerError, httpqueryclient.ErrTimeout):
		qErr.cause.cause = ErrTimeout
	case errors.Is(clientErr.InnerError, context.Canceled):
		qErr.cause.cause = context.Canceled
	case errors.Is(clientErr.InnerError, context.DeadlineExceeded):
		qErr.cause.cause = context.DeadlineExceeded
	}

	return qErr
}
