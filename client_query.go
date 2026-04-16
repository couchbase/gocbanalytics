package cbanalytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbanalytics/internal/httpqueryclient"
)

type queryClient interface {
	Query(ctx context.Context, statement string, opts *QueryOptions) (*QueryResult, error)
	StartQuery(ctx context.Context, statement string, opts *StartQueryOptions) (*QueryHandle, error)
}

type queryHandleProvider interface {
	fetchHandleStatus(ctx context.Context, handle string) (*QueryStatus, error)
	discardHandleResults(ctx context.Context, handle string) error
	cancelHandle(ctx context.Context, requestID string) error
	streamHandleResults(ctx context.Context, handle string, unmarshaler Unmarshaler) (*QueryResult, error)
}

type queryClientNamespace struct {
	Database string
	Scope    string
}
type httpQueryClient struct {
	credentials *credentialStore
	client      *httpqueryclient.Client
	namespace   *queryClientNamespace
	logger      Logger

	defaultServerQueryTimeout time.Duration
	defaultUnmarshaler        Unmarshaler
	defaultMaxRetries         uint32
}

type httpQueryClientConfig struct {
	Credentials *credentialStore
	Client      *httpqueryclient.Client
	Namespace   *queryClientNamespace
	Logger      Logger

	DefaultServerQueryTimeout time.Duration
	DefaultUnmarshaler        Unmarshaler
	DefaultMaxRetries         uint32
}

func newHTTPQueryClient(cfg httpQueryClientConfig) *httpQueryClient {
	return &httpQueryClient{
		credentials: cfg.Credentials,
		client:      cfg.Client,
		namespace:   cfg.Namespace,
		logger:      cfg.Logger,

		defaultServerQueryTimeout: cfg.DefaultServerQueryTimeout,
		defaultUnmarshaler:        cfg.DefaultUnmarshaler,
		defaultMaxRetries:         cfg.DefaultMaxRetries,
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
		switch *opts.ScanConsistency {
		case QueryScanConsistencyNotBounded:
			execOpts["scan_consistency"] = "not_bounded"
		case QueryScanConsistencyRequestPlus:
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

	maxRetries := c.defaultMaxRetries
	if opts.MaxRetries != nil {
		maxRetries = *opts.MaxRetries
	}

	return &httpqueryclient.QueryOptions{
		Payload:     execOpts,
		AuthHandler: c.handleAuthHandler(),
		MaxRetries:  maxRetries,
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

		return newAnalyticsError(baseErr, clientErr.Statement, clientErr.Endpoint, clientErr.HTTPResponseCode, clientErr.Retries).
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
		return newQueryError(
			ErrInvalidCredential,
			clientErr.Statement,
			clientErr.Endpoint,
			clientErr.HTTPResponseCode,
			code,
			msg,
			clientErr.Retries,
		).
			withErrors(descs)
	case 21002:
		return newQueryError(
			ErrTimeout,
			clientErr.Statement,
			clientErr.Endpoint,
			clientErr.HTTPResponseCode,
			code,
			msg,
			clientErr.Retries,
		).
			withErrors(descs)
	case 23000:
		return newQueryError(
			ErrServiceUnavailable,
			clientErr.Statement,
			clientErr.Endpoint,
			clientErr.HTTPResponseCode,
			code,
			msg,
			clientErr.Retries,
		).
			withErrors(descs)
	}

	qErr := newQueryError(
		nil,
		clientErr.Statement,
		clientErr.Endpoint,
		clientErr.HTTPResponseCode,
		code,
		msg,
		clientErr.Retries,
	).
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

func (c *httpQueryClient) handleAuthHandler() func(req *http.Request) {
	return func(req *http.Request) {
		switch credential := c.credentials.get().(type) {
		case *BasicAuthCredential:
			req.SetBasicAuth(credential.UserPassPair.Username, credential.UserPassPair.Password)
		case *DynamicBasicAuthCredential:
			userPassPair := credential.Credentials()
			req.SetBasicAuth(userPassPair.Username, userPassPair.Password)
		case *JWTCredential:
			req.Header.Set("Authorization", "Bearer "+credential.Token)
		case *CertificateCredential:
		}
	}
}

func (c *httpQueryClient) StartQuery(ctx context.Context, statement string, opts *StartQueryOptions) (*QueryHandle, error) {
	clientOpts, err := c.translateStartQueryOptions(ctx, statement, opts)
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
	clientOpts.Payload["mode"] = "async"

	res, err := c.client.Query(ctx, clientOpts)
	if err != nil {
		return nil, translateClientError(err)
	}

	defer func() {
		if closeErr := res.Close(); closeErr != nil {
			c.logger.Debug("Failed to close start query response: %v", closeErr)
		}
	}()

	metaBytes, err := res.MetaData()
	if err != nil {
		return nil, translateClientError(err)
	}

	var jsonResp jsonAnalyticsResponse
	if err := json.Unmarshal(metaBytes, &jsonResp); err != nil {
		return nil, fmt.Errorf("failed to parse async query response: %w", err) //nolint:err113
	}

	if jsonResp.Handle == "" {
		return nil, newAnalyticsError(ErrAnalytics, statement, c.client.Host(), 0, 0).
			withMessage("async query response did not contain a handle")
	}

	if jsonResp.RequestID == "" {
		return nil, newAnalyticsError(ErrAnalytics, statement, c.client.Host(), 0, 0).
			withMessage("async query response did not contain a request id")
	}

	return &QueryHandle{
		handle:    jsonResp.Handle,
		requestID: jsonResp.RequestID,
		provider:  c,
	}, nil
}

func (c *httpQueryClient) translateStartQueryOptions(ctx context.Context, statement string,
	opts *StartQueryOptions) (*httpqueryclient.QueryOptions, error) {
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
		switch *opts.ScanConsistency {
		case QueryScanConsistencyNotBounded:
			execOpts["scan_consistency"] = "not_bounded"
		case QueryScanConsistencyRequestPlus:
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

	maxRetries := c.defaultMaxRetries
	if opts.MaxRetries != nil {
		maxRetries = *opts.MaxRetries
	}

	return &httpqueryclient.QueryOptions{
		Payload:     execOpts,
		AuthHandler: c.handleAuthHandler(),
		MaxRetries:  maxRetries,
	}, nil
}

type jsonHandleStatusError struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
	Retry   bool   `json:"retriable"`
}

type jsonHandleStatusResponse struct {
	Status  string                  `json:"status"`
	Handle  string                  `json:"handle,omitempty"`
	Errors  []jsonHandleStatusError `json:"errors,omitempty"`
	Metrics json.RawMessage         `json:"metrics,omitempty"`
}

func (c *httpQueryClient) translateHandleError(err error) error {
	switch {
	case errors.Is(err, httpqueryclient.ErrQueryNotFound):
		var qerr *httpqueryclient.QueryError
		if errors.As(err, &qerr) {
			return newAnalyticsError(ErrQueryNotFound, qerr.Statement, qerr.Endpoint, qerr.HTTPResponseCode, qerr.Retries).
				withMessage("query handle not found")
		}

		return newAnalyticsError(ErrQueryNotFound, "", c.client.Host(), 0, 0).
			withMessage("query handle not found")
	case errors.Is(err, httpqueryclient.ErrInvalidCredential):
		var qerr *httpqueryclient.QueryError
		if errors.As(err, &qerr) {
			return newAnalyticsError(ErrInvalidCredential, qerr.Statement, qerr.Endpoint, qerr.HTTPResponseCode, qerr.Retries)
		}

		return newAnalyticsError(ErrInvalidCredential, "", c.client.Host(), 0, 0)
	default:
		return translateClientError(err)
	}
}

func (c *httpQueryClient) fetchHandleStatus(ctx context.Context, handle string) (*QueryStatus, error) {
	respBody, err := c.client.FetchHandleStatus(ctx, handle, c.handleAuthHandler(), c.defaultMaxRetries)
	if err != nil {
		return nil, c.translateHandleError(err)
	}

	var statusResp jsonHandleStatusResponse
	if err := json.Unmarshal(respBody, &statusResp); err != nil {
		return nil, newAnalyticsError(ErrAnalytics, "", c.client.Host(), 0, 0).
			withMessage("failed to parse handle status response")
	}

	if len(statusResp.Errors) > 0 {
		return nil, c.buildHandleStatusError(&statusResp)
	}

	metrics := string(statusResp.Metrics)

	if statusResp.Status == "success" {
		return &QueryStatus{
			status:  statusResp.Status,
			metrics: metrics,
			resultHandle: &QueryResultHandle{
				handle:      statusResp.Handle,
				provider:    c,
				unmarshaler: c.defaultUnmarshaler,
			},
		}, nil
	}

	return &QueryStatus{
		resultHandle: nil,
		status:       statusResp.Status,
		metrics:      metrics,
	}, nil
}

func (c *httpQueryClient) buildHandleStatusError(statusResp *jsonHandleStatusResponse) error {
	endpoint := c.client.Host()

	if len(statusResp.Errors) == 0 {
		return newAnalyticsError(ErrAnalytics, "", endpoint, 0, 0).
			withMessage(fmt.Sprintf("query ended with status: %s", statusResp.Status))
	}

	descs := make([]analyticsErrorDesc, len(statusResp.Errors))

	var firstNonRetriable *analyticsErrorDesc

	for i, e := range statusResp.Errors {
		descs[i] = analyticsErrorDesc{
			Code:    e.Code,
			Message: e.Message,
		}

		if firstNonRetriable == nil && !e.Retry {
			firstNonRetriable = &descs[i]
		}
	}

	var code int

	var msg string

	if firstNonRetriable == nil {
		code = int(statusResp.Errors[0].Code)
		msg = statusResp.Errors[0].Message
	} else {
		code = int(firstNonRetriable.Code)
		msg = firstNonRetriable.Message
	}

	var cause error

	switch code {
	case 20000:
		cause = ErrInvalidCredential
	case 21002:
		cause = ErrTimeout
	case 23000:
		cause = ErrServiceUnavailable
	default:
		cause = ErrQuery
	}

	return newQueryError(cause, "", endpoint, 0, code, msg, 0).
		withErrors(descs)
}

func (c *httpQueryClient) discardHandleResults(ctx context.Context, handle string) error {
	if err := c.client.DiscardHandleResults(ctx, handle, c.handleAuthHandler(), c.defaultMaxRetries); err != nil {
		return c.translateHandleError(err)
	}

	return nil
}

func (c *httpQueryClient) cancelHandle(ctx context.Context, requestID string) error {
	if err := c.client.CancelHandle(ctx, requestID, c.handleAuthHandler(), c.defaultMaxRetries); err != nil {
		return c.translateHandleError(err)
	}

	return nil
}

func (c *httpQueryClient) streamHandleResults(ctx context.Context, handle string,
	unmarshaler Unmarshaler) (*QueryResult, error) {
	res, err := c.client.StreamHandleResults(ctx, handle, c.handleAuthHandler(), c.defaultMaxRetries)
	if err != nil {
		return nil, c.translateHandleError(err)
	}

	if unmarshaler == nil {
		unmarshaler = c.defaultUnmarshaler
	}

	return &QueryResult{
		reader:      c.newRowReader(res),
		unmarshaler: unmarshaler,
	}, nil
}
