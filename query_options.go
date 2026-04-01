package cbanalytics

// QueryScanConsistency indicates the level of data consistency desired for an analytics query.
type QueryScanConsistency uint

const (
	// QueryScanConsistencyNotBounded indicates no data consistency is required.
	QueryScanConsistencyNotBounded QueryScanConsistency = iota + 1
	// QueryScanConsistencyRequestPlus indicates that request-level data consistency is required.
	QueryScanConsistencyRequestPlus
)

// QueryOptions is the set of options available to an Analytics query.
type QueryOptions struct {
	// ClientContextID is an optional identifier for the query.
	ClientContextID *string

	// PositionalParameters sets any positional placeholder parameters for the query.
	PositionalParameters []interface{}

	// NamedParameters sets any positional placeholder parameters for the query.
	NamedParameters map[string]interface{}

	// ReadOnly sets whether this query should be read-only.
	ReadOnly *bool

	// ScanConsistency specifies the level of data consistency required for this query.
	ScanConsistency *QueryScanConsistency

	// Raw provides a way to provide extra parameters in the request body for the query.
	Raw map[string]interface{}

	// Unmarshaler specifies the default unmarshaler to use for decoding rows from this query.
	Unmarshaler Unmarshaler

	// MaxRetries specifies the maximum number of retries that a query will be attempted.
	// This includes connection attempts.
	// VOLATILE: This API is subject to change at any time.
	MaxRetries *uint32
}

// NewQueryOptions creates a new instance of QueryOptions.
func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		ClientContextID:      nil,
		PositionalParameters: nil,
		NamedParameters:      nil,
		ReadOnly:             nil,
		ScanConsistency:      nil,
		Raw:                  nil,
		Unmarshaler:          nil,
		MaxRetries:           nil,
	}
}

// SetClientContextID sets the ClientContextID field in QueryOptions.
func (opts *QueryOptions) SetClientContextID(clientContextID string) *QueryOptions {
	opts.ClientContextID = &clientContextID

	return opts
}

// SetPositionalParameters sets the PositionalParameters field in QueryOptions.
func (opts *QueryOptions) SetPositionalParameters(params []interface{}) *QueryOptions {
	opts.PositionalParameters = params

	return opts
}

// SetNamedParameters sets the NamedParameters field in QueryOptions.
func (opts *QueryOptions) SetNamedParameters(params map[string]interface{}) *QueryOptions {
	opts.NamedParameters = params

	return opts
}

// SetReadOnly sets the ReadOnly field in QueryOptions.
func (opts *QueryOptions) SetReadOnly(readOnly bool) *QueryOptions {
	opts.ReadOnly = &readOnly

	return opts
}

// SetScanConsistency sets the ScanConsistency field in QueryOptions.
func (opts *QueryOptions) SetScanConsistency(scanConsistency QueryScanConsistency) *QueryOptions {
	opts.ScanConsistency = &scanConsistency

	return opts
}

// SetRaw sets the Raw field in QueryOptions.
func (opts *QueryOptions) SetRaw(raw map[string]interface{}) *QueryOptions {
	opts.Raw = raw

	return opts
}

// SetUnmarshaler sets the Unmarshaler field in QueryOptions.
func (opts *QueryOptions) SetUnmarshaler(unmarshaler Unmarshaler) *QueryOptions {
	opts.Unmarshaler = unmarshaler

	return opts
}

// SetMaxRetries sets the MaxRetries field in QueryOptions.
// VOLATILE: This API is subject to change at any time.
func (opts *QueryOptions) SetMaxRetries(maxRetries uint32) *QueryOptions {
	opts.MaxRetries = &maxRetries

	return opts
}

// StartQueryOptions is the set of options available to an Analytics query.
type StartQueryOptions struct {
	// ClientContextID is an optional identifier for the query.
	ClientContextID *string

	// PositionalParameters sets any positional placeholder parameters for the query.
	PositionalParameters []interface{}

	// NamedParameters sets any named parameters for the query.
	NamedParameters map[string]interface{}

	// ReadOnly sets whether this query should be read-only.
	ReadOnly *bool

	// ScanConsistency specifies the level of data consistency required for this query.
	ScanConsistency *QueryScanConsistency

	// Raw provides a way to provide extra parameters in the request body for the query.
	Raw map[string]interface{}

	// MaxRetries specifies the maximum number of retries that a query will be attempted.
	// This includes connection attempts.
	// VOLATILE: This API is subject to change at any time.
	MaxRetries *uint32
}

// NewStartQueryOptions creates a new instance of StartQueryOptions.
func NewStartQueryOptions() *StartQueryOptions {
	return &StartQueryOptions{
		ClientContextID:      nil,
		PositionalParameters: nil,
		NamedParameters:      nil,
		ReadOnly:             nil,
		ScanConsistency:      nil,
		Raw:                  nil,
		MaxRetries:           nil,
	}
}

// SetClientContextID sets the ClientContextID field in StartQueryOptions.
func (opts *StartQueryOptions) SetClientContextID(clientContextID string) *StartQueryOptions {
	opts.ClientContextID = &clientContextID

	return opts
}

// SetPositionalParameters sets the PositionalParameters field in StartQueryOptions.
func (opts *StartQueryOptions) SetPositionalParameters(params []interface{}) *StartQueryOptions {
	opts.PositionalParameters = params

	return opts
}

// SetNamedParameters sets the NamedParameters field in StartQueryOptions.
func (opts *StartQueryOptions) SetNamedParameters(params map[string]interface{}) *StartQueryOptions {
	opts.NamedParameters = params

	return opts
}

// SetReadOnly sets the ReadOnly field in StartQueryOptions.
func (opts *StartQueryOptions) SetReadOnly(readOnly bool) *StartQueryOptions {
	opts.ReadOnly = &readOnly

	return opts
}

// SetScanConsistency sets the ScanConsistency field in StartQueryOptions.
func (opts *StartQueryOptions) SetScanConsistency(scanConsistency QueryScanConsistency) *StartQueryOptions {
	opts.ScanConsistency = &scanConsistency

	return opts
}

// SetRaw sets the Raw field in StartQueryOptions.
func (opts *StartQueryOptions) SetRaw(raw map[string]interface{}) *StartQueryOptions {
	opts.Raw = raw

	return opts
}

// SetMaxRetries sets the MaxRetries field in StartQueryOptions.
// VOLATILE: This API is subject to change at any time.
func (opts *StartQueryOptions) SetMaxRetries(maxRetries uint32) *StartQueryOptions {
	opts.MaxRetries = &maxRetries

	return opts
}

func mergeStartQueryOptions(opts ...*StartQueryOptions) *StartQueryOptions {
	startOpts := &StartQueryOptions{
		ClientContextID:      nil,
		PositionalParameters: nil,
		NamedParameters:      nil,
		ReadOnly:             nil,
		ScanConsistency:      nil,
		Raw:                  nil,
		MaxRetries:           nil,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		if opt.ClientContextID != nil {
			startOpts.ClientContextID = opt.ClientContextID
		}

		if opt.ScanConsistency != nil {
			startOpts.ScanConsistency = opt.ScanConsistency
		}

		if opt.ReadOnly != nil {
			startOpts.ReadOnly = opt.ReadOnly
		}

		if len(opt.PositionalParameters) > 0 {
			startOpts.PositionalParameters = opt.PositionalParameters
		}

		if len(opt.NamedParameters) > 0 {
			startOpts.NamedParameters = opt.NamedParameters
		}

		if len(opt.Raw) > 0 {
			startOpts.Raw = opt.Raw
		}

		if opt.MaxRetries != nil {
			startOpts.MaxRetries = opt.MaxRetries
		}
	}

	return startOpts
}

// FetchResultsOptions is the set of options available to a FetchResults operation on a QueryResultHandle.
type FetchResultsOptions struct {
	// Unmarshaler specifies the unmarshaler to use for decoding rows from this result.
	Unmarshaler Unmarshaler
}

// NewFetchResultOptions creates a new instance of FetchResultsOptions.
func NewFetchResultOptions() *FetchResultsOptions {
	return &FetchResultsOptions{
		Unmarshaler: nil,
	}
}

// SetUnmarshaler sets the Unmarshaler field in FetchResultsOptions.
func (opts *FetchResultsOptions) SetUnmarshaler(unmarshaler Unmarshaler) *FetchResultsOptions {
	opts.Unmarshaler = unmarshaler

	return opts
}
