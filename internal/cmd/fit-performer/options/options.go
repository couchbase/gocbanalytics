package options

import (
	"errors"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/query"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/unmarshal"
)

func ConvertQueryProtoOptionsToSDK(opts *query.ExecuteQueryRequest_Options) (*cbanalytics.QueryOptions, error) {
	sdkOpts := cbanalytics.NewQueryOptions()

	if opts.ParametersPositional != nil {
		params := make([]interface{}, len(opts.ParametersPositional.Values))
		for i, param := range opts.ParametersPositional.Values {
			params[i] = param
		}

		sdkOpts = sdkOpts.SetPositionalParameters(params)
	}

	if opts.ParametersNamed != nil {
		params := make(map[string]interface{})
		for key, param := range opts.ParametersNamed.Fields {
			params[key] = param
		}

		sdkOpts = sdkOpts.SetNamedParameters(params)
	}

	if opts.Readonly != nil {
		sdkOpts = sdkOpts.SetReadOnly(*opts.Readonly)
	}

	if opts.ScanConsistency != nil {
		switch *opts.ScanConsistency {
		case query.ExecuteQueryRequest_Options_SCAN_CONSISTENCY_NOT_BOUNDED:
			sdkOpts = sdkOpts.SetScanConsistency(cbanalytics.QueryScanConsistencyNotBounded)
		case query.ExecuteQueryRequest_Options_SCAN_CONSISTENCY_REQUEST_PLUS:
			sdkOpts = sdkOpts.SetScanConsistency(cbanalytics.QueryScanConsistencyRequestPlus)
		default:
			return nil, errors.New("unsupported scan consistency")
		}
	}

	if opts.Raw != nil {
		params := make(map[string]interface{})
		for key, param := range opts.Raw.Fields {
			params[key] = param
		}

		sdkOpts = sdkOpts.SetRaw(params)
	}

	if opts.Deserializer != nil {
		u, err := unmarshal.NewUnmarshaler(opts.Deserializer)
		if err != nil {
			return nil, err
		}

		sdkOpts = sdkOpts.SetUnmarshaler(u)
	}

	if opts.MaxRetries != nil {
		m := uint32(*opts.MaxRetries)
		sdkOpts = sdkOpts.SetMaxRetries(m)
	}

	return sdkOpts, nil
}

func ConvertStartQueryProtoOptionsToSDK(opts *query.StartQueryRequest_Options) (*cbanalytics.StartQueryOptions, error) {
	sdkOpts := cbanalytics.NewStartQueryOptions()

	if opts.ParametersPositional != nil {
		params := make([]interface{}, len(opts.ParametersPositional.Values))
		for i, param := range opts.ParametersPositional.Values {
			params[i] = param
		}

		sdkOpts = sdkOpts.SetPositionalParameters(params)
	}

	if opts.ParametersNamed != nil {
		params := make(map[string]interface{})
		for key, param := range opts.ParametersNamed.Fields {
			params[key] = param
		}

		sdkOpts = sdkOpts.SetNamedParameters(params)
	}

	if opts.Readonly != nil {
		sdkOpts = sdkOpts.SetReadOnly(*opts.Readonly)
	}

	if opts.ScanConsistency != nil {
		switch *opts.ScanConsistency {
		case query.StartQueryRequest_Options_SCAN_CONSISTENCY_NOT_BOUNDED:
			sdkOpts = sdkOpts.SetScanConsistency(cbanalytics.QueryScanConsistencyNotBounded)
		case query.StartQueryRequest_Options_SCAN_CONSISTENCY_REQUEST_PLUS:
			sdkOpts = sdkOpts.SetScanConsistency(cbanalytics.QueryScanConsistencyRequestPlus)
		default:
			return nil, errors.New("unsupported scan consistency")
		}
	}

	if opts.Raw != nil {
		params := make(map[string]interface{})
		for key, param := range opts.Raw.Fields {
			params[key] = param
		}

		sdkOpts = sdkOpts.SetRaw(params)
	}

	if opts.MaxRetries != nil {
		m := uint32(*opts.MaxRetries)
		sdkOpts = sdkOpts.SetMaxRetries(m)
	}

	return sdkOpts, nil
}
