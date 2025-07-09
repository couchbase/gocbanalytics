package cbanalytics

import (
	"crypto/x509"
	"time"
)

// TrustOnly specifies the trust mode to use within the SDK.
type TrustOnly interface {
	trustOnly()
}

// TrustOnlyCapella  tells the SDK to trust only the Capella CA certificate(s) bundled with the SDK.
// This is the default behavior.
type TrustOnlyCapella struct{}

func (t TrustOnlyCapella) trustOnly() {}

// TrustOnlyPemFile tells the SDK to trust only the PEM-encoded certificate(s) in the file at the given FS path.
type TrustOnlyPemFile struct {
	Path string
}

func (t TrustOnlyPemFile) trustOnly() {}

// TrustOnlyPemString tells the SDK to trust only the PEM-encoded certificate(s) in the given string.
type TrustOnlyPemString struct {
	Pem string
}

func (t TrustOnlyPemString) trustOnly() {}

// TrustOnlyCertificates tells the SDK to trust only the specified certificates.
type TrustOnlyCertificates struct {
	Certificates *x509.CertPool
}

func (t TrustOnlyCertificates) trustOnly() {}

// TrustOnlySystem tells the SDK to trust only the certificates trusted by the system cert pool.
type TrustOnlySystem struct{}

func (t TrustOnlySystem) trustOnly() {}

type trustCapellaAndSystem struct{}

func (t trustCapellaAndSystem) trustOnly() {}

// SecurityOptions specifies options for controlling security related
// items such as TLS root certificates and verification skipping.
type SecurityOptions struct {
	// TrustOnly specifies the trust mode to use within the SDK.
	TrustOnly TrustOnly

	// DisableServerCertificateVerification when specified causes the SDK to trust ANY certificate
	// regardless of validity.
	DisableServerCertificateVerification *bool
}

// NewSecurityOptions creates a new instance of SecurityOptions.
func NewSecurityOptions() *SecurityOptions {
	return &SecurityOptions{
		TrustOnly:                            TrustOnlyCapella{},
		DisableServerCertificateVerification: nil,
	}
}

// SetTrustOnly sets the TrustOnly field in SecurityOptions.
func (opts *SecurityOptions) SetTrustOnly(trustOnly TrustOnly) *SecurityOptions {
	opts.TrustOnly = trustOnly

	return opts
}

// SetDisableServerCertificateVerification sets the DisableServerCertificateVerification field in SecurityOptions.
func (opts *SecurityOptions) SetDisableServerCertificateVerification(disabled bool) *SecurityOptions {
	opts.DisableServerCertificateVerification = &disabled

	return opts
}

// TimeoutOptions specifies options for various operation timeouts.
type TimeoutOptions struct {
	// ConnectTimeout specifies the socket connection timeout, or more broadly the timeout
	// for establishing an individual authenticated connection.
	// Default = 10 seconds
	ConnectTimeout *time.Duration

	// QueryTimeout specifies the default amount of time to spend executing a query before timing it out.
	// This value is only used if the context.Context at the operation level does not specify a deadline.
	// Default = 10 minutes
	QueryTimeout *time.Duration
}

// NewTimeoutOptions creates a new instance of TimeoutOptions.
func NewTimeoutOptions() *TimeoutOptions {
	return &TimeoutOptions{
		ConnectTimeout: nil,
		QueryTimeout:   nil,
	}
}

// SetConnectTimeout sets the ConnectTimeout field in TimeoutOptions.
func (opts *TimeoutOptions) SetConnectTimeout(timeout time.Duration) *TimeoutOptions {
	opts.ConnectTimeout = &timeout

	return opts
}

// SetQueryTimeout sets the QueryTimeout field in TimeoutOptions.
func (opts *TimeoutOptions) SetQueryTimeout(timeout time.Duration) *TimeoutOptions {
	opts.QueryTimeout = &timeout

	return opts
}

// ClusterOptions specifies options for configuring the cluster.
type ClusterOptions struct {
	// TimeoutOptions specifies various operation timeouts.
	TimeoutOptions *TimeoutOptions

	// SecurityOptions specifies security related configuration options.
	SecurityOptions *SecurityOptions

	// Unmarshaler specifies the default unmarshaler to use for decoding query response rows.
	Unmarshaler Unmarshaler

	// Logger specifies the logger to use for logging.
	Logger Logger

	// MaxRetries specifies the maximum number of retries that a query will be attempted.
	// This includes connection attempts.
	// VOLATILE: This API is subject to change at any time.
	MaxRetries *uint32
}

// NewClusterOptions creates a new instance of ClusterOptions.
func NewClusterOptions() *ClusterOptions {
	return &ClusterOptions{
		TimeoutOptions: &TimeoutOptions{
			ConnectTimeout: nil,
			QueryTimeout:   nil,
		},
		SecurityOptions: &SecurityOptions{
			TrustOnly:                            TrustOnlyCapella{},
			DisableServerCertificateVerification: nil,
		},
		Unmarshaler: nil,
		Logger:      nil,
		MaxRetries:  nil,
	}
}

// SetTimeoutOptions sets the TimeoutOptions field in ClusterOptions.
func (co *ClusterOptions) SetTimeoutOptions(timeoutOptions *TimeoutOptions) *ClusterOptions {
	co.TimeoutOptions = timeoutOptions

	return co
}

// SetSecurityOptions sets the SecurityOptions field in ClusterOptions.
func (co *ClusterOptions) SetSecurityOptions(securityOptions *SecurityOptions) *ClusterOptions {
	co.SecurityOptions = securityOptions

	return co
}

// SetUnmarshaler sets the Unmarshaler field in ClusterOptions.
func (co *ClusterOptions) SetUnmarshaler(unmarshaler Unmarshaler) *ClusterOptions {
	co.Unmarshaler = unmarshaler

	return co
}

// SetLogger sets the Logger field in ClusterOptions.
func (co *ClusterOptions) SetLogger(logger Logger) *ClusterOptions {
	co.Logger = logger

	return co
}

// SetMaxRetries sets the MaxRetries field in ClusterOptions.
// VOLATILE: This API is subject to change at any time.
func (co *ClusterOptions) SetMaxRetries(maxRetries uint32) *ClusterOptions {
	co.MaxRetries = &maxRetries

	return co
}

func mergeClusterOptions(opts ...*ClusterOptions) *ClusterOptions {
	clusterOpts := &ClusterOptions{
		TimeoutOptions:  nil,
		SecurityOptions: nil,
		Unmarshaler:     nil,
		Logger:          nil,
		MaxRetries:      nil,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		if opt.TimeoutOptions != nil {
			if clusterOpts.TimeoutOptions == nil {
				clusterOpts.TimeoutOptions = &TimeoutOptions{
					ConnectTimeout: nil,
					QueryTimeout:   nil,
				}
			}

			if opt.TimeoutOptions.ConnectTimeout != nil {
				clusterOpts.TimeoutOptions.ConnectTimeout = opt.TimeoutOptions.ConnectTimeout
			}

			if opt.TimeoutOptions.QueryTimeout != nil {
				clusterOpts.TimeoutOptions.QueryTimeout = opt.TimeoutOptions.QueryTimeout
			}
		}

		if opt.SecurityOptions != nil {
			if clusterOpts.SecurityOptions == nil {
				clusterOpts.SecurityOptions = &SecurityOptions{
					TrustOnly:                            nil,
					DisableServerCertificateVerification: nil,
				}
			}

			if opt.SecurityOptions.TrustOnly != nil {
				clusterOpts.SecurityOptions.TrustOnly = opt.SecurityOptions.TrustOnly
			}

			if opt.SecurityOptions.DisableServerCertificateVerification != nil {
				clusterOpts.SecurityOptions.DisableServerCertificateVerification = opt.SecurityOptions.DisableServerCertificateVerification
			}
		}

		if opt.Unmarshaler != nil {
			clusterOpts.Unmarshaler = opt.Unmarshaler
		}

		if opt.Logger != nil {
			clusterOpts.Logger = opt.Logger
		}

		if opt.MaxRetries != nil {
			clusterOpts.MaxRetries = opt.MaxRetries
		}
	}

	return clusterOpts
}
