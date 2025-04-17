package ganalytics

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Cluster is the main entry point for the SDK.
// It is used to perform operations on the data against a Couchbase Columnar cluster.
type Cluster struct {
	client clusterClient
}

// NewCluster creates a new Cluster instance.
func NewCluster(httpEndpoint string, credential Credential, opts ...*ClusterOptions) (*Cluster, error) {
	// This is leaking implementation detail of the client abstraction a little bit, but it's ok.
	// There's no point in overcomplicating this for the sake of perfection.
	connSpec, err := url.Parse(httpEndpoint)
	if err != nil {
		return nil, err
	}

	if connSpec.Scheme != "https" && connSpec.Scheme != "http" {
		return nil, invalidArgumentError{
			ArgumentName: "scheme",
			Reason:       "only http and https schemes are supported",
		}
	}

	var port int
	if connSpec.Port() == "" {
		if connSpec.Scheme == "https" {
			port = 443
		} else if connSpec.Scheme == "http" {
			port = 80
		}
	} else {
		thisPort, err := strconv.Atoi(connSpec.Port())
		if err != nil {
			return nil, err //nolint:err113
		}

		port = thisPort
	}

	addr := address{
		Host: connSpec.Hostname(),
		Port: port,
	}

	clusterOpts := mergeClusterOptions(opts...)

	if clusterOpts == nil {
		clusterOpts = NewClusterOptions()
	}

	logger := clusterOpts.Logger
	if logger == nil {
		logger = NewNoopLogger()
	}

	connectTimeout := 10000 * time.Millisecond
	queryTimeout := 10 * time.Minute

	timeoutOpts := clusterOpts.TimeoutOptions
	if timeoutOpts == nil {
		timeoutOpts = NewTimeoutOptions()
	}

	securityOpts := clusterOpts.SecurityOptions
	if securityOpts == nil {
		securityOpts = NewSecurityOptions()
	}

	if timeoutOpts.ConnectTimeout != nil {
		connectTimeout = *timeoutOpts.ConnectTimeout
	}

	if timeoutOpts.QueryTimeout != nil {
		queryTimeout = *timeoutOpts.QueryTimeout
	}

	query, err := url.ParseQuery(connSpec.RawQuery)
	if err != nil {
		return nil, err //nolint:err113
	}

	fetchOption := func(name string) (string, bool) {
		hasName := query.Has(name)
		if !hasName {
			return "", false
		}

		return query.Get(name), true
	}

	if valStr, ok := fetchOption("timeout.connect_timeout"); ok {
		duration, err := time.ParseDuration(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "timeout.connect_timeout",
				Reason:       err.Error(),
			}
		}

		connectTimeout = duration
	}

	if valStr, ok := fetchOption("timeout.query_timeout"); ok {
		duration, err := time.ParseDuration(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "timeout.query_timeout",
				Reason:       err.Error(),
			}
		}

		queryTimeout = duration
	}

	if valStr, ok := fetchOption("security.trust_only_pem_file"); ok {
		securityOpts.TrustOnly = TrustOnlyPemFile{
			Path: valStr,
		}
	}

	if valStr, ok := fetchOption("security.disable_server_certificate_verification"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "disable_server_certificate_verification",
				Reason:       err.Error(),
			}
		}

		securityOpts.DisableServerCertificateVerification = &val
	}

	if valStr, ok := fetchOption("security.cipher_suites"); ok {
		split := strings.Split(valStr, ",")

		securityOpts.CipherSuites = split
	}

	cipherSuites := make([]*tls.CipherSuite, len(securityOpts.CipherSuites))

	for i, suite := range securityOpts.CipherSuites {
		var s *tls.CipherSuite

		for _, supportedSuite := range tls.CipherSuites() {
			if supportedSuite.Name == suite {
				s = supportedSuite

				break
			}
		}

		for _, unsupportedSuite := range tls.InsecureCipherSuites() {
			if unsupportedSuite.Name == suite {
				logger.Warn("cipher suite %s is insecure, it is not recommended to use this", suite)

				s = unsupportedSuite

				break
			}
		}

		if s == nil {
			return nil, invalidArgumentError{
				ArgumentName: "CipherSuites",
				Reason:       fmt.Sprintf("unsupported cipher suite %s", suite),
			}
		}

		cipherSuites[i] = s
	}

	if connectTimeout == 0 {
		return nil, invalidArgumentError{
			ArgumentName: "ConnectTimeout",
			Reason:       "must be greater than 0",
		}
	}

	if queryTimeout == 0 {
		return nil, invalidArgumentError{
			ArgumentName: "QueryTimeout",
			Reason:       "must be greater than 0",
		}
	}

	unmarshaler := clusterOpts.Unmarshaler
	if unmarshaler == nil {
		unmarshaler = NewJSONUnmarshaler()
	}

	if clusterOpts.SecurityOptions.DisableServerCertificateVerification != nil && *clusterOpts.SecurityOptions.DisableServerCertificateVerification {
		logger.Warn("server certificate verification is disabled, this is insecure")
	}

	mgr, err := newClusterClient(clusterClientOptions{
		Credential:                           &credential,
		ConnectTimeout:                       connectTimeout,
		ServerQueryTimeout:                   queryTimeout,
		TrustOnly:                            securityOpts.TrustOnly,
		DisableServerCertificateVerification: securityOpts.DisableServerCertificateVerification,
		CipherSuites:                         cipherSuites,
		Address:                              addr,
		Unmarshaler:                          unmarshaler,
		Logger:                               logger,
	})
	if err != nil {
		return nil, err
	}

	c := &Cluster{
		client: mgr,
	}

	return c, nil
}

// Close shuts down the cluster and releases all resources.
func (c *Cluster) Close() error {
	return c.client.Close()
}
