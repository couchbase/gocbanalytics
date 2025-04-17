package cbcolumnar

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/couchbaselabs/gocbconnstr"
)

// Cluster is the main entry point for the SDK.
// It is used to perform operations on the data against a Couchbase Columnar cluster.
type Cluster struct {
	client clusterClient
}

// NewCluster creates a new Cluster instance.
func NewCluster(connStr string, credential Credential, opts ...*ClusterOptions) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	if connSpec.Scheme != "couchbases" {
		return nil, invalidArgumentError{
			ArgumentName: "scheme",
			Reason:       "only couchbases scheme is supported",
		}
	}

	clusterOpts := mergeClusterOptions(opts...)

	if clusterOpts == nil {
		clusterOpts = NewClusterOptions()
	}

	connectTimeout := 10000 * time.Millisecond
	queryTimeout := 10 * time.Minute
	useSrv := true

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

	fetchOption := func(name string) (string, bool) {
		optValue := connSpec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}

		return optValue[len(optValue)-1], true
	}

	if valStr, ok := fetchOption("srv"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "srv",
				Reason:       err.Error(),
			}
		}

		useSrv = val
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
				logWarnf("cipher suite %s is insecure, it is not recommended to use this", suite)

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

	var addrs []address

	srvRecord := connSpec.SrvRecordName()

	if srvRecord == "" {
		useSrv = false
	}

	if useSrv {
		_, srvAddrs, err := net.LookupSRV("couchbases", "tcp", connSpec.Addresses[0].Host)
		if err != nil {
			if isLogRedactionLevelFull() {
				logInfof("Failed to lookup SRV record: %s", redactSystemData(err))
			} else {
				logInfof("Failed to lookup SRV record: %s", err)
			}
		}

		for _, srvAddrs := range srvAddrs {
			addrs = append(addrs, address{
				Host: strings.TrimSuffix(srvAddrs.Target, "."),
				Port: int(srvAddrs.Port),
			})
		}
	} else {
		for _, addr := range connSpec.Addresses {
			addrs = append(addrs, address{
				Host: addr.Host,
				Port: addr.Port,
			})
		}
	}

	unmarshaler := clusterOpts.Unmarshaler
	if unmarshaler == nil {
		unmarshaler = NewJSONUnmarshaler()
	}

	if clusterOpts.SecurityOptions.DisableServerCertificateVerification != nil && *clusterOpts.SecurityOptions.DisableServerCertificateVerification {
		logWarnf("server certificate verification is disabled, this is insecure")
	}

	mgr, err := newClusterClient(clusterClientOptions{
		Spec:                                 connSpec,
		Credential:                           &credential,
		ConnectTimeout:                       connectTimeout,
		ServerQueryTimeout:                   queryTimeout,
		TrustOnly:                            securityOpts.TrustOnly,
		DisableServerCertificateVerification: securityOpts.DisableServerCertificateVerification,
		CipherSuites:                         cipherSuites,
		DisableSrv:                           !useSrv,
		Addresses:                            addrs,
		Unmarshaler:                          unmarshaler,
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
