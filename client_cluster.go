package cbcolumnar

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbaselabs/gocbconnstr"
)

type clusterClient interface {
	QueryClient() queryClient
	Database(name string) databaseClient

	Close() error
}

type address struct {
	Host string
	Port int
}

type clusterClientOptions struct {
	Spec                                 gocbconnstr.ConnSpec
	Credential                           *Credential
	ConnectTimeout                       time.Duration
	ServerQueryTimeout                   time.Duration
	TrustOnly                            TrustOnly
	DisableServerCertificateVerification *bool
	CipherSuites                         []*tls.CipherSuite
	DisableSrv                           bool
	Addresses                            []address
	Unmarshaler                          Unmarshaler
}

func newClusterClient(opts clusterClientOptions) (clusterClient, error) {
	return newGocbcoreClusterClient(opts)
}

type gocbcoreClusterClient struct {
	agent *gocbcore.ColumnarAgent

	serverQueryTimeout time.Duration
	unmarshaler        Unmarshaler
}

func newGocbcoreClusterClient(opts clusterClientOptions) (*gocbcoreClusterClient, error) {
	addresses := make([]string, len(opts.Addresses))

	for i, addr := range opts.Addresses {
		port := addr.Port
		if port == -1 {
			port = 11207
		}

		addresses[i] = fmt.Sprintf("%s:%d", addr.Host, port)
	}

	var srvRecord *gocbcore.SRVRecord

	if !opts.DisableSrv {
		var host string
		if len(opts.Addresses) > 0 {
			host = opts.Addresses[0].Host
		}

		srvRecord = &gocbcore.SRVRecord{
			Proto:  "tcp",
			Scheme: "couchbases",
			Host:   host,
		}
	}

	trustOnly := opts.TrustOnly
	if trustOnly == nil {
		trustOnly = TrustOnlyCapella{}
	}

	var caProvider func() *x509.CertPool

	switch to := trustOnly.(type) {
	case TrustOnlyCapella:
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(capellaRootCA)

		caProvider = func() *x509.CertPool {
			return pool
		}
	case TrustOnlySystem:
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to read system cert pool %w", err)
		}

		caProvider = func() *x509.CertPool {
			return pool
		}
	case TrustOnlyPemFile:
		data, err := os.ReadFile(to.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read pem file %w", err)
		}

		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(data)

		caProvider = func() *x509.CertPool {
			return pool
		}
	case TrustOnlyPemString:
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM([]byte(to.Pem))

		caProvider = func() *x509.CertPool {
			return pool
		}
	case TrustOnlyCertificates:
		caProvider = func() *x509.CertPool {
			return to.Certificates
		}
	}

	if opts.DisableServerCertificateVerification != nil && *opts.DisableServerCertificateVerification {
		caProvider = func() *x509.CertPool {
			return nil
		}
	}

	coreOpts := &gocbcore.ColumnarAgentConfig{
		UserAgent:      Identifier(),
		ConnectTimeout: opts.ConnectTimeout,
		SeedConfig: gocbcore.ColumnarSeedConfig{
			MemdAddrs: addresses,
			SRVRecord: srvRecord,
		},
		SecurityConfig: gocbcore.ColumnarSecurityConfig{
			TLSRootCAProvider: caProvider,
			CipherSuite:       opts.CipherSuites,
			Auth: gocbcore.PasswordAuthProvider{
				Username: opts.Credential.UsernamePassword.Username,
				Password: opts.Credential.UsernamePassword.Password,
			},
		},
		ConfigPollerConfig: gocbcore.ColumnarConfigPollerConfig{
			CccpMaxWait:    0,
			CccpPollPeriod: 0,
		},
		KVConfig: gocbcore.ColumnarKVConfig{
			ConnectTimeout:       opts.ConnectTimeout,
			ServerWaitBackoff:    0,
			ConnectionBufferSize: 0,
		},
		HTTPConfig: gocbcore.ColumnarHTTPConfig{
			MaxIdleConns:          0,
			MaxIdleConnsPerHost:   0,
			MaxConnsPerHost:       0,
			IdleConnectionTimeout: 1 * time.Second,
		},
	}

	agent, err := gocbcore.CreateColumnarAgent(coreOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent: %s", err) // nolint: err113, errorlint
	}

	return &gocbcoreClusterClient{
		agent:              agent,
		serverQueryTimeout: opts.ServerQueryTimeout,
		unmarshaler:        opts.Unmarshaler,
	}, nil
}

func (c *gocbcoreClusterClient) Database(name string) databaseClient {
	return newGocbcoreDatabaseClient(c.agent, name, c.serverQueryTimeout, c.unmarshaler)
}

func (c *gocbcoreClusterClient) QueryClient() queryClient {
	return newGocbcoreQueryClient(c.agent, c.serverQueryTimeout, c.unmarshaler, nil)
}

func (c *gocbcoreClusterClient) Close() error {
	err := c.agent.Close()
	if err != nil {
		return fmt.Errorf("failed to close agent: %s", err) // nolint: err113, errorlint
	}

	return nil
}
