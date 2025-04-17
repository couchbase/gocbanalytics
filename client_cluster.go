package ganalytics

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/couchbaselabs/gocbconnstr"

	"github.com/couchbase/ganalytics/internal/httpqueryclient"
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
	Address                              address
	Unmarshaler                          Unmarshaler
	Logger                               Logger
}

func newClusterClient(opts clusterClientOptions) (clusterClient, error) {
	return newHTTPClusterClient(opts)
}

type httpClusterClient struct {
	client *httpqueryclient.Client

	serverQueryTimeout time.Duration
	unmarshaler        Unmarshaler
	logger             Logger
}

func newHTTPClusterClient(opts clusterClientOptions) (*httpClusterClient, error) {
	port := opts.Address.Port
	if port == -1 {
		port = 11207
	}

	addr := fmt.Sprintf("%s:%d", opts.Address.Host, port)

	trustOnly := opts.TrustOnly
	if trustOnly == nil {
		trustOnly = TrustOnlyCapella{}
	}

	var pool *x509.CertPool
	switch to := trustOnly.(type) {
	case TrustOnlyCapella:
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(capellaRootCA)
	case TrustOnlySystem:
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to read system cert pool %w", err)
		}

		pool = certPool
	case TrustOnlyPemFile:
		data, err := os.ReadFile(to.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read pem file %w", err)
		}

		pool = x509.NewCertPool()
		pool.AppendCertsFromPEM(data)
	case TrustOnlyPemString:
		pool = x509.NewCertPool()
		pool.AppendCertsFromPEM([]byte(to.Pem))
	case TrustOnlyCertificates:
		pool = to.Certificates
	}

	if opts.DisableServerCertificateVerification != nil && *opts.DisableServerCertificateVerification {
		pool = nil
	}

	clientOpts := httpqueryclient.ClientConfig{
		TLSConfig: createTlsConfig(opts.CipherSuites, pool, opts.Logger),
	}

	client := httpqueryclient.NewClient(addr, clientOpts)

	return &httpClusterClient{
		client:             client,
		serverQueryTimeout: opts.ServerQueryTimeout,
		unmarshaler:        opts.Unmarshaler,
		logger:             opts.Logger,
	}, nil
}

func (c *httpClusterClient) Database(name string) databaseClient {
	return newHTTPDatabaseClient(httpDatabaseClientConfig{
		Client:               c.client,
		Name:                 name,
		DefaultServerTimeout: c.serverQueryTimeout,
		DefaultUnmarshaler:   c.unmarshaler,
		Logger:               c.logger,
	})
}

func (c *httpClusterClient) QueryClient() queryClient {
	return newHTTPQueryClient(httpQueryClientConfig{
		Client:                    c.client,
		DefaultServerQueryTimeout: c.serverQueryTimeout,
		DefaultUnmarshaler:        c.unmarshaler,
		Namespace:                 nil,
		Logger:                    c.logger,
	})
}

func (c *httpClusterClient) Close() error {
	err := c.client.Close()
	if err != nil {
		return fmt.Errorf("failed to close client: %s", err) // nolint: err113, errorlint
	}

	return nil
}

func createTlsConfig(cipherSuite []*tls.CipherSuite, pool *x509.CertPool, logger Logger) *tls.Config {
	var suites []uint16

	if cipherSuite != nil {
		suites = make([]uint16, len(cipherSuite))
		for i, suite := range cipherSuite {
			var s uint16
			for _, suiteID := range tls.CipherSuites() {
				if suite.Name == suiteID.Name {
					s = suiteID.ID
					break
				}
			}
			for _, suiteID := range tls.InsecureCipherSuites() {
				if suite.Name == suiteID.Name {
					s = suiteID.ID
					break
				}
			}

			if s > 0 {
				suites[i] = s
			} else {
				logger.Warn("Unknown cipher suite %s, ignoring", suite.Name)
			}
		}
	}

	var insecureSkipVerify bool
	if pool == nil {
		insecureSkipVerify = true
	}

	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		CipherSuites:       suites,
		RootCAs:            pool,
		InsecureSkipVerify: insecureSkipVerify,
	}
}
