package cbanalytics

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/couchbase/gocbanalytics/internal/httpqueryclient"
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
	Scheme                               string
	Credential                           Credential
	ConnectTimeout                       time.Duration
	ServerQueryTimeout                   time.Duration
	TrustOnly                            TrustOnly
	DisableServerCertificateVerification *bool
	Address                              address
	Unmarshaler                          Unmarshaler
	Logger                               Logger
	MaxRetries                           uint32
}

func newClusterClient(opts clusterClientOptions) (clusterClient, error) {
	return newHTTPClusterClient(opts)
}

type httpClusterClient struct {
	client *httpqueryclient.Client

	credential         Credential
	serverQueryTimeout time.Duration
	unmarshaler        Unmarshaler
	logger             Logger
	maxRetries         uint32
}

func newHTTPClusterClient(opts clusterClientOptions) (*httpClusterClient, error) {
	trustOnly := opts.TrustOnly
	if trustOnly == nil {
		trustOnly = trustCapellaAndSystem{}
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
	case trustCapellaAndSystem:
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to read system cert pool %w", err)
		}

		certPool.AppendCertsFromPEM(capellaRootCA)
		pool = certPool
	}

	if opts.DisableServerCertificateVerification != nil && *opts.DisableServerCertificateVerification {
		pool = nil
	}

	clientOpts := httpqueryclient.ClientConfig{
		TLSConfig:      createTLSConfig(opts.Address.Host, pool),
		Logger:         opts.Logger,
		ConnectTimeout: opts.ConnectTimeout,
	}

	client := httpqueryclient.NewClient(opts.Scheme, opts.Address.Host, opts.Address.Port, clientOpts)

	return &httpClusterClient{
		credential:         opts.Credential,
		client:             client,
		serverQueryTimeout: opts.ServerQueryTimeout,
		unmarshaler:        opts.Unmarshaler,
		logger:             opts.Logger,
		maxRetries:         opts.MaxRetries,
	}, nil
}

func (c *httpClusterClient) Database(name string) databaseClient {
	return newHTTPDatabaseClient(httpDatabaseClientConfig{
		Credential:           c.credential,
		Client:               c.client,
		Name:                 name,
		DefaultServerTimeout: c.serverQueryTimeout,
		DefaultUnmarshaler:   c.unmarshaler,
		Logger:               c.logger,
		DefaultMaxRetries:    c.maxRetries,
	})
}

func (c *httpClusterClient) QueryClient() queryClient {
	return newHTTPQueryClient(httpQueryClientConfig{
		Credential:                c.credential,
		Client:                    c.client,
		DefaultServerQueryTimeout: c.serverQueryTimeout,
		DefaultUnmarshaler:        c.unmarshaler,
		Namespace:                 nil,
		Logger:                    c.logger,
		DefaultMaxRetries:         c.maxRetries,
	})
}

func (c *httpClusterClient) Close() error {
	err := c.client.Close()
	if err != nil {
		return fmt.Errorf("failed to close client: %s", err) // nolint: err113, errorlint
	}

	return nil
}

func createTLSConfig(endpoint string, pool *x509.CertPool) *tls.Config {
	var suites []uint16

	var insecureSkipVerify bool
	if pool == nil {
		insecureSkipVerify = true
	}

	return &tls.Config{ //nolint:exhaustruct
		MinVersion:         tls.VersionTLS13,
		CipherSuites:       suites,
		RootCAs:            pool,
		InsecureSkipVerify: insecureSkipVerify,
		ServerName:         endpoint,
	}
}
