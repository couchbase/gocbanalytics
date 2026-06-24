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
	SetCredential(credential Credential) error

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

	scheme             string
	credentials        *credentialStore
	serverQueryTimeout time.Duration
	unmarshaler        Unmarshaler
	logger             Logger
	maxRetries         uint32
}

func newHTTPClusterClient(opts clusterClientOptions) (*httpClusterClient, error) {
	credentials := newCredentialStore(opts.Credential)

	var tlsConfig *tls.Config

	if opts.Scheme == "https" {
		trustOnly := opts.TrustOnly
		if trustOnly == nil {
			trustOnly = trustCapellaAndSystem{}
		}

		var pool *x509.CertPool

		switch to := trustOnly.(type) {
		case TrustOnlyCapella:
			pool = x509.NewCertPool()
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

		tlsConfig = createTLSConfig(opts.Address.Host, pool)
		// Always set GetClientCertificate so that certificate auth works if the credential is changed at
		// runtime via SetCredential. When the active credential is not a CertificateCredential an empty
		// certificate is returned, which is the same behavior as not setting this callback at all.
		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if certCred, ok := credentials.get().(*CertificateCredential); ok && certCred.ClientCertificate != nil {
				return certCred.ClientCertificate, nil
			}

			return &tls.Certificate{
				Certificate:                  nil,
				PrivateKey:                   nil,
				SupportedSignatureAlgorithms: nil,
				OCSPStaple:                   nil,
				SignedCertificateTimestamps:  nil,
				Leaf:                         nil,
			}, nil
		}
	}

	clientOpts := httpqueryclient.ClientConfig{
		TLSConfig:      tlsConfig,
		Logger:         opts.Logger,
		ConnectTimeout: opts.ConnectTimeout,
	}

	client := httpqueryclient.NewClient(opts.Scheme, opts.Address.Host, opts.Address.Port, clientOpts)

	return &httpClusterClient{
		scheme:             opts.Scheme,
		credentials:        credentials,
		client:             client,
		serverQueryTimeout: opts.ServerQueryTimeout,
		unmarshaler:        opts.Unmarshaler,
		logger:             opts.Logger,
		maxRetries:         opts.MaxRetries,
	}, nil
}

func (c *httpClusterClient) Database(name string) databaseClient {
	return newHTTPDatabaseClient(httpDatabaseClientConfig{
		Credentials:          c.credentials,
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
		Credentials:               c.credentials,
		Client:                    c.client,
		DefaultServerQueryTimeout: c.serverQueryTimeout,
		DefaultUnmarshaler:        c.unmarshaler,
		Namespace:                 nil,
		Logger:                    c.logger,
		DefaultMaxRetries:         c.maxRetries,
	})
}

func (c *httpClusterClient) SetCredential(credential Credential) error {
	if credential == nil {
		return invalidArgumentError{
			ArgumentName: "Credential",
			Reason:       "cannot be nil",
		}
	}

	switch credential.(type) {
	case *CertificateCredential:
		if c.scheme != "https" {
			return invalidArgumentError{
				ArgumentName: "Credential",
				Reason:       "certificateCredential requires https scheme",
			}
		}
	case *JWTCredential:
		if c.scheme != "https" {
			return invalidArgumentError{
				ArgumentName: "Credential",
				Reason:       "jwtCredential requires https scheme",
			}
		}
	}

	return c.credentials.set(credential)
}

func (c *httpClusterClient) Close() error {
	err := c.client.Close()
	if err != nil {
		return fmt.Errorf("failed to close client: %s", err) // nolint: err113, errorlint
	}

	return nil
}

func createTLSConfig(endpoint string, pool *x509.CertPool) *tls.Config {
	var insecureSkipVerify bool
	if pool == nil {
		insecureSkipVerify = true
	}

	return &tls.Config{ //nolint:exhaustruct
		MinVersion:         tls.VersionTLS13,
		CipherSuites:       nil,
		RootCAs:            pool,
		InsecureSkipVerify: insecureSkipVerify,
		ServerName:         endpoint,
	}
}
