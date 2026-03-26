package cbanalytics

import (
	"time"

	"github.com/couchbase/gocbanalytics/internal/httpqueryclient"
)

type scopeClient interface {
	Name() string
	QueryClient() queryClient
}

type httpScopeClient struct {
	credentials  *credentialStore
	client       *httpqueryclient.Client
	name         string
	databaseName string
	logger       Logger

	defaultServerQueryTimeout time.Duration
	defaultUnmarshaler        Unmarshaler
	defaultMaxRetries         uint32
}

type httpScopeClientConfig struct {
	Credentials  *credentialStore
	Client       *httpqueryclient.Client
	DatabaseName string
	Name         string
	Logger       Logger

	DefaultServerQueryTimeout time.Duration
	DefaultUnmarshaler        Unmarshaler
	DefaultMaxRetries         uint32
}

func newHTTPScopeClient(cfg httpScopeClientConfig) *httpScopeClient {
	return &httpScopeClient{
		credentials:  cfg.Credentials,
		client:       cfg.Client,
		name:         cfg.Name,
		databaseName: cfg.DatabaseName,
		logger:       cfg.Logger,

		defaultServerQueryTimeout: cfg.DefaultServerQueryTimeout,
		defaultUnmarshaler:        cfg.DefaultUnmarshaler,
		defaultMaxRetries:         cfg.DefaultMaxRetries,
	}
}

func (c *httpScopeClient) Name() string {
	return c.name
}

func (c *httpScopeClient) QueryClient() queryClient {
	return newHTTPQueryClient(httpQueryClientConfig{
		Credentials: c.credentials,
		Client:      c.client,
		Namespace: &queryClientNamespace{
			Database: c.databaseName,
			Scope:    c.name,
		},
		Logger: c.logger,

		DefaultServerQueryTimeout: c.defaultServerQueryTimeout,
		DefaultUnmarshaler:        c.defaultUnmarshaler,
		DefaultMaxRetries:         c.defaultMaxRetries,
	})
}
