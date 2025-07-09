package cbanalytics

import (
	"time"

	"github.com/couchbase/gocbanalytics/internal/httpqueryclient"
)

type databaseClient interface {
	Name() string
	Scope(name string) scopeClient
}

type httpDatabaseClient struct {
	credential Credential
	client     *httpqueryclient.Client
	name       string
	logger     Logger

	defaultServerQueryTimeout time.Duration
	defaultUnmarshaler        Unmarshaler
	defaultMaxRetries         uint32
}

type httpDatabaseClientConfig struct {
	Credential Credential
	Client     *httpqueryclient.Client
	Name       string
	Logger     Logger

	DefaultServerTimeout time.Duration
	DefaultUnmarshaler   Unmarshaler
	DefaultMaxRetries    uint32
}

func newHTTPDatabaseClient(cfg httpDatabaseClientConfig) *httpDatabaseClient {
	return &httpDatabaseClient{
		credential:                cfg.Credential,
		client:                    cfg.Client,
		name:                      cfg.Name,
		defaultServerQueryTimeout: cfg.DefaultServerTimeout,
		defaultUnmarshaler:        cfg.DefaultUnmarshaler,
		logger:                    cfg.Logger,
		defaultMaxRetries:         cfg.DefaultMaxRetries,
	}
}

func (c *httpDatabaseClient) Name() string {
	return c.name
}

func (c *httpDatabaseClient) Scope(name string) scopeClient {
	return newHTTPScopeClient(httpScopeClientConfig{
		Credential:   c.credential,
		Client:       c.client,
		DatabaseName: c.name,
		Name:         name,
		Logger:       c.logger,

		DefaultServerQueryTimeout: c.defaultServerQueryTimeout,
		DefaultUnmarshaler:        c.defaultUnmarshaler,
		DefaultMaxRetries:         c.defaultMaxRetries,
	})
}
