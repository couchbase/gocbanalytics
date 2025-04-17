package cbanalytics

import (
	"github.com/couchbase/gocbanalytics/internal/httpqueryclient"
	"time"
)

type databaseClient interface {
	Name() string
	Scope(name string) scopeClient
}

type httpDatabaseClient struct {
	client                    *httpqueryclient.Client
	name                      string
	defaultServerQueryTimeout time.Duration
	defaultUnmarshaler        Unmarshaler
	logger                    Logger
}

type httpDatabaseClientConfig struct {
	Client               *httpqueryclient.Client
	Name                 string
	DefaultServerTimeout time.Duration
	DefaultUnmarshaler   Unmarshaler
	Logger               Logger
}

func newHTTPDatabaseClient(cfg httpDatabaseClientConfig) *httpDatabaseClient {
	return &httpDatabaseClient{
		client:                    cfg.Client,
		name:                      cfg.Name,
		defaultServerQueryTimeout: cfg.DefaultServerTimeout,
		defaultUnmarshaler:        cfg.DefaultUnmarshaler,
		logger:                    cfg.Logger,
	}
}

func (c *httpDatabaseClient) Name() string {
	return c.name
}

func (c *httpDatabaseClient) Scope(name string) scopeClient {
	return newHTTPScopeClient(httpScopeClientConfig{
		Client:                    c.client,
		DatabaseName:              c.name,
		Name:                      name,
		DefaultServerQueryTimeout: c.defaultServerQueryTimeout,
		DefaultUnmarshaler:        c.defaultUnmarshaler,
		Logger:                    c.logger,
	})
}
