package cbcolumnar

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

type databaseClient interface {
	Name() string
	Scope(name string) scopeClient
}

type gocbcoreDatabaseClient struct {
	agent                     *gocbcore.ColumnarAgent
	name                      string
	defaultServerQueryTimeout time.Duration
	defaultUnmarshaler        Unmarshaler
}

func newGocbcoreDatabaseClient(agent *gocbcore.ColumnarAgent, name string, defaultServerQueryTimeout time.Duration, defaultUnmarshaler Unmarshaler) *gocbcoreDatabaseClient {
	return &gocbcoreDatabaseClient{
		agent:                     agent,
		name:                      name,
		defaultServerQueryTimeout: defaultServerQueryTimeout,
		defaultUnmarshaler:        defaultUnmarshaler,
	}
}

func (c *gocbcoreDatabaseClient) Name() string {
	return c.name
}

func (c *gocbcoreDatabaseClient) Scope(name string) scopeClient {
	return newGocbcoreScopeClient(c.agent, name, c.name, c.defaultServerQueryTimeout, c.defaultUnmarshaler)
}
