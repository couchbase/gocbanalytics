package cbcolumnar

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

type scopeClient interface {
	Name() string
	QueryClient() queryClient
}

type gocbcoreScopeClient struct {
	agent                     *gocbcore.ColumnarAgent
	name                      string
	databaseName              string
	defaultServerQueryTimeout time.Duration
	defaultUnmarshaler        Unmarshaler
}

func newGocbcoreScopeClient(agent *gocbcore.ColumnarAgent, name, databaseName string,
	defaultServerQueryTimeout time.Duration, defaultUnmarshaler Unmarshaler) *gocbcoreScopeClient {
	return &gocbcoreScopeClient{
		agent:                     agent,
		name:                      name,
		databaseName:              databaseName,
		defaultServerQueryTimeout: defaultServerQueryTimeout,
		defaultUnmarshaler:        defaultUnmarshaler,
	}
}

func (c *gocbcoreScopeClient) Name() string {
	return c.name
}

func (c *gocbcoreScopeClient) QueryClient() queryClient {
	return newGocbcoreQueryClient(c.agent, c.defaultServerQueryTimeout, c.defaultUnmarshaler,
		&gocbcoreQueryClientNamespace{
			Database: c.databaseName,
			Scope:    c.name,
		})
}
