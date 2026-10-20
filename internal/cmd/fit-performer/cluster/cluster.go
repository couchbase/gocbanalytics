package cluster

import (
	"time"

	cbanalytics "github.com/couchbase/gocbanalytics"
)

const DefaultHandleTimeout = 10 * time.Second

type Cluster struct {
	*cbanalytics.Cluster

	handleTimeout time.Duration
}

func New(sdkCluster *cbanalytics.Cluster, handleTimeout time.Duration) *Cluster {
	if handleTimeout == 0 {
		handleTimeout = DefaultHandleTimeout
	}

	return &Cluster{
		Cluster:       sdkCluster,
		handleTimeout: handleTimeout,
	}
}

func (c *Cluster) HandleTimeout() time.Duration {
	if c == nil {
		return 0
	}

	return c.handleTimeout
}
