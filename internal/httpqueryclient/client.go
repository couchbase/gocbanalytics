// Package httpqueryclient implements an HTTP client for making requests against a server.
package httpqueryclient

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/couchbase/gocbanalytics/internal/logging"
)

// ClientConfig holds the configuration for the client.
type ClientConfig struct {
	TLSConfig      *tls.Config
	Logger         logging.Logger
	ConnectTimeout time.Duration
}

// Client represents an HTTP client that can be used to make requests to the server.
type Client struct {
	scheme      string
	host        string
	port        int
	innerClient *http.Client
	resolver    *net.Resolver
	logger      logging.Logger
}

// NewClient creates a new Client with the given endpoint and configuration.
func NewClient(scheme string, host string, port int, config ClientConfig) *Client {
	client, resolver := createHTTPClient(config.TLSConfig, config.ConnectTimeout)

	return &Client{
		scheme:      scheme,
		host:        host,
		port:        port,
		innerClient: client,
		resolver:    resolver,
		logger:      config.Logger,
	}
}

// Host returns the host that the client is connected to.
func (c *Client) Host() string {
	return c.host
}

// Close closes the client and releases any resources it holds.
func (c *Client) Close() error {
	if tsport, ok := c.innerClient.Transport.(*http.Transport); ok {
		tsport.CloseIdleConnections()
	}

	return nil
}

func createHTTPClient(tlsConfig *tls.Config, connectTimeout time.Duration) (*http.Client, *net.Resolver) {
	resolver := net.DefaultResolver

	httpDialer := &net.Dialer{ //nolint:exhaustruct
		Timeout:   connectTimeout,
		KeepAlive: 30 * time.Second,
		Resolver:  resolver,
	}

	// We set ForceAttemptHTTP2, which will update the base-config to support HTTP2
	// automatically, so that all configs from it will look for that.
	httpTransport := &http.Transport{ //nolint:exhaustruct
		ForceAttemptHTTP2: true,

		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return httpDialer.DialContext(ctx, network, addr)
		},

		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: 0,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     1000 * time.Millisecond,
	}

	httpCli := &http.Client{ //nolint:exhaustruct
		Transport: httpTransport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			// We are not expected to follow any redirects
			return http.ErrUseLastResponse
		},
	}

	return httpCli, resolver
}
