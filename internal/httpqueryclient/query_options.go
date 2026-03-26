package httpqueryclient

import "net/http"

// QueryOptions is the set of options available to an Analytics query.
type QueryOptions struct {
	// Payload represents the JSON payload to be sent to the query server.
	Payload map[string]interface{}

	// AuthHandler applies authentication to an outgoing HTTP request.
	AuthHandler func(req *http.Request)

	// MaxRetries specifies the maximum number of retries that a query will be attempted.
	MaxRetries uint32
}
