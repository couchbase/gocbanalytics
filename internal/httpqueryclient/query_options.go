package httpqueryclient

// QueryOptions is the set of options available to an Analytics query.
type QueryOptions struct {
	// Payload represents the JSON payload to be sent to the query server.
	Payload map[string]interface{}

	// CredentialProvider is a function that returns the username and password for authentication.
	CredentialProvider func() (string, string)

	// MaxRetries specifies the maximum number of retries that a query will be attempted.
	MaxRetries uint32
}
