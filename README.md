# Couchbase Go Columnar Client

Go client for [Couchbase](https://couchbase.com) Columnar

## Useful Links
### Documentation
You can explore our API reference through godoc at [https://pkg.go.dev/github.com/couchbase/gocbcolumnar](https://pkg.go.dev/github.com/couchbase/gocbcolumnar).

You can also find documentation for the Go Columnar SDK on the [official Couchbase docs](https://docs.couchbase.com/go-columnar-sdk/current/hello-world/overview.html).

## Installing

To install the latest stable version, run:
```bash
go get github.com/couchbase/gocbcolumnar@latest
```

To install the latest developer version, run:
```bash
go get github.com/couchbase/gocbcolumnar@main
```

## Getting Started

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbaselabs/gocbcolumnar"
)

func main() {
	const (
		connStr  = "couchbases://..."
		username = "..."
		password = "..."
	)
	
	cluster, err := cbcolumnar.NewCluster(
		connStr,
		cbcolumnar.NewCredential(username, password),
		// The third parameter is optional.
		// This example sets the default server query timeout to 3 minutes,
		// that is the timeout value sent to the query server.
		cbcolumnar.NewClusterOptions().SetTimeoutOptions(
			cbcolumnar.NewTimeoutOptions().SetQueryTimeout(3*time.Minute),
		),
	)
	handleErr(err)

	// We create a new context with a timeout of 2 minutes.
	// This context will apply timeout/cancellation on the client side.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	printRows := func(result *cbcolumnar.QueryResult) {
		for row := result.NextRow(); row != nil; row = result.NextRow() {
			var content map[string]interface{}

			err = row.ContentAs(&content)
			handleErr(err)

			fmt.Printf("Got row content: %v", content)
		}
	}

	// Execute a query and process rows as they arrive from server.
	// Results are always streamed from the server.
	result, err := cluster.ExecuteQuery(ctx, "select 1")
	handleErr(err)

	printRows(result)

	// Execute a query with positional arguments.
	result, err = cluster.ExecuteQuery(
		ctx,
		"select ?=1",
		cbcolumnar.NewQueryOptions().SetPositionalParameters([]interface{}{1}),
	)
	handleErr(err)

	printRows(result)

	// Execute a query with named arguments.
	result, err = cluster.ExecuteQuery(
		ctx,
		"select $foo=1",
		cbcolumnar.NewQueryOptions().SetNamedParameters(map[string]interface{}{"foo": 1}),
	)
	handleErr(err)

	printRows(result)

	err = cluster.Close()
	handleErr(err)
}

func handleErr(err error) {
	if err != nil {
		panic(err)
	}
}

```

## Testing

You can run tests in the usual Go way:

`go test -race ./... --connstr couchbases://... --username ... --password ... --database ... --scope ...`

Which will execute tests against the specified Columnar instance.
See the `testmain_test.go` file for more information on command line arguments.

## Linting

Linting is performed used `golangci-lint`.
To run:

`golangci-lint run`

## License
Copyright 2025 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See
[LICENSE](https://github.com/couchbase/gocbcolumnar/blob/main/LICENSE)
for further details.
