devsetup:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

update-protocol:
	rm -rf proto
	mkdir -p proto
	curl --location --fail https://github.com/couchbaselabs/fit-protocol/archive/refs/heads/main.tar.gz -o fit-protocol.tar.gz
	tar -xz --strip-components=2 -C proto -f fit-protocol.tar.gz fit-protocol-main/columnar fit-protocol-main/operational/shared.echo.proto
	rm fit-protocol.tar.gz
	sed -i '' 's|github.com/couchbaselabs/transactions-fit-performer/protocol|github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol|g' proto/*.proto

generate:
	go generate .

lint: generate
	golangci-lint run -v

.PHONY: all lint generate update-protocol devsetup
