GOLINT := golangci-lint

.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-google-sheets.version=${VERSION}'" -o conduit-connector-google-sheets cmd/connector/main.go
	go build -o google-token-gen cmd/tokengen/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	$(GOLINT) run --timeout=5m -c .golangci.yml
