GOLINT := golangci-lint

.PHONY: build test

build:
	go build -o conduit-connector-google-sheets cmd/connector/main.go
	go build -o google-token-gen cmd/tokengen/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	$(GOLINT) run --timeout=5m -c .golangci.yml
