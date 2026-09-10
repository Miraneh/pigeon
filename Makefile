.PHONY: fmt lint test build

fmt:
	gofmt -l -w .
	goimports -l -w .

lint:
	golangci-lint run

test:
	go test ./... -race -cover

build:
	go build ./...
