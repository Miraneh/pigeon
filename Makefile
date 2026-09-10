.PHONY: fmt lint test build run docs up down

fmt:
	gofmt -l -w .
	goimports -l -w .

lint:
	golangci-lint run

test:
	go test ./... -race -cover

build:
	go build ./...

run:
	go run ./cmd/api

# regenerate swagger docs (needs swag: go install github.com/swaggo/swag/cmd/swag@latest)
docs:
	swag init -g cmd/api/main.go -o docs

up:
	docker compose up --build

down:
	docker compose down -v
