.PHONY: fmt lint test build build-server build-reaper run docs \
	images image-server image-reaper up down

fmt:
	gofmt -l -w .
	goimports -l -w .

lint:
	golangci-lint run

test:
	go test ./... -race -cover

build: build-server build-reaper

build-server:
	go build -trimpath -o bin/server ./server

build-reaper:
	go build -trimpath -o bin/reaper ./reaper

run:
	go run ./server

# regenerate swagger docs (needs swag: go install github.com/swaggo/swag/cmd/swag@latest)
docs:
	swag init -g server/main.go -o docs

images: image-server image-reaper

image-server:
	docker build -f production/Dockerfile --target server -t pigeon-server:latest .

image-reaper:
	docker build -f production/Dockerfile --target reaper -t pigeon-reaper:latest .

up:
	docker compose -f production/docker-compose.yml up --build

down:
	docker compose -f production/docker-compose.yml down -v
