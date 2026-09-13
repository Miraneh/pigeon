# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS builder

WORKDIR /src

# go.mod/go.sum copied first so this layer only reruns when deps change.
COPY go.mod go.sum ./
# Retried since a stuck connection to the module proxy otherwise destroys the whole download;
# each retry reuses whatever's already cached and only fetches what's missing.
RUN for i in 1 2 3 4 5; do go mod download && exit 0; echo "go mod download failed, retrying ($i)..."; sleep 5; done; exit 1

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/reaper ./reaper

FROM alpine:3.22 AS base

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 app && \
    adduser -S -u 10001 -G app -H -D app

USER app
WORKDIR /app

FROM base AS server

COPY --from=builder --chown=app:app /out/server /app/server

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD wget -q --spider http://localhost:8080/ping || exit 1

ENTRYPOINT ["/app/server"]

FROM base AS reaper

COPY --from=builder --chown=app:app /out/reaper /app/reaper

ENTRYPOINT ["/app/reaper"]
