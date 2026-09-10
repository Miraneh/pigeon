# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS builder

WORKDIR /src

# go.mod/go.sum copied first so this layer only reruns when deps change, not on every source edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 app && \
    adduser -S -u 10001 -G app -H -D app

COPY --from=builder --chown=app:app /out/api /app/api

USER app
WORKDIR /app

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD wget -q --spider http://localhost:8080/ping || exit 1

ENTRYPOINT ["/app/api"]
