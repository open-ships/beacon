# syntax=docker/dockerfile:1

# --- Build stage ---
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Download dependencies first (layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .

ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-X main.version=${VERSION}" \
    -o beacon ./cmd/beacon

# --- Runtime stage ---
FROM alpine:3.19

# ca-certificates: TLS support
RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /build/beacon .

# Persistent SQLite database (config + connector buffers)
VOLUME ["/data"]

# Data server (sink endpoints) | Admin (health + metrics)
EXPOSE 8080 2112

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:2112/health || exit 1

ENTRYPOINT ["/app/beacon"]
CMD ["--db", "/data/beacon.db"]
