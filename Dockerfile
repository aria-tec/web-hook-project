# syntax=docker/dockerfile:1

# Stage 1: Build static binary
FROM golang:alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build fully static stripped binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -a -installsuffix cgo \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/webhook-engine \
    ./cmd/server/main.go

# Stage 2: Minimal Alpine runtime image (< 20MB)
FROM alpine:3.21

# Install TLS certificates and timezone data
RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /bin/webhook-engine /webhook-engine

# Run as unprivileged user
USER 65534:65534

EXPOSE 8080

ENTRYPOINT ["/webhook-engine"]
