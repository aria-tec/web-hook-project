# syntax=docker/dockerfile:1

# Stage 1: Build static binary
FROM golang:1.24-alpine AS builder

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
    -o /bin/mock-receiver \
    ./cmd/mockreceiver/main.go

# Stage 2: Minimal Alpine runtime image (< 20MB)
FROM alpine:3.21

# Install TLS certificates and timezone data
RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /bin/mock-receiver /mock-receiver

# Run as unprivileged user
USER 65534:65534

EXPOSE 9090

ENTRYPOINT ["/mock-receiver"]
