# Builder: has the full Go toolchain, which the final image deliberately
# does not. Version matches go.mod's `go 1.26.5`.
FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# CGO_ENABLED=0 produces a static binary - this project has no cgo
# dependencies (prometheus/client_golang is pure Go), and a static binary is
# required for distroless/static below, which has no libc to link against.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/broker ./cmd/broker

# Final: distroless/static - no shell, no package manager, nothing but the
# binary. Smallest attack surface and image size; there's no docker exec-ing
# in to poke around, so debugging a running container relies on its logs
# and the /metrics endpoint, same as it would for any real deployment.
FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=builder /out/broker /app/broker

# cmd/broker/main.go's dataDir constant is the relative path "data" - this
# WORKDIR is what makes that resolve to /app/data inside the container,
# which docker-compose.yml mounts a named volume at.
EXPOSE 9092 9101
ENTRYPOINT ["/app/broker"]
