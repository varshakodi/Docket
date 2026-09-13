# Stage 1: build. Has the Go toolchain, source, and module cache -- big.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Copy the dependency manifest first and download modules on their own layer.
# Source changes on every commit; dependencies rarely do. Ordering it this way
# means a code edit reuses the cached module layer instead of re-downloading.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 produces a fully static binary with no libc dependency, so it
# can run in the minimal image below.
RUN CGO_ENABLED=0 go build -o /out/docket ./cmd/docket

# Stage 2: run. Only the binary is copied across; the toolchain never ships.
FROM alpine:3.20
RUN adduser -D -u 10001 docket
USER docket
COPY --from=build /out/docket /usr/local/bin/docket
ENTRYPOINT ["docket"]
CMD ["help"]
