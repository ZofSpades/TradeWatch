# Builds every TradeWatch Go service into a single distroless image. Compose
# and Kubernetes select the binary to run with `command`.
FROM golang:1.26.9-trixie AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-buildvcs=false
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/bin/ ./cmd/... \
    && mkdir -p /out/recordings

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bin/ /usr/local/bin/
# Mount point for session recordings, owned by the runtime user so a fresh
# named volume inherits writable ownership.
COPY --from=build --chown=65532:65532 /out/recordings /recordings
USER nonroot:nonroot
