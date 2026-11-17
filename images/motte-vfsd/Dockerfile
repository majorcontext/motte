# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.4-bookworm@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea AS build
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH GOFLAGS=-trimpath go build \
    -ldflags "-s -w" \
    -o /out/motte-vfsd ./cmd/motte-vfsd

# git serves "motte-vfsd import". The daemon mounts with mount(2), so the image needs no fusermount.
FROM debian:trixie-slim@sha256:89c1ea91e897f0fe0a3731595330bc45b8bdef4047464a07116f891a94054b31
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/motte-vfsd /usr/local/bin/motte-vfsd

ENTRYPOINT ["/usr/local/bin/motte-vfsd"]
