# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.4-bookworm@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea AS build
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH GOFLAGS=-trimpath go build \
    -ldflags "-s -w" \
    -o /out/motte ./cmd/motte

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/motte /usr/local/bin/motte
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/motte"]
