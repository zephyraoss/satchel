FROM golang:1.26-alpine AS build
RUN apk add --no-cache gcc musl-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -tags sqlite_omit_load_extension \
    -ldflags='-s -w -extldflags "-static"' -o /out/satchel ./cmd/satchel

FROM alpine:3.20 AS litestream
ARG LITESTREAM_VERSION=0.5.16
ARG TARGETARCH
RUN apk add --no-cache curl tar \
 && case "$TARGETARCH" in amd64) arch=x86_64;; arm64) arch=arm64;; *) arch="$TARGETARCH";; esac \
 && curl -fsSL "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/litestream-${LITESTREAM_VERSION}-linux-${arch}.tar.gz" \
  | tar -xz -C /usr/local/bin litestream

FROM alpine:3.20
RUN apk add --no-cache ca-certificates fuse3 && mkdir -p /run/docker/plugins /var/lib/satchel/mounts
COPY --from=build /out/satchel /usr/local/bin/satchel
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
ENTRYPOINT ["/usr/local/bin/satchel", "plugin"]
