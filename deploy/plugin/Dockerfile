FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/satchel ./cmd/satchel

FROM alpine:3.20
RUN apk add --no-cache ca-certificates e2fsprogs mount \
 && mkdir -p /run/docker/plugins /var/lib/satchel/mounts
COPY --from=build /out/satchel /usr/local/bin/satchel
ENTRYPOINT ["/usr/local/bin/satchel", "plugin"]
