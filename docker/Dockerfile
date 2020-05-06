# Create base builder image
FROM golang:1.14-alpine
WORKDIR /go/src/github.com/ava-labs/ortelius
RUN apk add git

# Build app
COPY . .
RUN go get ./cmds/...
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /opt/orteliusd ./cmds/orteliusd/*.go

# Create base final image
FROM scratch
VOLUME /var/log/ortelius
VOLUME /var/lib/ortelius

# Copy in and wire up build artifacts
COPY --from=0 /opt/orteliusd /opt/orteliusd
COPY --from=0 /go/src/github.com/ava-labs/ortelius/docker/config.json /var/lib/ortelius/config.json
ENTRYPOINT ["/opt/orteliusd"]
