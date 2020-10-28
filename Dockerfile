# Create base builder image
FROM golang:1.14-alpine
WORKDIR /go/src/github.com/ava-labs/ortelius
RUN apk add git

# Build app
COPY . .
RUN if [ -d "./vendor" ];then export MOD=vendor; else export MOD=mod; fi && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=$MOD -o /opt/orteliusd ./cmds/orteliusd/*.go

# Create final image
FROM scratch
VOLUME /var/log/ortelius
WORKDIR /opt

# Copy in and wire up build artifacts
COPY --from=0 /opt/orteliusd /opt/orteliusd
COPY --from=0 /go/src/github.com/ava-labs/ortelius/docker/config.json /opt/config.json
COPY --from=0 /go/src/github.com/ava-labs/ortelius/services/db/migrations /opt/migrations
ENTRYPOINT ["/opt/orteliusd"]
