FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata curl jq bash

ARG TARGETPLATFORM
COPY bin/${TARGETPLATFORM}/vykar /usr/local/bin/vykar
COPY bin/${TARGETPLATFORM}/vykar-server /usr/local/bin/vykar-server

RUN mkdir -p /etc/vykar /data /repo /cache

ENV XDG_CACHE_HOME=/cache

WORKDIR /data

# Default port for the daemon's read-only HTTP status page. Exposing the
# port does not bind it — opt in by setting VYKAR_HTTP_LISTEN (and, for
# non-loopback addresses, VYKAR_HTTP_ALLOW_PUBLIC=1).
EXPOSE 7575

ENTRYPOINT ["vykar"]
CMD ["daemon"]
