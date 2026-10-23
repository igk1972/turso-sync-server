ARG SERVER_IMAGE=localhost/turso-sync-server:soak
FROM ${SERVER_IMAGE} AS server
FROM docker.io/library/golang:1.26-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o /usr/local/bin/soak-go .

# Trixie, like the server image, whose binary the restore check runs.
FROM debian:trixie-slim
# The restore check reads the backup from S3 over HTTPS.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home soak
COPY --from=build /usr/local/bin/soak-go /usr/local/bin/
COPY --from=server /usr/local/bin/turso-sync-server /usr/local/bin/
USER soak
CMD ["soak-go"]
