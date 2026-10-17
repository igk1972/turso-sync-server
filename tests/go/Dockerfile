ARG SERVER_IMAGE=localhost/turso-sync-server:dev
FROM ${SERVER_IMAGE} AS server

FROM golang:1.26-trixie
WORKDIR /tests
COPY go.mod go.sum ./
# The driver's own tests need its test dependencies as well.
RUN go mod download \
    && cd "$(go list -m -f '{{.Dir}}' turso.tech/database/tursogo)" \
    && go mod download
COPY *.go upstream.sh ./
COPY --from=server /usr/local/bin/turso-sync-server /usr/local/bin/
ENV LOCAL_SYNC_SERVER=/usr/local/bin/turso-sync-server
CMD ["sh", "-c", "go test -count=1 ./... && ./upstream.sh"]
