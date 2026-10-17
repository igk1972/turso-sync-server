ARG SERVER_IMAGE=localhost/turso-sync-server:dev
FROM ${SERVER_IMAGE} AS server

FROM node:24-trixie-slim
# The S3 client refuses to start without system root certificates.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /tests
COPY package.json package-lock.json ./
RUN npm ci
COPY *.ts ./
COPY --from=server /usr/local/bin/turso-sync-server /usr/local/bin/
ENV LOCAL_SYNC_SERVER=/usr/local/bin/turso-sync-server
CMD ["npx", "vitest", "--run"]
