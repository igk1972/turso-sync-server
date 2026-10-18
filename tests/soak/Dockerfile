ARG SERVER_IMAGE=localhost/turso-sync-server:soak
FROM ${SERVER_IMAGE} AS server

FROM node:24-trixie-slim
# The restore check reads the backup from S3 over HTTPS.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /soak
COPY package.json package-lock.json ./
RUN npm ci
COPY soak.ts ./
COPY --from=server /usr/local/bin/turso-sync-server /usr/local/bin/
USER node
CMD ["node", "--disable-warning=ExperimentalWarning", "soak.ts"]
