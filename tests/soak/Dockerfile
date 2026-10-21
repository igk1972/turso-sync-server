ARG SERVER_IMAGE=localhost/turso-sync-server:soak
FROM ${SERVER_IMAGE} AS server
FROM docker.io/oven/bun:1.4.3-slim AS bun

# Trixie, like the server image, whose binary the restore check runs.
FROM debian:trixie-slim
# The restore check reads the backup from S3 over HTTPS.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home soak
COPY --from=bun /usr/local/bin/bun /usr/local/bin/
WORKDIR /soak
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY soak.ts ./
COPY --from=server /usr/local/bin/turso-sync-server /usr/local/bin/
USER soak
CMD ["bun", "soak.ts"]
