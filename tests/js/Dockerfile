ARG SERVER_IMAGE=localhost/turso-sync-server:dev
FROM ${SERVER_IMAGE} AS server

FROM node:24-trixie-slim
WORKDIR /tests
COPY package.json package-lock.json ./
RUN npm ci
COPY *.ts ./
COPY --from=server /usr/local/bin/turso-sync-server /usr/local/bin/
ENV LOCAL_SYNC_SERVER=/usr/local/bin/turso-sync-server
CMD ["npx", "vitest", "--run"]
