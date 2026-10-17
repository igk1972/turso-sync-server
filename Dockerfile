# syntax=docker/dockerfile:1
FROM rust:1.99-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
    && cp target/release/turso-sync-server /usr/local/bin/

FROM debian:trixie-slim
RUN useradd --uid 10001 --home-dir /data --no-create-home --shell /usr/sbin/nologin turso \
    && mkdir /data \
    && chown turso /data
COPY --from=build /usr/local/bin/turso-sync-server /usr/local/bin/
USER turso
VOLUME /data
EXPOSE 8080
ENV TURSO_SYNC_LISTEN=0.0.0.0:8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s CMD ["turso-sync-server", "healthcheck"]
ENTRYPOINT ["turso-sync-server"]
CMD ["/data/main.db"]
