# Multi-stage build: compile cassango and cassango-cli, then ship them on a slim Debian image.
FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY client ./client
COPY src ./src
COPY tests ./tests
RUN cargo build --release --locked --bin cassango --bin cassango-cli

FROM debian:bookworm-slim
COPY --from=build /src/target/release/cassango /src/target/release/cassango-cli /usr/local/bin/
VOLUME /data
EXPOSE 7000
# Override flags (node id, seeds, ...) in docker-compose.yml or `docker run`.
ENTRYPOINT ["cassango", "--listen", "0.0.0.0:7000", "--data-dir", "/data"]
