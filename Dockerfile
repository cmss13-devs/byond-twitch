FROM rust:1.90 AS builder

WORKDIR /usr/src/app
COPY src/ src/
COPY Cargo.toml Cargo.toml

RUN RUSTFLAGS="--cfg tokio_unstable" cargo install --path .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/byond-twitch /usr/local/bin/byond-twitch

CMD ["byond-twitch"]
