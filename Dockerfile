# syntax=docker/dockerfile:experimental

FROM rust:slim-buster as builder
WORKDIR /src

COPY . .
RUN --mount=type=cache,target=target
RUN apt-get update && apt-get install -y pkg-config libssl-dev
RUN mkdir -p /out
RUN cargo build -p relay --features metrics-prometheus --release
RUN mv target/release/relay /out/relay

FROM debian:buster-slim
RUN apt-get update && apt-get install -y ca-certificates libc6 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/relay /usr/local/bin/relay
CMD ["relay"]
