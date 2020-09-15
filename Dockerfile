###################
# BUILD CONTAINER #
###################
FROM rust:1.40 as builder

WORKDIR /usr/src/rpcn
COPY . .

RUN apt-get update && apt-get install -y pkg-config libssl-dev && cargo build --release --verbose

########################
# PRODUCTION CONTAINER #
########################

FROM debian:buster-slim

WORKDIR /app/rpcn

RUN apt-get update && apt-get install -y openssl pkg-config libssl-dev && rm -rf /var/lib/apt/lists/* && mkdir log
RUN openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -subj "/C=US/ST=North Carolina/L=Cary/O=pirate-boxxx/CN=pirate-boxxx" -keyout key.pem -out cert.pem

COPY --from=builder /usr/src/rpcn/target/release/rpcn /app/rpcn

ENTRYPOINT ./rpcn
