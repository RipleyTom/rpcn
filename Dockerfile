FROM rust:latest as builder
WORKDIR /usr/src/rpcn
COPY . .
RUN cargo install --path .

FROM debian:buster-slim
WORKDIR /home/rpcn
RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*
RUN openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem -subj '/CN=localhost'
COPY --from=builder /usr/local/cargo/bin/rpcn /usr/local/bin/config
COPY --from=builder /usr/local/cargo/bin/db/ ./db/
COPY --from=builder /usr/local/cargo/bin/score_data/ ./score_data/
COPY --from=builder /usr/src/rpcn/*.pem /home/rpcn/
COPY --from=builder /usr/src/rpcn/*.cfg /home/rpcn/
CMD ["rpcn"]