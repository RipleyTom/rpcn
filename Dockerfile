FROM rust:latest AS builder
RUN apt-get update -y && \
  apt-get install -y protobuf-compiler
WORKDIR /usr/src/rpcn
COPY . .
RUN cargo build --release
RUN /usr/src/rpcn/target/release/rpcn --cert-gen

FROM gcr.io/distroless/cc
WORKDIR /rpcn
COPY --from=builder /usr/src/rpcn/target/release/rpcn /usr/local/bin/rpcn
COPY --from=builder /usr/src/rpcn/*.cfg ./
COPY --from=builder /usr/src/rpcn/*.pem ./

CMD ["rpcn"]
