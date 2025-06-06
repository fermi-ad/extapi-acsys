# ------------------------
# BUILD
# ------------------------
FROM rust:1.87-slim as builder

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /app
COPY . .
RUN cargo build --release

# ------------------------
# RUN
# ------------------------
FROM debian:bookworm-slim

COPY --from=builder /app/target/release/extapi-dpm /usr/local/bin/extapi-dpm
EXPOSE 8000
CMD ["extapi-dpm"]