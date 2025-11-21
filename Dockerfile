# ------------------------
# BUILD
# ------------------------
FROM adregistry.fnal.gov/dev-containers/rust:1.90.0 AS builder

WORKDIR /app
COPY . .
RUN cargo build --release

# ------------------------
# RUN
# ------------------------
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/target/release/extapi-dpm /usr/local/bin/extapi-dpm
EXPOSE 8000
CMD ["extapi-dpm"]