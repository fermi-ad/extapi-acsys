# ------------------------
# BUILD
# ------------------------
FROM adregistry.fnal.gov/dev-containers/rust:1.93.0 AS builder

COPY --chown=dev . /app/
WORKDIR /app
RUN cargo build --release

# ------------------------
# RUN
# ------------------------
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/target/release/extapi-acsys /usr/local/bin/extapi-acsys
EXPOSE 8000
CMD ["extapi-acsys"]