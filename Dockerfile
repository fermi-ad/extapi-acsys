# ------------------------
# BUILD
# ------------------------
FROM adregistry.fnal.gov/dev-containers/rust-with-c:1.97.0-curl AS builder

COPY --chown=dev . /app/
WORKDIR /app
RUN cargo build --release

# ------------------------
# RUN
# ------------------------
FROM gcr.io/distroless/cc-debian13

COPY --from=builder /app/target/release/extapi-acsys /usr/local/bin/extapi-acsys

EXPOSE 443
CMD ["extapi-acsys"]
