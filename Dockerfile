# ------------------------
# BUILD
# ------------------------
FROM adregistry.fnal.gov/dev-containers/rust-kafka:1.94.0-debian AS builder

COPY --chown=dev . /app/
WORKDIR /app
RUN cargo build --release

# ------------------------
# RUN
# ------------------------
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/target/release/extapi-acsys /usr/local/bin/extapi-acsys
COPY --from=builder /lib/x86_64-linux-gnu/libz.so.1 /lib/x86_64-linux-gnu/libz.so.1
COPY --from=builder /lib/x86_64-linux-gnu/libsasl2.so.2 /lib/x86_64-linux-gnu/libsasl2.so.2
EXPOSE 8000
CMD ["extapi-acsys"]
