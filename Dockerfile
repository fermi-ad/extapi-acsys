FROM gcr.io/distroless/cc-debian12

COPY target/release/extapi-dpm /usr/local/bin/extapi-dpm
EXPOSE 8000
CMD ["extapi-dpm"]