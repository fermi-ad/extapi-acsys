# ------------------------
# BUILD
# ------------------------
FROM adregistry.fnal.gov/dev-containers/rust:1.97.1 AS builder

ARG GIT_TOKEN

COPY --chown=dev . /app/

WORKDIR /app

RUN git config --global url."https://x-access-token:${GIT_TOKEN}@github.com/".insteadOf "https://github.com/"

RUN cargo build --release

# ------------------------
# RUN
# ------------------------
FROM adregistry.fnal.gov/dev-containers/redhat-ubi9-minimal

RUN useradd -u 10001 -r -M -s /sbin/nologin appuser

COPY --from=builder --chown=10001:10001 /app/target/release/extapi-acsys /usr/local/bin/extapi-acsys

USER 10001

EXPOSE 443

ENTRYPOINT ["/usr/local/bin/extapi-acsys"]
