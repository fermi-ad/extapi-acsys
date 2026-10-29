# `extapi-acsys`

![Latest](../../actions/workflows/cd.yml/badge.svg?branch=main)

Provides public APIs to the Fermilab control system. This service exposes several GraphQL endpoints for various, logical APIs that clients may use to retrieve control system data and, in some cases, make changes to the control system. This service has a [production instance](https://ad-api.fnal.gov/acsys) and a [development instance](https://ad-api-dev.fnal.gov/alarms).

The middle layer of the control system uses gRPCs for communications. The GraphQL resolvers of this service use various gRPC services to obtain the information that is returned. This uses the `async-graphql` and `warp` crates to provide GraphQL over http support. The resolvers use the `tonic` crate for gRPC client support.

## Developers

### Prerequisites

- [Rust >= 1.90](https://www.rust-lang.org/learn/get-started)

#### Kafka (rdkafka) native dependencies

This service uses [`rdkafka`](https://crates.io/crates/rdkafka) (librdkafka bindings) for Kafka pub/sub.

If you are building locally (outside the devcontainer/Docker image), you may need native dependencies installed.

Debian/Ubuntu packages typically required:

- `libsasl2-dev`
- `libcurl4-openssl-dev`

Depending on your environment, you may also need a C toolchain and build tooling (e.g. `cmake`, `pkg-config`). The associated [`dev container`](.devcontainer/devcontainer.json) comes prepackaged with all the development tools necessary to build this project.

### Environment variables

The following variables exist for configuring the service at runtime:

- `ALARMS_KAFKA_HOST` -> Hostname for the Kafka instance that supports the alarms service
- `ALARMS_KAFKA_TOPIC` -> Topic name for alarms in Kafka
- `BLM_GRPC_HOST` -> Hostname for the BLM gRPC service
- `CLOCK_GRPC_HOST` -> Hostname for the clock gRPC service
- `DEVDB_GRPC_HOST` -> Hostname for the DevDB gRPC service
- `DPM_GRPC_HOST` -> Hostname for the DPM gRPC service
- `GRAPHQL_PORT` -> Port for clients to connect via GraphQL to this service
- `GRPC_ALARMS_DB_HOST` -> Hostname for the Alarms DB Access gRPC service
- `GRPC_ALARMS_SERVICE_HOST` -> Hostname for the Alarms gRPC service
- `MI8_AUTOTUNE_GRPC_HOST` -> Hostname for the MI8 Autotune gRPC service
- `REQUIRED_UPSTREAMS` -> Optional comma-separated list of upstreams that must be configured for the service to start (see below)
- `RUST_LOG` -> The default logging environment variable from Rust. Can be configured to log specific crates/modules at different levels from the global default.
- `SCANNER_GRPC_HOST` -> Hostname for the wire scanner gRPC service
- `TLG_GRPC_HOST` -> Hostname for the TLG gRPC service
- `UNR_GRPC_HOST` -> Hostname for the UNR gRPC service

#### Optional upstreams and feature degradation

By default, no upstream variable is required to start the service. If a variable is unset or blank, the service logs a `WARN` naming the variable and the disabled feature, then boots normally. `ALARMS_KAFKA_HOST` and `ALARMS_KAFKA_TOPIC` must both be set to enable alarm subscriptions.

| Variable(s)                                       | Route                                                                                                   | When missing                                                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `BLM_GRPC_HOST`                                   | `/blm`                                                                                                  | Route is not mounted (404)                                                                           |
| `DEVDB_GRPC_HOST`                                 | `/devdb`                                                                                                | Route is not mounted (404)                                                                           |
| `DEVDB_GRPC_HOST`                                 | `/acsys` plot configuration (`plotConfiguration`, `updatePlotConfiguration`, `deletePlotConfiguration`) | Field returns `FEATURE_DISABLED`                                                                     |
| `MI8_AUTOTUNE_GRPC_HOST`                          | `/mi8-autotune`, `/mi8-autotune/s`                                                                      | Both routes are not mounted (404)                                                                    |
| `TLG_GRPC_HOST`                                   | `/tlg`                                                                                                  | Route is not mounted (404)                                                                           |
| `UNR_GRPC_HOST`                                   | `/unr`                                                                                                  | Route is not mounted (404)                                                                           |
| `SCANNER_GRPC_HOST`                               | `/wscan`                                                                                                | Route is not mounted (404)                                                                           |
| `GRPC_ALARMS_DB_HOST`, `GRPC_ALARMS_SERVICE_HOST` | `/alarms`                                                                                               | Route is not mounted only when both are missing; otherwise affected fields return `FEATURE_DISABLED` |
| `ALARMS_KAFKA_HOST`, `ALARMS_KAFKA_TOPIC`         | `/alarms/s` subscriptions                                                                               | Subscription returns `FEATURE_DISABLED`                                                              |
| `CLOCK_GRPC_HOST`                                 | `/acsys` triggered plots                                                                                | Field returns `FEATURE_DISABLED`                                                                     |

`/acsys`, `/bbm` and `/faas` are always mounted. The landing page (`/`) only links to mounted routes. Requests that reach a disabled feature inside a mounted route get a GraphQL error with `extensions.code = "FEATURE_DISABLED"`.

#### Requiring upstreams

To keep fail-fast behavior for upstreams a deployment cannot run without, list them in `REQUIRED_UPSTREAMS`:

```shell
REQUIRED_UPSTREAMS=devdb,unr,clock
```

Keys: `alarms_db`, `alarms_kafka` (only with the `kafka` feature), `alarms_svc`, `blm`, `clock`, `devdb`, `mi8_autotune`, `tlg`, `unr`, `wscan`. If any listed upstream is missing, the service logs every missing variable and exits with a non-zero status. An unknown key is also a startup error, so a typo cannot silently drop a requirement. Unset or blank means nothing is required.

### MI8 Autotune query and subscriptions

When [`MI8_AUTOTUNE_GRPC_HOST`](src/config.rs:139) is configured, the [`/mi8-autotune`](src/graphql.rs:238) endpoint accepts GraphQL POST requests and serves GraphiQL on GET. Its query field [`bpms`](src/graphql/mi8_autotune.rs:128) returns the BPM names that the autotuner is configured to read, as a non-null list of non-null strings. For example, send the GraphQL query [`{ bpms }`](src/graphql/mi8_autotune.rs:128) to retrieve the list.

The caller's Bearer token is forwarded to the upstream service; upstream authentication and authorization rules still apply. RPC initiation and stream failures return a generic error with an Error ID, with details logged server-side. This API is read-only: no mutations or settings access are exposed.

GraphiQL is configured to use the [`/mi8-autotune/s`](src/graphql.rs) WebSocket endpoint. Both HTTP and WebSocket routes are absent (404) when the upstream is disabled. Use a GraphQL WebSocket client (for example, the graphql-transport-ws protocol) and set the connection-init payload to [`{"Authorization":"Bearer YOUR_TOKEN"}`](src/graphql/auth_handlers.rs:159). The key is case-sensitive: it must be capitalized Authorization, not authorization. The HTTP upgrade Authorization header is not used for subscription authentication. Omitting the connection-init token forwards an empty token and leaves authorization to the upstream; a supplied value must use the Bearer scheme.

Run one subscription operation at a time, or use separate operations on the same connection:

- [`subscription { armedState }`](src/graphql/mi8_autotune.rs:147) reports UNKNOWN, ARMED, or DISARMED.
- [`subscription { operatingMode }`](src/graphql/mi8_autotune.rs:165) reports UNKNOWN, LIVE, or SHADOW.
- [`subscription { events { autotuneEvent errorMessage } }`](src/graphql/mi8_autotune.rs:183) reports an event enum and its associated upstream message.

Each upstream subscription sends its current state immediately, then updates when the state or associated details change. The gateway forwards those messages in order; it does not manufacture an initial value or call unary state getters. Unrecognized numeric protobuf enum values map to UNKNOWN. Event values are UNKNOWN, NO_AUTO, FIRST_PASS, MOVE, MOVE_PENDING, DAQ_ERROR, BIG_MOVE, VERNIER_MOVE, BEAM_OK, BIG_SETTING, INTENSITY_LOW, AUXILIARY_BAD, and SETTING_ERROR (see [enum mappings](src/graphql/mi8_autotune/types.rs)). The event's errorMessage is application data supplied by the upstream, distinct from a sanitized RPC failure. Upstream completion completes the subscription; cancelling the operation drops its upstream stream.

### Error IDs in responses

Alarms and MI8 Autotune requests intentionally return a generic error message that includes an **Error ID** (a UUID). The underlying error details are logged server-side.

To debug an issue reported by a client:

1. Copy the Error ID from the client-visible message.
2. Search the server logs for that UUID to find the corresponding detailed error entry.

### Check out the project:

```shell
$ git clone https://github.com/fermi-ad/extapi-acsys.git
$ cd extapi-acsys
```

The `main` branch is used for deployment; developers cannot commit directly to the `main` branch. Create a development branch which will host your changes. Once you're ready to release them, create a pull request.

### Creating a branch

```shell
$ git checkout -b devel
```

Make changes and commit them to this branch.

### Pushing your branch

```shell
$ git push origin devel
```

Go to GitHub and make a pull request using this branch.
