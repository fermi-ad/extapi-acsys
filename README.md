# `extapi-acsys`
![Latest](../../actions/workflows/cd.yml/badge.svg?branch=main)

Provides public APIs to the Fermilab control system. This service exposes several GraphQL endpoints for various, logical APIs that clients may use to retrieve control system data and, in some cases, make changes to the control system. This service is currently running on *acsys-proxy.fnal.gov* on port 8000 with the development instance on port 8001.

The middle layer of the control system uses gRPCs for communications. The GraphQL resolvers of this service use various gRPC services to obtain the information that is returned. This uses the `async-graphql` and `warp` crates to provide GraphQL over http support. The resolvers use the `tonic` crate for gRPC client support.

## Developers


### Prerequisites

- [Rust >= 1.90](https://www.rust-lang.org/learn/get-started)
- [Protocol Buffer](https://grpc.io/docs/protoc-installation/)

### Environment variables
The following variables exist for configuring the service at runtime:
- `ALARMS_KAFKA_TOPIC` -> Topic name for alarms in Kafka
- `CLOCK_GRPC_HOST` -> Hostname for the clock gRPC service
- `DEVDB_GRPC_HOST` -> Hostname for the DevDB gRPC service
- `DPM_GRPC_HOST` -> Hostname for the DPM gRPC service
- `GRAPHQL_PORT` -> Port for the GraphQL service
- `KAFKA_HOST` -> Hostname for the Controls Kafka instance
- `SCANNER_GRPC_HOST` -> Hostname for the wire scanner gRPC service
- `TLG_GRPC_HOST` -> Hostname for the TLG gRPC service


### Check out the project:

```shell
$ git clone  --recurse-submodules https://github.com/fermi-ad/extapi-acsys.git
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
