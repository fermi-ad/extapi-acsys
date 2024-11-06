# `extapi-acsys`

Provides public APIs to the Fermilab control system. This service exposes several GraphQL endpoints for various, logical APIs that clients may use to retrieve control system data and, in some cases, make changes to the control system. This service is currently running on *acsys-proxy.fnal.gov* on port 8000 with the development instance on port 8001.

The middle layer of the control system uses gRPCs for communications. The GraphQL resolvers of this service use various gRPC services to obtain the information that is returned. This uses the `async-graphql` and `warp` crates to provide GraphQL over http support. The resolvers use the `tonic` crate for gRPC client support.

## Developers

Check out the project:

```shell
$ git clone https://github.com/fermi-ad/extapi-acsys.git
$ cd extapi-acsys
```

*NOTE: acsys-proxy.fnal.gov is a temporary host. Once we set up the permanent host, we'll also configure continuous deployment. But for now, the service is manually restarted after a build.*

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
