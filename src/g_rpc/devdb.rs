use crate::{config::GrpcConfig, g_rpc::errors::handle_rpc_error};

use super::proto::services::devdb::{
    DeviceInfoReply, DeviceList, PlotConfigResult, PlotConfigResults,
    PlotConfigSpecification, PlotSelector, dev_db_client::DevDbClient,
    plot_config_result,
};
use rust_grpc_lib::auth::ForwardedToken;
use tonic::Response;

pub async fn get_device_info(
    devdb_config: &GrpcConfig, token: ForwardedToken, device: &[String],
) -> Result<DeviceInfoReply, tonic::Status> {
    let mut client = DevDbClient::from_endpoint_with_provider(
        &devdb_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "DevDB"))?;

    let req = DeviceList {
        device: device.to_vec(),
    };

    client.get_device_info(req).await.map(Response::into_inner)
}

pub async fn save_plot_config(
    devdb_config: &GrpcConfig, token: ForwardedToken, id: Option<usize>,
    name: String, config: String,
) -> Result<usize, tonic::Status> {
    let mut client = DevDbClient::from_endpoint_with_provider(
        &devdb_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "DevDB"))?;

    // Build the request message. If the id is None, use an
    // illegal value. The gRPC uses this value to decide to
    // insert a new record or update a current record.

    let req = PlotConfigSpecification {
        id: id.unwrap_or(0x80000000) as u32,
        name,
        config,
    };

    let PlotConfigResult { result } =
        client.save_plot_configuration(req).await?.into_inner();

    // Do some heavy pattern-matching to get down to the
    // single ID that we want to return.

    if let Some(plot_config_result::Result::Config(PlotConfigResults { data })) =
        result
        && let &[PlotConfigSpecification { id, .. }] = data.as_slice()
    {
        Ok(id as usize)
    } else {
        Err(tonic::Status::unavailable("unexpected response"))
    }
}

pub async fn get_plot_config(
    devdb_config: &GrpcConfig, token: ForwardedToken, id: Option<u32>,
) -> Result<PlotConfigResult, tonic::Status> {
    let mut client = DevDbClient::from_endpoint_with_provider(
        &devdb_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "DevDB"))?;

    let req = PlotSelector { id };

    client
        .get_plot_configuration(req)
        .await
        .map(Response::into_inner)
}

pub async fn delete_plot_config(
    devdb_config: &GrpcConfig, token: ForwardedToken, id: i32,
) -> Result<PlotConfigResult, tonic::Status> {
    let mut client = DevDbClient::from_endpoint_with_provider(
        &devdb_config.host_addr,
        token,
    )
    .map_err(|err| handle_rpc_error(err, "DevDB"))?;

    let req = PlotSelector {
        id: Some(id.cast_unsigned()),
    };

    client
        .delete_plot_configuration(req)
        .await
        .map(Response::into_inner)
}
