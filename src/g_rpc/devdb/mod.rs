use super::proto::services::devdb::{
    dev_db_client::DevDbClient, plot_config_result, DeviceInfoReply,
    DeviceList, PlotConfigResult, PlotConfigResults, PlotConfigSpecification,
    PlotSelector,
};
use crate::env_var;

const DEVDB_HOST: &str = "DEVDB_GRPC_HOST";

pub async fn get_device_info(
    device: &[String],
) -> Result<tonic::Response<DeviceInfoReply>, tonic::Status> {
    let host: String = env_var::expect(DEVDB_HOST);

    match DevDbClient::connect(host).await {
        Ok(mut client) => {
            let req = DeviceList {
                device: device.to_vec(),
            };

            client.get_device_info(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("DevDB service unavailable")),
    }
}

pub async fn save_plot_config(
    id: Option<usize>, name: String, config: String,
) -> Result<usize, tonic::Status> {
    let host: String = env_var::expect(DEVDB_HOST);

    match DevDbClient::connect(host.clone()).await {
        Ok(mut client) => {
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

            if let Some(plot_config_result::Result::Config(
                PlotConfigResults { data },
            )) = result
            {
                // Should only have returned an array of one result.

                if let &[PlotConfigSpecification { id, .. }] = data.as_slice() {
                    return Ok(id as usize);
                }
            }
            Err(tonic::Status::unavailable("unexpected response"))
        }
        Err(_) => Err(tonic::Status::unavailable(format!(
            "DevDB service ({}) unavailable",
            &host
        ))),
    }
}

pub async fn get_plot_config(
    id: Option<u32>,
) -> Result<PlotConfigResult, tonic::Status> {
    let host: String = env_var::expect(DEVDB_HOST);

    match DevDbClient::connect(host.clone()).await {
        Ok(mut client) => {
            let req = PlotSelector { id };

            Ok(client
                .get_plot_configuration(req)
                .await
                .map(|v| v.into_inner())?)
        }
        Err(e) => Err(tonic::Status::unavailable(format!(
            "DevDB service ({}) unavailable: {}",
            &host, e
        ))),
    }
}
