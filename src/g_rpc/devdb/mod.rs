//! Device Database gRPC Module

pub mod proto {
    tonic::include_proto!("devdb");
}

use proto::dev_db_client::DevDbClient;
use rust_env_var_lib::env_var;

const DEVDB_HOST: &str = "DEVDB_GRPC_HOST";

pub async fn get_device_info(
    device: &[String],
) -> Result<tonic::Response<proto::DeviceInfoReply>, tonic::Status> {
    let host: String = env_var::expect(DEVDB_HOST);
    match DevDbClient::connect(host).await {
        Ok(mut client) => {
            let req = proto::DeviceList {
                device: device.to_vec(),
            };

            client.get_device_info(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("DevDB service unavailable")),
    }
}
