use proto::dev_db_client::DevDbClient;

pub mod proto {
    tonic::include_proto!("devdb");
}

use crate::env_var;

const DEVDB_HOST: &str = "DEVDB_GRPC_HOST";
const DEFAULT_DEVDB_HOST: &str = "10.200.24.105";

const DEVDB_PORT: &str = "DEVDB_GRPC_PORT";
const DEFAULT_DEVDB_PORT: &str = "6802";

pub async fn get_device_info(
    device: &[String],
) -> Result<tonic::Response<proto::DeviceInfoReply>, tonic::Status> {
    let host = env_var::get(DEVDB_HOST).into_str_or(DEFAULT_DEVDB_HOST);
    let port = env_var::get(DEVDB_PORT).into_str_or(DEFAULT_DEVDB_PORT);
    let address = format!("http://{}:{}", host, port);
    match DevDbClient::connect(address).await {
        Ok(mut client) => {
            let req = proto::DeviceList {
                device: device.to_vec(),
            };

            client.get_device_info(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("DevDB service unavailable")),
    }
}
