use proto::dev_db_client::DevDbClient;

pub mod proto {
    tonic::include_proto!("devdb");
}

use crate::env_var;

const DEVDB_HOST: &str = "DEVDB_GRPC_HOST";
const DEFAULT_DEVDB_HOST: &str = "http://10.200.24.105:6802";

pub async fn get_device_info(
    device: &[String],
) -> Result<tonic::Response<proto::DeviceInfoReply>, tonic::Status> {
    let host = env_var::get(DEVDB_HOST).or(DEFAULT_DEVDB_HOST.to_owned());
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
