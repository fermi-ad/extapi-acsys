use proto::dev_db_client::DevDbClient;
use tracing::info;

pub mod proto {
    tonic::include_proto!("devdb");
}

pub async fn get_device_info(
    device: &[String],
) -> Result<tonic::Response<proto::DeviceInfoReply>, tonic::Status> {
    info!("looking up \"{}\"", &device[0]);
    match DevDbClient::connect("http://10.200.24.120:6802/").await {
        Ok(mut client) => {
            let req = proto::DeviceList {
                device: device.to_vec(),
            };

            client.get_device_info(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("DevDB service unavailable")),
    }
}
