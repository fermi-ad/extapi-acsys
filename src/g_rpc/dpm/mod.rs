use proto::{
    dpm_client::DpmClient, AcquisitionList, Setting, SettingList, StatusList,
};
use tonic::transport::{Channel, Error};
use tracing::info;

pub mod proto {
    tonic::include_proto!("dpm");
}

pub struct Connection(DpmClient<Channel>);

// Builds a sharable connection to the DPM pool. All instances will use the
// same connection.

pub async fn build_connection() -> Result<Connection, Error> {
    Ok(Connection(
        DpmClient::connect("http://dce09.fnal.gov:50051/").await?,
    ))
}

pub async fn acquire_devices(
    conn: &Connection, session_id: &str, devices: Vec<String>,
) -> Result<tonic::Response<tonic::Streaming<proto::Reading>>, tonic::Status> {
    let req = AcquisitionList {
        session_id: session_id.to_owned(),
        req: devices,
    };

    conn.0.clone().start_acquisition(req).await
}

// This function wraps the logic needed to make the `ApplySettings()`
// gRPC transaction.

pub async fn set_device(
    conn: &Connection, session_id: &str, device: String, value: proto::Data,
) -> Result<i32, tonic::Status> {
    info!("setting device {} to {:?}", &device, &value);

    // Build the setting request. This function only sets one device, so the
    // request only has a 1-element array containing the setting.

    let req = SettingList {
        session_id: session_id.to_owned(),
        setting: vec![Setting {
            name: device,
            data: Some(value),
        }],
        event: "".to_owned(),
    };

    let StatusList { status } =
        conn.0.clone().apply_settings(req).await?.into_inner();

    if status.len() == 1 {
        Ok(status[0])
    } else {
        Err(tonic::Status::internal("received more than one status"))
    }
}
