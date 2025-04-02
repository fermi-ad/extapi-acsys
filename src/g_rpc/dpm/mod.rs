use proto::{
    dpm_client::DpmClient, AcquisitionList, Setting, SettingList, StatusList,
};
use tonic::transport::{Channel, Error};
use tracing::{info, warn};

pub mod proto {
    tonic::include_proto!("dpm");
}

pub struct Connection(DpmClient<Channel>);

type TonicStreamResult<T> =
    Result<tonic::Response<tonic::Streaming<T>>, tonic::Status>;
type TonicQueryResult<T> = Result<T, tonic::Status>;

// Builds a sharable connection to the DPM pool. All instances will use the
// same connection.

pub async fn build_connection() -> Result<Connection, Error> {
    Ok(Connection(
        DpmClient::connect("http://dce46.fnal.gov:50051/").await?,
    ))
}

pub async fn acquire_devices(
    conn: &Connection, jwt: Option<&String>, devices: Vec<String>,
) -> TonicStreamResult<proto::Reading> {
    let mut req = tonic::Request::new(AcquisitionList {
        session_id: "".to_owned(),
        req: devices,
    });

    if let Some(jwt) = jwt {
        use std::str::FromStr;
        use tonic::metadata::MetadataValue;

        match MetadataValue::from_str(&format!("Bearer {}", jwt)) {
            Ok(val) => {
                req.metadata_mut().insert("authorization", val);
            }
            Err(e) => warn!("error creating JWT : {}", e),
        }
    } else {
        warn!("no JWT for this request");
    }

    conn.0.clone().start_acquisition(req).await
}

// This function wraps the logic needed to make the `ApplySettings()`
// gRPC transaction.

pub async fn set_device(
    conn: &Connection, session_id: Option<String>, device: String,
    value: proto::Data,
) -> TonicQueryResult<i32> {
    use tonic::{metadata::MetadataValue, IntoRequest};

    info!("setting to {:?}", &value);

    // Build the setting request. This function only sets one device, so the
    // request only has a 1-element array containing the setting.

    let mut req = SettingList {
        session_id: "*** DO NOT USE ***".to_string(),
        setting: vec![Setting {
            name: device,
            data: Some(value),
        }],
        event: "".to_owned(),
    }
    .into_request();

    // If a JWT token has been found, add it to the request.

    if let Some(token) = session_id {
        if let Ok(val) = MetadataValue::try_from(format!("Bearer {token}")) {
            req.metadata_mut().insert("authorization", val);
        }
    }

    let StatusList { status } =
        conn.0.clone().apply_settings(req).await?.into_inner();

    if status.len() == 1 {
        Ok(status[0])
    } else {
        Err(tonic::Status::internal("received more than one status"))
    }
}
