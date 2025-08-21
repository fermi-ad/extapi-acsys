use super::proto::{
    common::device,
    services::daq::{
        daq_client::DaqClient, ReadingList, ReadingReply, Setting, SettingList,
        SettingReply,
    },
};
use tokio::time::{timeout, Duration};
use tonic::transport::{Channel, Error};
use tracing::{error, info, instrument, warn};

pub struct Connection(DaqClient<Channel>);

type TonicStreamResult<T> =
    Result<tonic::Response<tonic::Streaming<T>>, tonic::Status>;
type TonicQueryResult<T> = Result<T, tonic::Status>;

// Builds a sharable connection to the DPM pool. All instances will use the
// same connection.

pub async fn build_connection() -> Result<Connection, Error> {
    const DPM: &'static str = "http://dce07.fnal.gov:50051/";

    Ok(Connection(DaqClient::connect(DPM).await?))
}

#[instrument(skip(conn, jwt))]
pub async fn acquire_devices(
    conn: &Connection, jwt: Option<&String>, devices: Vec<String>,
) -> TonicStreamResult<ReadingReply> {
    let mut req = tonic::Request::new(ReadingList { drf: devices });

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

    match timeout(Duration::from_secs(2), conn.0.clone().read(req)).await {
        Ok(response) => {
            if let Err(ref e) = response {
                error!("error creating stream : {}", &e)
            }
            response
        }
        Err(_) => {
            error!("connection to DPM timed-out");
            Err(tonic::Status::cancelled("connection to DPM timed-out"))
        }
    }
}

// This function wraps the logic needed to make the `ApplySettings()`
// gRPC transaction.

pub async fn set_device(
    conn: &Connection, session_id: Option<String>, device: String,
    value: device::Value,
) -> TonicQueryResult<Vec<i32>> {
    use tonic::{metadata::MetadataValue, IntoRequest};

    info!("setting to {:?}", &value);

    // Build the setting request. This function only sets one device, so the
    // request only has a 1-element array containing the setting.

    let mut req = SettingList {
        setting: vec![Setting {
            device,
            value: Some(value),
        }],
    }
    .into_request();

    // If a JWT token has been found, add it to the request.

    if let Some(token) = session_id {
        if let Ok(val) = MetadataValue::try_from(format!("Bearer {token}")) {
            req.metadata_mut().insert("authorization", val);
        }

        let SettingReply { status } =
            conn.0.clone().set(req).await?.into_inner();

        Ok(status
            .iter()
            .map(|v| v.facility_code + v.status_code * 256)
            .collect())
    } else {
        Err(tonic::Status::internal("not authorized"))
    }
}
