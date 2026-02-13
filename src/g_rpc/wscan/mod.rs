// This module implements the client side of the wire scan gRPC
// protocol.

use proto::{
    DetectorRequest, ScanProgress, ScanRequest, ScanResult,
    scanner_client::ScannerClient,
};
use std::collections::HashMap;
use tonic::{Response, Status, Streaming, transport};

pub mod proto {
    tonic::include_proto!("scanner");
}

use crate::env_var;

const WIRE_SCANNER_HOST: &str = "SCANNER_GRPC_HOST";

// Local helper function to get a connection to the gRPC service.

async fn get_client() -> Result<ScannerClient<transport::Channel>, Status> {
    let host: String = env_var::expect(WIRE_SCANNER_HOST);
    ScannerClient::connect(host)
        .await
        .map_err(|_| Status::unavailable("wire-scanner service unavailable"))
}

pub async fn _retrieve_scans() -> Result<HashMap<String, String>, Status> {
    let map = HashMap::from([
        (
            "scl-ws-station1".into(),
            "Super Conduction Linac Wire Scanner - Station 1".into(),
        ),
        (
            "scl-ws-station2".into(),
            "Super Conducting Linac Wire Scanner - Station 2".into(),
        ),
    ]);

    Ok(map)
}

pub async fn start_scan(
    id: String, pos_start: f32, pos_end: f32, pos_step: f32, samp_dur: f32,
    pps: i32,
) -> Result<Response<Streaming<ScanResult>>, Status> {
    get_client()
        .await?
        .start_scan(ScanRequest {
            detector_id: id,
            position_start: pos_start,
            position_end: pos_end,
            position_step: pos_step,
            sampling_duration: samp_dur,
            pulses_per_sample: pps,
        })
        .await
}

pub async fn get_progress(
    id: String,
) -> Result<Response<ScanProgress>, Status> {
    get_client()
        .await?
        .get_progress(DetectorRequest { detector_id: id })
        .await
}

pub async fn abort_scan(id: String) -> Result<Response<ScanProgress>, Status> {
    get_client()
        .await?
        .abort_scan(DetectorRequest { detector_id: id })
        .await
}
