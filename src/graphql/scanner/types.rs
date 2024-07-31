use crate::g_rpc::wscan::proto;
use async_graphql::{types, SimpleObject, Union};

#[derive(SimpleObject)]
pub struct ScanConfiguration {
    pub id: types::ID,
    pub name: String,
    pub parameters: Vec<String>,
}

#[derive(SimpleObject)]
pub struct ScanStateIdle {
    pub position: f32,
}

#[derive(SimpleObject)]
pub struct ScanStateScanning {
    pub start_time: i32,
    pub position: f32,
    pub progress_percentage: i32,
}

#[derive(SimpleObject)]
pub struct ScanStateError {
    pub err_message: String,
    pub position: Option<f32>,
}

#[derive(Union)]
pub enum ScanState {
    Idle(ScanStateIdle),
    Scanning(ScanStateScanning),
    Error(ScanStateError),
}

#[derive(SimpleObject)]
pub struct ScanCurrentState {
    pub detector_id: types::ID,
    pub state: ScanState,
}

impl From<proto::ScanProgress> for ScanCurrentState {
    fn from(o: proto::ScanProgress) -> Self {
        if o.message.is_empty() {
            if o.progress_percentage == 0 || o.progress_percentage == 100 {
                ScanCurrentState {
                    detector_id: types::ID(o.detector_id),
                    state: ScanState::Idle(ScanStateIdle {
                        position: o.current_position,
                    }),
                }
            } else {
                ScanCurrentState {
                    detector_id: types::ID(o.detector_id),
                    state: ScanState::Scanning(ScanStateScanning {
                        position: o.current_position,
                        start_time: o.start_time,
                        progress_percentage: o.progress_percentage,
                    }),
                }
            }
        } else {
            ScanCurrentState {
                detector_id: types::ID(o.detector_id),
                state: ScanState::Error(ScanStateError {
                    err_message: o.message,
                    position: Some(o.current_position),
                }),
            }
        }
    }
}

#[derive(SimpleObject)]
pub struct ScanResult {
    pub progress: ScanCurrentState,
    pub voltage: Vec<f32>,
}
