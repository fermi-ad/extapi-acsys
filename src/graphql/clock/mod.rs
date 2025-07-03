use crate::g_rpc::{clock, proto::services::aclk};

use async_graphql::*;
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use tracing::{error, info};

// Pull in our local types.

pub mod types;

type EventStream = Pin<Box<dyn Stream<Item = types::EventInfo> + Send>>;

#[derive(Default)]
pub struct ClockSubscriptions;

#[Subscription]
impl ClockSubscriptions {
    async fn report_events(&self, events: Vec<i32>) -> EventStream {
        info!("subscribing to clock events: {:?}", &events);
        match clock::subscribe(&events).await {
            Ok(s) => Box::pin(s.into_inner().map(Result::unwrap).map(
                |aclk::EventInfo { stamp, event, .. }| {
                    let stamp = stamp.unwrap();

                    types::EventInfo {
                        timestamp: (std::time::UNIX_EPOCH
                            + std::time::Duration::from_millis(
                                (stamp.seconds * 1_000) as u64
                                    + (stamp.nanos / 1_000_000) as u64,
                            ))
                        .into(),
                        event: event as u16,
                    }
                },
            )) as EventStream,
            Err(e) => {
                error!("{}", &e);
                Box::pin(stream::empty()) as EventStream
            }
        }
    }
}
