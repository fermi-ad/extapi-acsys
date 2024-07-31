use crate::g_rpc::xform;

use async_graphql::*;
use chrono::TimeZone;
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use tracing::{error, info};

// Pull in global types.

use super::types as global;

// Pull in our local types.

pub mod types;

fn xlat_expr(expr: &types::XFormExpr) -> Option<Box<xform::proto::Operation>> {
    match expr {
        types::XFormExpr {
            dev_ex: Some(types::XFormDeviceExpr { device }),
            avg_ex: None,
        } => Some(Box::new(xform::proto::Operation {
            op: Some(xform::proto::operation::Op::Device(device.into())),
        })),
        types::XFormExpr {
            dev_ex: None,
            avg_ex: Some(types::XFormAvgExpr { expr, n }),
        } => Some(Box::new(xform::proto::Operation {
            op: Some(xform::proto::operation::Op::Avg(Box::new(
                xform::proto::Average {
                    n: *n,
                    op: xlat_expr(&*expr),
                },
            ))),
        })),
        _ => None,
    }
}

fn xlat_xform_reply(
    res: tonic::Result<xform::proto::ExprResult>,
) -> types::XFormResult {
    if let Ok(xform::proto::ExprResult {
        timestamp,
        result: Some(xform::proto::expr_result::Result::Value(value)),
        ..
    }) = res
    {
        if let chrono::MappedLocalTime::Single(timestamp) =
            chrono::Utc.timestamp_millis_opt(timestamp.try_into().unwrap())
        {
            types::XFormResult {
                timestamp,
                result: global::Scalar {
                    scalar_value: value,
                },
            }
        } else {
            error!("bad timestamp");
            unreachable!()
        }
    } else {
        error!("xform returned error: {:?}", &res);
        unreachable!()
    }
}

type XFormStream = Pin<Box<dyn Stream<Item = types::XFormResult> + Send>>;

#[derive(Default)]
pub struct XFormSubscriptions;

#[Subscription]
impl XFormSubscriptions {
    async fn calc_stream(&self, config: types::XFormRequest) -> XFormStream {
        info!("calculating {}", &config.expr);

        if let Some(expr) = xlat_expr(&config.expr) {
            match xform::activate_expression(config.event, expr).await {
                Ok(s) => Box::pin(s.into_inner().map(xlat_xform_reply))
                    as XFormStream,
                Err(e) => {
                    error!("{}", &e);
                    Box::pin(stream::empty()) as XFormStream
                }
            }
        } else {
            Box::pin(stream::empty()) as XFormStream
        }
    }
}
