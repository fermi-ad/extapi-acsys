// Defines a stream that accumulates incoming scalar readings and
// emits them in a chunk. If the source stream closes, then the
// pending readings are sent. If a waveform comes in, each waveform is
// forwarded one at a time. The first element returned by the stream
// determines what data is forwarded.

use super::{global, DataStream};
use futures::Stream;
use futures_util::StreamExt;
use std::{pin::Pin, task::Poll};
use tracing::warn;

enum StreamState {
    Unknown,
    Scalar(global::DataReply),
    Waveform,
}

pub struct GroupScalars<const MAX_PAYLOAD: usize> {
    archived: DataStream,
    state: StreamState,
}

impl<const MAX_PAYLOAD: usize> GroupScalars<MAX_PAYLOAD> {
    pub fn new(archived: DataStream) -> Self {
        GroupScalars {
            archived,
            state: StreamState::Unknown,
        }
    }
}

pub fn group_scalars<const MAX_PAYLOAD: usize>(s: DataStream) -> DataStream {
    Box::pin(GroupScalars::<MAX_PAYLOAD>::new(s)) as DataStream
}

impl<const MAX_PAYLOAD: usize> Stream for GroupScalars<MAX_PAYLOAD> {
    type Item = global::DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.archived.poll_next_unpin(ctxt) {
                // The stream returned data. Handle the packet
                // appropriately, based upon the determined stream
                // type.
                Poll::Ready(Some(mut payload)) => match &mut self.state {
                    // The stream type hasn't been determined. Look at
                    // the first element in the data array to
                    // determine the stream type.
                    StreamState::Unknown => match *payload.data.as_slice() {
                        // If the stream has scalar data, set the type
                        // to "scalar".
                        [global::DataInfo {
                            result: global::DataType::Scalar(_),
                            ..
                        }, ..] => {
                            if payload.data.len() >= MAX_PAYLOAD {
                                let mut tmp = global::DataReply {
                                    ref_id: payload.ref_id,
                                    data: Vec::with_capacity(MAX_PAYLOAD),
                                };

                                tmp.data
                                    .extend(payload.data.drain(..MAX_PAYLOAD));
                                self.state = StreamState::Scalar(payload);
                                break Poll::Ready(Some(tmp));
                            } else {
                                self.state = StreamState::Scalar(payload);
                            }
                        }

                        // If the stream has waveform data, set the
                        // type of the stream to "waveform" for future
                        // data.
                        [global::DataInfo {
                            result: global::DataType::ScalarArray(_),
                            ..
                        }, ..] => {
                            self.state = StreamState::Waveform;
                            break Poll::Ready(Some(payload));
                        }

                        // We don't handle other types of data yet.
                        _ => {
                            warn!("archive stream contained non-scalar / non-waveform data");
                            break Poll::Ready(None);
                        }
                    },

                    // For waveform streams, each packet is simply
                    // forwarded.
                    StreamState::Waveform => {
                        break Poll::Ready(Some(payload));
                    }

                    // If this is a Scalar stream, we append the
                    // incoming data to the pending data until it
                    // reaches MAX_PAYLOAD. Once it fills, the
                    // accumulated data is returned.
                    StreamState::Scalar(ref mut pending) => {
                        let space =
                            MAX_PAYLOAD.saturating_sub(pending.data.len());
                        let amount_to_move =
                            std::cmp::min(space, payload.data.len());

                        if amount_to_move > 0 {
                            pending
                                .data
                                .extend(payload.data.drain(..amount_to_move));
                        }

                        // If the payload data isn't empty, it means
                        // the pending buffer is full and should be
                        // sent out.

                        if !payload.data.is_empty() {
                            // Swap `pending` and `payload` so that
                            // the state contains the extra and the
                            // local variable (payload) can be sent
                            // and dropped.

                            std::mem::swap(pending, &mut payload);
                            break Poll::Ready(Some(payload));
                        }
                    }
                },

                // Stream is done.
                Poll::Ready(None) => {
                    // If the stream returns scalar data, return
                    // any pending data.

                    if let StreamState::Scalar(ref mut pending) =
                        &mut self.state
                    {
                        if !pending.data.is_empty() {
                            let mut tmp = global::DataReply {
                                ref_id: pending.ref_id,
                                data: vec![],
                            };

                            std::mem::swap(pending, &mut tmp);
                            break Poll::Ready(Some(tmp));
                        }
                    }
                    break Poll::Ready(None);
                }

                // Nothing to read. Return Pending.
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}
