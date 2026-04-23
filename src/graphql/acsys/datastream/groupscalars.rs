// Defines a stream that accumulates incoming scalar readings and
// emits them in a chunk. If the source stream closes, then the
// pending readings are sent. If a waveform comes in, each waveform is
// forwarded one at a time. The first element returned by the stream
// determines what data is forwarded.

use super::global;
use futures::Stream;
use futures_util::StreamExt;
use std::{pin::Pin, task::Poll};
use tracing::warn;

enum StreamState {
    Unknown,
    Scalar(global::DataReply),
    Waveform,
    Done,
}

pub struct GroupScalars<const MAX_PAYLOAD: usize, S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    archived: S,
    state: StreamState,
}

impl<const MAX_PAYLOAD: usize, S> GroupScalars<MAX_PAYLOAD, S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    pub fn new(archived: S) -> Self {
        GroupScalars {
            archived,
            state: StreamState::Unknown,
        }
    }
}

pub fn group_scalars<const MAX_PAYLOAD: usize, S>(
    s: S,
) -> GroupScalars<MAX_PAYLOAD, S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    GroupScalars::<MAX_PAYLOAD, S>::new(s)
}

impl<const MAX_PAYLOAD: usize, S> Stream for GroupScalars<MAX_PAYLOAD, S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    type Item = global::DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // If we're done, don't try to poll the stream. We go in
            // the "Done" state when the stream tells us it's done.
            // Some streams panic if you try to poll them after they
            // return Ready(None).

            if let StreamState::Done = self.state {
                return Poll::Ready(None);
            }

            match self.archived.poll_next_unpin(ctxt) {
                // The stream returned data. Handle the packet
                // appropriately, based upon the determined stream
                // type.
                Poll::Ready(Some(mut payload)) => match &mut self.state {
                    StreamState::Done => unreachable!(),

                    // The stream type hasn't been determined. Look at
                    // the first element in the data array to
                    // determine the stream type.
                    StreamState::Unknown => match *payload.data.as_slice() {
                        // If the stream has scalar data, set the type
                        // to "scalar".
                        [
                            global::DataInfo {
                                result: global::DataType::Scalar(_),
                                ..
                            },
                            ..,
                        ] => {
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
                        [
                            global::DataInfo {
                                result: global::DataType::ScalarArray(_),
                                ..
                            },
                            ..,
                        ] => {
                            self.state = StreamState::Waveform;
                            break Poll::Ready(Some(payload));
                        }

                        // We don't handle other types of data yet.
                        _ => {
                            warn!(
                                "archive stream contained non-scalar / non-waveform data"
                            );
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
                    StreamState::Scalar(pending) => {
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

                    if let StreamState::Scalar(pending) = &mut self.state
                        && !pending.data.is_empty()
                    {
                        let mut tmp = global::DataReply {
                            ref_id: pending.ref_id,
                            data: vec![],
                        };

                        std::mem::swap(pending, &mut tmp);
                        self.state = StreamState::Done;
                        break Poll::Ready(Some(tmp));
                    }
                    break Poll::Ready(None);
                }

                // Nothing to read. Return Pending.
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::global;

    fn scalar_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: ts / 2.0,
            }),
        }
    }

    fn waveform_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::ScalarArray(global::ScalarArray {
                scalar_array_value: vec![
                    ts / 2.0,
                    ts / 2.0 + 1.0,
                    ts / 2.0 + 2.0,
                ],
            }),
        }
    }

    #[tokio::test]
    async fn test_grouping_waveforms() {
        use futures::stream::{self, StreamExt};

        let input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(100.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(110.0)],
            },
            // Should return both waveforms.
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(120.0), waveform_info(130.0)],
            },
        ];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(100.0)]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(110.0),]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(120.0), waveform_info(130.0)]
            },
        );
    }

    #[tokio::test]
    async fn test_grouping_scalars() {
        use futures::stream::{self, StreamExt};

        let input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(100.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(110.0)],
            },
            // Should return both waveforms.
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(120.0), scalar_info(130.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(140.0)],
            },
        ];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(100.0),
                    scalar_info(110.0),
                    scalar_info(120.0),
                    scalar_info(130.0)
                ]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(140.0),]
            },
        );
    }
}
