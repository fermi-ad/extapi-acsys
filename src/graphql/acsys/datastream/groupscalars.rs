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
    #[inline(never)]
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
                            pending.data.reserve(amount_to_move);
                            pending.data.extend_from_slice(
                                &payload.data[..amount_to_move],
                            );
                            // Remove the transferred items from payload
                            payload.data.drain(..amount_to_move);
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

    fn status_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::StatusReply(global::StatusReply {
                status: -1,
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Waveform tests

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
        assert!(s.next().await.is_none());
    }

    // A single waveform packet is forwarded and the stream closes.
    #[tokio::test]
    async fn test_single_waveform() {
        use futures::stream::{self, StreamExt};

        let input = &[global::DataReply {
            ref_id: 3,
            data: vec![waveform_info(50.0)],
        }];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 3,
                data: vec![waveform_info(50.0)]
            },
        );
        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Scalar tests

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
        assert!(s.next().await.is_none());
    }

    // A single scalar is held in the pending buffer and flushed when the
    // source stream closes.
    #[tokio::test]
    async fn test_single_scalar_flushed_on_close() {
        use futures::stream::{self, StreamExt};

        let input = &[global::DataReply {
            ref_id: 7,
            data: vec![scalar_info(42.0)],
        }];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 7,
                data: vec![scalar_info(42.0)]
            },
        );
        assert!(s.next().await.is_none());
    }

    // Scalars that accumulate to exactly MAX_PAYLOAD are emitted as one
    // chunk; the stream then closes without a trailing flush.
    #[tokio::test]
    async fn test_scalars_fill_exactly_max_payload() {
        use futures::stream::{self, StreamExt};

        // 4 individual packets → one full chunk of 4, nothing left over.
        let input: Vec<global::DataReply> = (0..4)
            .map(|i| global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(i as f64 * 10.0)],
            })
            .collect();
        let mut s = super::group_scalars::<4, _>(stream::iter(input));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(0.0),
                    scalar_info(10.0),
                    scalar_info(20.0),
                    scalar_info(30.0),
                ]
            },
        );
        // No leftover — stream should be done.
        assert!(s.next().await.is_none());
    }

    // The very first packet already contains MAX_PAYLOAD scalars. It must be
    // emitted immediately (the `>= MAX_PAYLOAD` branch in Unknown state).
    #[tokio::test]
    async fn test_first_packet_exactly_max_payload() {
        use futures::stream::{self, StreamExt};

        let input = &[global::DataReply {
            ref_id: 0,
            data: vec![
                scalar_info(1.0),
                scalar_info(2.0),
                scalar_info(3.0),
                scalar_info(4.0),
            ],
        }];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(1.0),
                    scalar_info(2.0),
                    scalar_info(3.0),
                    scalar_info(4.0),
                ]
            },
        );
        // The pending buffer is now empty; stream closes cleanly.
        assert!(s.next().await.is_none());
    }

    // The very first packet contains MORE than MAX_PAYLOAD scalars. The
    // excess must be retained and emitted in a subsequent chunk.
    #[tokio::test]
    async fn test_first_packet_exceeds_max_payload() {
        use futures::stream::{self, StreamExt};

        // 6 scalars in one shot with MAX_PAYLOAD = 4.
        let input = &[global::DataReply {
            ref_id: 0,
            data: vec![
                scalar_info(1.0),
                scalar_info(2.0),
                scalar_info(3.0),
                scalar_info(4.0),
                scalar_info(5.0),
                scalar_info(6.0),
            ],
        }];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        // First chunk: the first MAX_PAYLOAD items.
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(1.0),
                    scalar_info(2.0),
                    scalar_info(3.0),
                    scalar_info(4.0),
                ]
            },
        );
        // Remaining 2 items flushed when the source closes.
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(5.0), scalar_info(6.0),]
            },
        );
        assert!(s.next().await.is_none());
    }

    // A payload that is much larger than MAX_PAYLOAD is split only once
    // during the Unknown→Scalar transition: the first MAX_PAYLOAD items are
    // emitted immediately and the remainder is stored as pending. When the
    // source stream closes, the entire pending buffer is flushed as a single
    // chunk (the implementation does not re-split on flush).
    #[tokio::test]
    async fn test_large_single_packet_multiple_chunks() {
        use futures::stream::{self, StreamExt};

        // 10 scalars in one packet, MAX_PAYLOAD = 3.
        // Poll 1 (Unknown state): emits [0,1,2], stores [3..9] as pending.
        // Poll 2 (stream closes): flushes all 7 remaining items as one chunk.
        let input = &[global::DataReply {
            ref_id: 0,
            data: (0..10).map(|i| scalar_info(i as f64)).collect(),
        }];
        let mut s = super::group_scalars::<3, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(0.0),
                    scalar_info(1.0),
                    scalar_info(2.0),
                ]
            },
        );
        // The remaining 7 items are flushed as a single chunk when the source
        // stream closes — the implementation does not re-split pending data.
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: (3..10)
                    .map(|i| scalar_info(i as f64))
                    .collect::<Vec<_>>()
            },
        );
        assert!(s.next().await.is_none());
    }

    // With MAX_PAYLOAD = 1 every scalar must be emitted immediately.
    #[tokio::test]
    async fn test_max_payload_one_emits_each_scalar_immediately() {
        use futures::stream::{self, StreamExt};

        let input: Vec<global::DataReply> = (0..4)
            .map(|i| global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(i as f64 * 5.0)],
            })
            .collect();
        let mut s = super::group_scalars::<1, _>(stream::iter(input));

        for i in 0..4u32 {
            assert_eq!(
                s.next().await.unwrap(),
                global::DataReply {
                    ref_id: 0,
                    data: vec![scalar_info(i as f64 * 5.0)]
                },
            );
        }
        assert!(s.next().await.is_none());
    }

    // The ref_id from the first packet must be preserved in all emitted
    // chunks, even when data spans multiple source packets.
    #[tokio::test]
    async fn test_ref_id_is_preserved() {
        use futures::stream::{self, StreamExt};

        let input: Vec<global::DataReply> = (0..6)
            .map(|i| global::DataReply {
                ref_id: 42,
                data: vec![scalar_info(i as f64)],
            })
            .collect();
        let mut s = super::group_scalars::<4, _>(stream::iter(input));

        let chunk1 = s.next().await.unwrap();
        assert_eq!(chunk1.ref_id, 42);
        assert_eq!(chunk1.data.len(), 4);

        let chunk2 = s.next().await.unwrap();
        assert_eq!(chunk2.ref_id, 42);
        assert_eq!(chunk2.data.len(), 2);

        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Empty / degenerate stream tests

    // An empty source stream must produce no items.
    #[tokio::test]
    async fn test_empty_stream() {
        use futures::stream::{self, StreamExt};

        let input: Vec<global::DataReply> = vec![];
        let mut s = super::group_scalars::<4, _>(stream::iter(input));

        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Unknown / unsupported data-type tests

    // A packet whose first element is neither Scalar nor ScalarArray must
    // cause the stream to terminate immediately.
    #[tokio::test]
    async fn test_unknown_data_type_terminates_stream() {
        use futures::stream::{self, StreamExt};

        let input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![status_info(1.0)],
            },
            // This packet should never be reached.
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(2.0)],
            },
        ];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        // The stream must close without yielding any item.
        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Boundary / accumulation tests

    // Two packets that together exactly fill MAX_PAYLOAD, followed by one
    // more packet — verifies the boundary between a full chunk and a flush.
    #[tokio::test]
    async fn test_two_packets_fill_then_one_more() {
        use futures::stream::{self, StreamExt};

        // MAX_PAYLOAD = 4; first two packets contribute 2 each (fills buffer),
        // third packet contributes 1 (flushed on close).
        let input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(1.0), scalar_info(2.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(3.0), scalar_info(4.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(5.0)],
            },
        ];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(1.0),
                    scalar_info(2.0),
                    scalar_info(3.0),
                    scalar_info(4.0),
                ]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(5.0)]
            },
        );
        assert!(s.next().await.is_none());
    }

    // An incoming packet that would overflow the pending buffer must be split:
    // the portion that fits is appended to the pending buffer (which is then
    // emitted), and the remainder becomes the new pending buffer.
    #[tokio::test]
    async fn test_overflow_packet_splits_correctly() {
        use futures::stream::{self, StreamExt};

        // Pending has 3 items; next packet has 3 items; MAX_PAYLOAD = 4.
        // Expected: emit [a,b,c,d], then flush [e,f].
        let input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(1.0),
                    scalar_info(2.0),
                    scalar_info(3.0),
                ],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(4.0),
                    scalar_info(5.0),
                    scalar_info(6.0),
                ],
            },
        ];
        let mut s = super::group_scalars::<4, _>(stream::iter(input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![
                    scalar_info(1.0),
                    scalar_info(2.0),
                    scalar_info(3.0),
                    scalar_info(4.0),
                ]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![scalar_info(5.0), scalar_info(6.0),]
            },
        );
        assert!(s.next().await.is_none());
    }
}
