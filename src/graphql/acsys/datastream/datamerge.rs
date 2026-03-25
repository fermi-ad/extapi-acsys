use super::{DataChannel, global};
use futures::Stream;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::warn;

// This stream type merges a stream of archive data with a stream of live
// data. It has to take several things into consideration:
//
//   1) All the live data, for a given `refId` must be delivered *after*
//      all the archived data for that refId have been sent.
//   2) We pull data from all streams so that producers don't close our
//      incoming streams. This means the stream supplying live data should
//      be polled and the data buffered until the archived data has been
//      delivered.

pub struct DataMerge<SA, SL>
where
    SA: Stream<Item = global::DataReply> + Send + 'static + Unpin,
    SL: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    archived: SA,
    archived_done: bool,
    live: SL,
    live_done: bool,
    pending: HashMap<i32, DataChannel>,
}

// Useful combinator that assembles the internal stream type.

pub fn merge(
    archived: impl Stream<Item = global::DataReply> + Send + 'static + Unpin,
    live: impl Stream<Item = global::DataReply> + Send + 'static + Unpin,
) -> impl Stream<Item = global::DataReply> + Send + 'static + Unpin {
    DataMerge::new(archived, live)
}

impl<SA, SL> DataMerge<SA, SL>
where
    SA: Stream<Item = global::DataReply> + Send + 'static + Unpin,
    SL: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    pub fn new(archived: SA, live: SL) -> Self {
        DataMerge {
            archived,
            archived_done: false,
            live,
            live_done: false,
            pending: HashMap::new(),
        }
    }
}

impl<SA, SL> Stream for DataMerge<SA, SL>
where
    SA: Stream<Item = global::DataReply> + Send + 'static + Unpin,
    SL: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    type Item = global::DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // See if there's any archive data to process. If so, pass it
            // through the associated data channel.

            if !self.archived_done {
                match self.archived.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(global::DataReply { ref_id, data })) => {
                        let buf = self
                            .pending
                            .entry(ref_id)
                            .or_insert_with(DataChannel::new);
                        let data = buf.process_archive_data(data);

                        // If there's no data in this packet, then this
                        // channel's archive data is done. We don't forward
                        // empty data packets, so we need to loop and let
                        // the archive stream have a chance to return more
                        // data or register a Waker.

                        if data.is_empty() {
                            continue;
                        }

                        // TEMP: Log any error status. This won't
                        // be in the final product!

                        for point in &data {
                            if let global::DataInfo {
                                result:
                                    global::DataType::StatusReply(
                                        global::StatusReply { status },
                                    ),
                                ..
                            } = point
                            {
                                warn!(
                                    "ref {} returned status [{} {}]",
                                    ref_id,
                                    status & 255,
                                    status / 256
                                )
                            }
                        }

                        // Return the data (either archive data or buffered
                        // live data).

                        return Poll::Ready(Some(global::DataReply {
                            ref_id,
                            data,
                        }));
                    }
                    Poll::Ready(None) => self.archived_done = true,
                    Poll::Pending => (),
                }
            }

            // If we receive live data, we need to buffer it. We could
            // let the gRPC socket do the buffering. But a large archiver
            // request could take a while to send over and we don't want
            // DPM to get tired of us not acknowledging live data.

            if !self.live_done {
                match self.live.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(global::DataReply { ref_id, data })) => {
                        // Grab multiple references inside `Self`.

                        let DataMerge {
                            ref mut pending,
                            ref archived_done,
                            ..
                        } = *self;

                        // Look-up the data channel associated with the
                        // `ref_id`. If it doesn't exist, insert a new one.

                        let buf = pending
                            .entry(ref_id)
                            .or_insert_with(DataChannel::new);

                        if let Some(data) =
                            buf.process_live_data(data, *archived_done)
                        {
                            if data.is_empty() {
                                warn!("received empty data packet");
                            } else {
                                // TEMP: Log any error status. This won't
                                // be in the final product!

                                for point in &data {
                                    if let global::DataInfo {
                                        result:
                                            global::DataType::StatusReply(
                                                global::StatusReply { status },
                                            ),
                                        ..
                                    } = point
                                    {
                                        warn!(
                                            "ref {} returned status [{} {}]",
                                            ref_id,
                                            status & 255,
                                            status / 256
                                        )
                                    }
                                }

                                return Poll::Ready(Some(global::DataReply {
                                    ref_id,
                                    data,
                                }));
                            }
                        }
                        continue;
                    }
                    Poll::Ready(None) => self.live_done = true,
                    Poll::Pending => {}
                }
            }

            return if self.archived_done && self.live_done {
                // If both streams are exhausted, check to see if there's
                // any pending data to be sent.

                for (ref_id, buf) in self.pending.iter_mut() {
                    if let Some(data) = buf.get_buffer() {
                        return Poll::Ready(Some(global::DataReply {
                            ref_id: *ref_id,
                            data,
                        }));
                    }
                }
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::global;

    fn data_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: ts / 2.0,
            }),
        }
    }

    #[tokio::test]
    async fn test_merge_with_only_live() {
        use futures::stream::{self, StreamExt};

        let live_input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        ];
        let mut s =
            super::merge(stream::empty(), stream::iter(live_input.clone()));

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        );
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_archive_with_live() {
        use futures::stream::{self, StreamExt};

        let archive_input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![],
            },
        ];
        let live_input = &[
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        ];
        let mut s = super::merge(
            stream::iter(archive_input.clone()),
            stream::iter(live_input.clone()),
        );

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        );
        assert!(s.next().await.is_none());
    }
}
