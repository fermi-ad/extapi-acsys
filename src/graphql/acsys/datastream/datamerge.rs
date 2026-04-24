use super::{datachannel::BufferResult, global, DataChannel};
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
    pending: HashMap<i32, (DataChannel, f64)>,
}

// Useful combinator that assembles the internal stream type.
// This uses a closure to create a type-erased stream that composes
// different stream implementations without boxing.

#[inline(never)]
pub fn merge<SA, SL>(
    archived: SA, live: SL,
) -> impl Stream<Item = global::DataReply> + Send + 'static + Unpin
where
    SA: Stream<Item = global::DataReply> + Send + 'static + Unpin,
    SL: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
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

    /// Filters out data points that have already been seen based on the
    /// `latest` timestamp and updates `latest` with the newest timestamp in
    /// the batch. Returns true if there is data left to emit.

    fn filter_and_update_latest(
        data: &mut Vec<global::DataInfo>, latest: &mut f64,
    ) -> bool {
        if data.is_empty() {
            return false;
        }

        let start = data.partition_point(|info| info.timestamp <= *latest);

        // Remember the latest timestamp we've seen.

        *latest = (*latest).max(data.last().unwrap().timestamp);

        // Throw away any data points we've already seen.

        if start > 0 {
            data.drain(0..start);
        }

        !data.is_empty()
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
        let this = &mut *self;
        loop {
            // See if there's any archive data to process. If so, pass it
            // through the associated data channel.

            if !this.archived_done {
                match this.archived.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(global::DataReply { ref_id, data })) => {
                        let (chan, latest) = this
                            .pending
                            .entry(ref_id)
                            .or_insert_with(|| (DataChannel::new(), 0.0));

                        if let Some(mut data) = chan.process_archive_data(data)
                        {
                            if Self::filter_and_update_latest(&mut data, latest)
                            {
                                return Poll::Ready(Some(global::DataReply {
                                    ref_id,
                                    data,
                                }));
                            }
                        }
                        continue;
                    }
                    Poll::Ready(None) => this.archived_done = true,
                    Poll::Pending => (),
                }
            }

            if !this.live_done {
                match this.live.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(global::DataReply { ref_id, data }))
                        if !data.is_empty() =>
                    {
                        let (chan, latest) = this
                            .pending
                            .entry(ref_id)
                            .or_insert_with(|| (DataChannel::new(), 0.0));

                        match chan.process_live_data(data, this.archived_done) {
                            BufferResult::Data(None) => {}
                            BufferResult::Data(Some(mut data)) => {
                                if Self::filter_and_update_latest(
                                    &mut data, latest,
                                ) {
                                    return Poll::Ready(Some(
                                        global::DataReply { ref_id, data },
                                    ));
                                }
                            }
                            BufferResult::Overflow => {
                                warn!("buffer overflow for ref_id {ref_id}");
                                return Poll::Ready(None);
                            }
                        }
                        continue;
                    }
                    Poll::Ready(Some(global::DataReply { ref_id, .. })) => {
                        warn!("received empty live data packet for ref_id {ref_id}");
                    }
                    Poll::Ready(None) => this.live_done = true,
                    Poll::Pending => {}
                }
            }

            // Both, archive and live stream, are done. Flush any pending data
            // in the channels before shutting down the stream.

            if this.archived_done && this.live_done {
                if let Some(&ref_id) = this.pending.keys().next() {
                    let (mut chan, mut latest) =
                        this.pending.remove(&ref_id).unwrap();
                    if let Some(mut data) = chan.get_buffer() {
                        if Self::filter_and_update_latest(
                            &mut data,
                            &mut latest,
                        ) {
                            return Poll::Ready(Some(global::DataReply {
                                ref_id,
                                data,
                            }));
                        }
                    }
                    continue;
                }
                return Poll::Ready(None);
            }

            return Poll::Pending;
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

        let live_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        ];
        let mut s = super::merge(stream::empty(), stream::iter(live_input));

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

        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![],
            },
        ];
        let live_input = [
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
            super::merge(stream::iter(archive_input), stream::iter(live_input));

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

    #[tokio::test]
    async fn test_merge_multiple_ref_ids_dedupe() {
        use futures::stream::{self, StreamExt};

        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(200.0), data_info(210.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![], // End archive for 0
            },
        ];
        let live_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(110.0), data_info(120.0)], // 110 is dupe
            },
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(210.0), data_info(220.0)], // 210 is dupe
            },
        ];

        let mut s =
            super::merge(stream::iter(archive_input), stream::iter(live_input));

        // Expect archive data for ref 0 and 1
        assert_eq!(s.next().await.unwrap().ref_id, 0);
        assert_eq!(s.next().await.unwrap().ref_id, 1);

        // Live ref 0 should have 110 filtered out
        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 0);
        assert_eq!(r.data, vec![data_info(120.0)]);

        // Live ref 1 should have 210 filtered out
        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 1);
        assert_eq!(r.data, vec![data_info(220.0)]);

        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_decreasing_timestamps() {
        use futures::stream::{self, StreamExt};

        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(105.0), data_info(115.0)], // 105 should be filtered
            },
        ];

        let mut s = super::merge(stream::iter(archive_input), stream::empty());

        assert_eq!(s.next().await.unwrap().data.len(), 2);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(115.0)]);
        assert!(s.next().await.is_none());
    }
}
