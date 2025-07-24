use super::{global, DataStream};
use futures::Stream;
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    task::{Context, Poll},
};
use tracing::warn;

// Implements the merge logic for a data channel. When the channel is
// in buffering mode, it adds any new live data to its buffer. In feed
// through mode, all live data is simply forwarded on.

enum DataChannel {
    Buffering(Vec<global::DataInfo>),
    FeedThrough,
}

impl DataChannel {
    // Creates a new data channel. Channels start in buffering mode with
    // an empty buffer.

    pub fn new() -> Self {
        DataChannel::Buffering(vec![])
    }

    // Processes a chunk of live data.

    pub fn process_live_data(
        &mut self, mut live_data: Vec<global::DataInfo>,
    ) -> Option<Vec<global::DataInfo>> {
        match self {
            // In feedthrough mode, we simply pass on the live data.
            Self::FeedThrough => Some(live_data),

            // If in buffering mode, we append the data and return
            // `None` so the caller knows there's nothing to do.
            Self::Buffering(ref mut data) => {
                data.append(&mut live_data);
                None
            }
        }
    }

    // Process a chunk of archive data.

    pub fn process_archive_data(
        &mut self, archive_data: Vec<global::DataInfo>,
    ) -> Vec<global::DataInfo> {
        match self {
            // We shouldn't get archived data once we've entered
            // feed-through mode. The producer made a mistake. Generate
            // a log message and pass on the data; the timestamps will
            // probably be earlier and will get filtered by a later stage.
            Self::FeedThrough => {
                warn!("received archived data after end was specified");
                archive_data
            }

            // If we're in buffer mode, the contents of this archive
            // packet determines what comes next.
            Self::Buffering(data) => {
                // If the archived data is empty, there won't be any more
                // from the archiver. We switch to FeedThrough mode and
                // return our buffered data.

                if archive_data.is_empty() {
                    let mut tmp = vec![];

                    std::mem::swap(data, &mut tmp);
                    *self = Self::FeedThrough;
                    tmp
                } else {
                    // If there's archive data, pass it on.

                    archive_data
                }
            }
        }
    }
}

// This stream type merges a stream of archive data with a stream of live
// data. It has to take several things into consideration:
//
//   1) All the live data, for a given `refId` must be delivered *after*
//      all the archived data for that refId have been sent.
//   2) We pull data from all streams so that producers don't close our
//      incoming streams. This means the stream supplying live data should
//      be polled and the data buffered until the archived data has been
//      delivered.

struct DataMerge {
    archived: DataStream,
    archived_done: bool,
    live: DataStream,
    live_done: bool,
    pending: HashMap<i32, DataChannel>,
}

// Useful combinator that assembles the internal stream type.

pub fn merge(archived: DataStream, live: DataStream) -> DataStream {
    Box::pin(DataMerge::new(archived, live)) as DataStream
}

impl DataMerge {
    pub fn new(archived: DataStream, live: DataStream) -> Self {
        DataMerge {
            archived,
            archived_done: false,
            live,
            live_done: false,
            pending: HashMap::new(),
        }
    }
}

impl Stream for DataMerge {
    type Item = global::DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {

            // If we receive live data, we need to buffer it. We could
            // let the gRPC socket do the buffering. But a large archiver
            // request could take a while to send over and we don't want
            // DPM to get tired of us not acknowledging live data.

            if !self.live_done {
                match self.live.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(global::DataReply { ref_id, data })) => {
                        let buf = self
                            .pending
                            .entry(ref_id)
                            .or_insert_with(DataChannel::new);

                        if let Some(data) = buf.process_live_data(data) {
                            if data.is_empty() {
                                warn!("received empty data packet");
                            } else {
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
                        // channel's archive data is done. We don't foreward
                        // empty data packets, so we need to loop and let
                        // the archive stream have a chance to return more
                        // data or register a Waker.

                        if data.is_empty() {
                            continue;
                        }

                        // Return the data (either archve data or buffered
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

            return if self.archived_done && self.live_done {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        }
    }
}

// Forwards a stream of DataReply types, removing entries that have a
// decreasing timestamp (i.e. data duplicated in archive and live data
// streams.

struct FilterDupes {
    s: DataStream,
    latest: HashMap<i32, f64>,
}

// Friendy function to wrap a stream with the FilterDupes stream.

pub fn filter_dupes(s: DataStream) -> DataStream {
    Box::pin(FilterDupes::new(s))
}

impl FilterDupes {
    pub fn new(s: DataStream) -> Self {
        FilterDupes {
            s,
            latest: HashMap::new(),
        }
    }
}

impl Stream for FilterDupes {
    type Item = global::DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<std::option::Option<Self::Item>> {
        loop {
            match self.s.poll_next_unpin(ctxt) {
                Poll::Ready(Some(mut v)) => {
                    // If we get an empty data packet, drop it.

                    if v.data.is_empty() {
                        continue;
                    }

                    let entry = self.latest.entry(v.ref_id).or_insert(0.0);

                    // Find the starting point in the data in which the
                    // timestamp is greater than the last one seen.

                    let start_index = v.data[..]
                        .partition_point(|info| info.timestamp <= *entry);

                    // Update the last seen timestamp. WE NEED TO DO THIS
                    // BEFORE DRAINING ANY DUPLICATES. You might think we
                    // can always use `v.data.last()`, but it could be the
                    // case that, after draining, there's no elements in the
                    // vector so we do this while we know it's still safe.

                    *entry = entry.max(v.data.last().unwrap().timestamp);

                    // Remove any readings that have already been sent.

                    v.data.drain(..start_index);
                    if v.data.is_empty() {
                        continue;
                    }

                    break Poll::Ready(Some(v));
                }
                v @ Poll::Ready(None) => break v,
                v @ Poll::Pending => break v,
            }
        }
    }
}

struct EndOnDate {
    s: DataStream,
    end_date: f64,
    remaining: HashSet<i32>,
}

impl EndOnDate {
    pub fn new(s: DataStream, end_date: f64, total: i32) -> Self {
        EndOnDate {
            s,
            end_date,
            remaining: (0..total).collect(),
        }
    }
}

pub fn end_stream_at(
    s: DataStream, total: i32, end_date: Option<f64>,
) -> DataStream {
    if let Some(ts) = end_date {
        Box::pin(EndOnDate::new(s, ts, total)) as DataStream
    } else {
        s
    }
}

impl Stream for EndOnDate {
    type Item = global::DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<std::option::Option<<Self as Stream>::Item>> {
        loop {
            match self.s.poll_next_unpin(ctxt) {
                Poll::Ready(Some(mut v)) => {
                    // Find the starting point in the data in which the
                    // timestamp is less than or equal to the last one seen.

                    let start_index = v.data[..].partition_point(|info| {
                        info.timestamp <= self.end_date
                    });

                    // Remove any readings that have already been sent.

                    v.data.drain(start_index..);

                    // If the data is empty, then we need to remove the
                    // ref ID from our set to mark that device as complete.

                    if v.data.is_empty() {
                        self.remaining.remove(&v.ref_id);

                        // If all the devices have exceeded the end time,
                        // close the stream.

                        if self.remaining.is_empty() {
                            break Poll::Ready(None);
                        }

                        // We pulled data from the stream but aren't
                        // forwarding it on. We have to loop again to
                        // poll the stream because there is currently
                        // no waker registered with it.

                        continue;
                    } else {
                        break Poll::Ready(Some(v));
                    }
                }
                v @ Poll::Ready(None) => break v,
                v @ Poll::Pending => break v,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{global, DataChannel};

    fn data_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: ts / 2.0,
            }),
        }
    }

    #[test]
    fn test_data_channel() {
        let mut chan = DataChannel::new();

        // Assert a new channel is in buffer mode.

        assert!(matches!(chan, DataChannel::Buffering(_)));

        // Run an archive packet through. The channel should return
        // it, as is.

        assert_eq!(
            chan.process_archive_data(vec![data_info(100.0)]),
            vec![data_info(100.0)]
        );

        // Add some live data to the channel. Since we're in buffer
        // mode, live data is saved and `None` should be returned.

        assert_eq!(
            chan.process_live_data(vec![data_info(200.0), data_info(210.0),]),
            None
        );

        // Add some more archived data. The array should still be
        // returned.

        assert_eq!(
            chan.process_archive_data(
                vec![data_info(110.0), data_info(120.0),]
            ),
            vec![data_info(110.0), data_info(120.0),]
        );

        // Send an empty archive packet. This signifies no more archive
        // data will be received. The channel should return the buffered
        // data and switch to feed-through mode.

        assert_eq!(
            chan.process_archive_data(vec![]),
            vec![data_info(200.0), data_info(210.0)]
        );

        // Now add live data. It should get passed through.

        assert_eq!(
            chan.process_live_data(vec![data_info(220.0), data_info(230.0)]),
            Some(vec![data_info(220.0), data_info(230.0)])
        );
    }

    #[tokio::test]
    async fn test_merge() {
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
            Box::pin(stream::iter(archive_input.clone())) as super::DataStream,
            Box::pin(stream::iter(live_input.clone())) as super::DataStream
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
                data: vec![data_info(120.0), data_info(130.0)],
            },
	);
	assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn test_dedupe() {
        use futures::stream::{self, StreamExt};

        let input = &[
            // device channel 0 receives two data points. These should
            // go through.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            // Another data point for device 0. This has the same timestamp
            // as the previous so it shouldn't appear in the output.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(110.0)],
            },
            // A different device has a data point. It should go through.
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(100.0)],
            },
            // Shouldn't return the first element.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(105.0), data_info(115.0)],
            },
        ];
        let mut s = super::filter_dupes(
            Box::pin(stream::iter(input.clone())) as super::DataStream
        );

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0),]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(100.0),]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(115.0),]
            },
        );
    }

    #[tokio::test]
    async fn test_end_time() {
        use futures::stream::{self, StreamExt};

        let input = &[
            // device channel 0 receives two data points. These should
            // go through.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            // Another data point for device 0. This timestamp exceeds the
            // end time so it shouldn't get sent.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
            // A different device has a data point. It should go through.
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(100.0)],
            },
            // Shouldn't return the second element. And the stream should
            // close after sending this data.
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(110.0), data_info(120.0)],
            },
        ];
        let mut s = super::end_stream_at(
            Box::pin(stream::iter(input.clone())) as super::DataStream,
            2,
            Some(115.0),
        );

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)]
            }
        );

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(100.0)]
            },
        );

        assert_eq!(
            s.next().await.unwrap(),
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(110.0)]
            },
        );

        assert!(s.next().await.is_none());
    }
}
