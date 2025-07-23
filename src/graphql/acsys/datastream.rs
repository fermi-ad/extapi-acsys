use super::{global, DataStream};
use futures::Stream;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
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

                        // If there's no data in this packet, we've already
                        // updated the state of the data channel. But we need
                        // to do another `poll_next()` on the stream becaue
                        // it doesn't have a `Waker` associated with it (because
                        // it returned data) and we're not going to return an
                        // empty array.

                        if data.is_empty() {
                            continue;
                        }
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
                        let buf = self
                            .pending
                            .entry(ref_id)
                            .or_insert_with(DataChannel::new);

                        if let Some(data) = buf.process_live_data(data) {
                            if data.is_empty() {
                                warn!("received empty data packet");
                                continue;
                            } else {
                                return Poll::Ready(Some(global::DataReply {
                                    ref_id,
                                    data,
                                }));
                            }
                        } else {
                            continue;
                        }
                    }
                    Poll::Ready(None) => self.live_done = true,
                    Poll::Pending => {}
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

struct FilterDupes {
    s: DataStream,
    latest: HashMap<i32, f64>,
}

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
                    // If we get an data packet, drop it.

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

#[cfg(test)]
mod test {
    use super::{global, DataChannel};

    #[test]
    fn test_data_channel() {
        let mut chan = DataChannel::new();

        assert!(matches!(chan, DataChannel::Buffering(_)));

        assert_eq!(
            chan.process_archive_data(vec![global::DataInfo {
                timestamp: 100.0,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 10.0
                })
            }]),
            vec![global::DataInfo {
                timestamp: 100.0,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 10.0
                })
            }]
        );

        assert_eq!(
            chan.process_live_data(vec![
                global::DataInfo {
                    timestamp: 200.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 20.0
                    })
                },
                global::DataInfo {
                    timestamp: 210.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 21.0
                    })
                }
            ]),
            None
        );

        assert_eq!(
            chan.process_archive_data(vec![
                global::DataInfo {
                    timestamp: 110.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 11.0
                    })
                },
                global::DataInfo {
                    timestamp: 120.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 12.0
                    })
                }
            ]),
            vec![
                global::DataInfo {
                    timestamp: 110.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 11.0
                    })
                },
                global::DataInfo {
                    timestamp: 120.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 12.0
                    })
                }
            ]
        );

        assert_eq!(
            chan.process_archive_data(vec![]),
            vec![
                global::DataInfo {
                    timestamp: 200.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 20.0
                    })
                },
                global::DataInfo {
                    timestamp: 210.0,
                    result: global::DataType::Scalar(global::Scalar {
                        scalar_value: 21.0
                    })
                }
            ]
        );
    }
}
