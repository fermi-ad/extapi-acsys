use super::{
    super::types::{DataInfo, DataReply, DataType, StatusReply},
    DataStream,
};
use futures::Stream;
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    task::{Context, Poll},
};
use tracing::warn;

/// Implements the merge logic for a data channel. When the channel is
/// in buffering mode, it adds any new live data to its buffer. In feed
/// through mode, all live data is simply forwarded on.
#[derive(Debug, PartialEq)]
struct DataChannel {
    buffer: Option<Vec<DataInfo>>,
}
impl DataChannel {
    /// Creates a new data channel. Channels start in buffering mode with
    /// an empty buffer.
    fn new() -> Self {
        DataChannel {
            buffer: Some(vec![]),
        }
    }

    /// Returns the buffered data, if any.
    fn get_buffer(&mut self) -> Option<Vec<DataInfo>> {
        self.buffer.take()
    }

    /// Processes a chunk of live data.
    fn process_live_data(&mut self, live_data: Vec<DataInfo>) {
        self.buffer.get_or_insert_default().extend(live_data);
    }
}
impl Default for DataChannel {
    fn default() -> Self {
        Self::new()
    }
}

// This stream understands the format of our archive data stream. Archive
// data is sent as packets of `DataReply` structs, each containing an array
// of data points. If the array is empty, no more data will ever arrive.
// However, DPM doesn't close the stream because it allows a client to
// specify more than one device. In our case, we only ask for one device
// per stream. This wrapper Stream, once it sees and returns the empty
// array, will close the stream.

struct ArchiveStream {
    archived: DataStream,
    done: bool,
}

impl ArchiveStream {
    fn new(archived: DataStream) -> Self {
        ArchiveStream {
            archived,
            done: false,
        }
    }
}

pub fn as_archive_stream(s: DataStream) -> DataStream {
    Box::pin(ArchiveStream::new(s)) as DataStream
}

impl Stream for ArchiveStream {
    type Item = DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // If the stream is marked "done", close it.

        if self.done {
            Poll::Ready(None)
        } else {
            let mut reply = self.archived.poll_next_unpin(ctxt);

            if let Poll::Ready(Some(ref mut packet)) = reply {
                self.done = packet.data.is_empty();
            }
            reply
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
    type Item = DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // See if there's any archive data to process. If so, pass it
            // through the associated data channel.

            if !self.archived_done {
                match self.archived.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(DataReply { ref_id, data })) => {
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
                            if let DataType::StatusReply(StatusReply {
                                status,
                            }) = point.result
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

                        return Poll::Ready(Some(DataReply { ref_id, data }));
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
                    Poll::Ready(Some(DataReply { ref_id, data })) => {
                        if self.archived_done {
                            if data.is_empty() {
                                warn!("received empty data packet");
                            } else {
                                // TEMP: Log any error status. This won't
                                // be in the final product!

                                for point in &data {
                                    if let DataType::StatusReply(
                                        StatusReply { status },
                                    ) = point.result
                                    {
                                        warn!(
                                            "ref {} returned status [{} {}]",
                                            ref_id,
                                            status & 255,
                                            status / 256
                                        )
                                    }
                                }

                                return Poll::Ready(Some(DataReply {
                                    ref_id,
                                    data,
                                }));
                            }
                        } else {
                            // Look-up the data channel associated with the
                            // `ref_id`. If it doesn't exist, insert a new one.
                            let buf = self.pending.entry(ref_id).or_default();
                            buf.process_live_data(data);
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
                        return Poll::Ready(Some(DataReply {
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

// Forwards a stream of DataReply types, removing entries that have a
// decreasing timestamp (i.e. data duplicated in archive and live data
// streams.

struct FilterDupes {
    s: DataStream,
    latest: HashMap<i32, f64>,
}

// Friendly function to wrap a stream with the FilterDupes stream.

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
    type Item = DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
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
    type Item = DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
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
    use super::super::super::types::Scalar;
    use super::*;
    use futures::stream;

    fn data_info(ts: f64) -> DataInfo {
        DataInfo {
            timestamp: ts,
            result: DataType::Scalar(Scalar {
                scalar_value: ts / 2.0,
            }),
        }
    }

    #[test]
    fn test_data_channel() {
        let mut chan = DataChannel::new();
        // Add some live data to the channel. The resulting channel should now just contain the new data.
        let live_data = vec![data_info(200.0), data_info(210.0)];
        chan.process_live_data(live_data.clone());

        let expected = DataChannel {
            buffer: Some(live_data.clone()),
        };
        assert_eq!(chan, expected);

        // Add some more archived data. The buffer should have the new data appended.
        let new_data = vec![data_info(110.0), data_info(120.0)];

        chan.process_live_data(new_data.clone());
        assert_eq!(
            chan,
            DataChannel {
                buffer: Some(
                    [live_data.as_slice(), new_data.as_slice()].concat()
                )
            }
        );

        // See what happens when empty data is passed, for completeness
        chan.process_live_data(vec![]);
        assert_eq!(
            chan,
            DataChannel {
                buffer: Some(
                    [live_data.as_slice(), new_data.as_slice()].concat()
                )
            }
        );

        // Now get the buffer
        let result = chan.get_buffer();
        assert_eq!(
            result,
            Some([live_data.as_slice(), new_data.as_slice()].concat())
        );
        assert_eq!(chan.get_buffer(), None);
    }

    #[tokio::test]
    async fn test_merge_with_only_live() {
        let live_input = &[
            DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
            DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        ];
        let mut s = merge(
            Box::pin(stream::empty()) as DataStream,
            Box::pin(stream::iter(live_input.clone())) as DataStream,
        );

        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        );
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_archive_with_live() {
        let archive_input = &[
            DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            DataReply {
                ref_id: 0,
                data: vec![],
            },
        ];
        let live_input = &[
            DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
            DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        ];
        let mut s = merge(
            Box::pin(stream::iter(archive_input.clone())) as DataStream,
            Box::pin(stream::iter(live_input.clone())) as DataStream,
        );

        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        );
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn test_dedupe() {
        let input = &[
            // device channel 0 receives two data points. These should
            // go through.
            DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            // Another data point for device 0. This has the same timestamp
            // as the previous so it shouldn't appear in the output.
            DataReply {
                ref_id: 0,
                data: vec![data_info(110.0)],
            },
            // A different device has a data point. It should go through.
            DataReply {
                ref_id: 1,
                data: vec![data_info(100.0)],
            },
            // Shouldn't return the first element.
            DataReply {
                ref_id: 0,
                data: vec![data_info(105.0), data_info(115.0)],
            },
        ];
        let mut s =
            filter_dupes(Box::pin(stream::iter(input.clone())) as DataStream);

        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0),]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 1,
                data: vec![data_info(100.0),]
            },
        );
        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(115.0),]
            },
        );
    }

    #[tokio::test]
    async fn test_end_time() {
        let input = &[
            // device channel 0 receives two data points. These should
            // go through.
            DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            // Another data point for device 0. This timestamp exceeds the
            // end time so it shouldn't get sent.
            DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
            // A different device has a data point. It should go through.
            DataReply {
                ref_id: 1,
                data: vec![data_info(100.0)],
            },
            // Shouldn't return the second element. And the stream should
            // close after sending this data.
            DataReply {
                ref_id: 1,
                data: vec![data_info(110.0), data_info(120.0)],
            },
        ];
        let mut s = end_stream_at(
            Box::pin(stream::iter(input.clone())) as DataStream,
            2,
            Some(115.0),
        );

        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)]
            }
        );

        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 1,
                data: vec![data_info(100.0)]
            },
        );

        assert_eq!(
            s.next().await.unwrap(),
            DataReply {
                ref_id: 1,
                data: vec![data_info(110.0)]
            },
        );

        assert!(s.next().await.is_none());
    }
}
