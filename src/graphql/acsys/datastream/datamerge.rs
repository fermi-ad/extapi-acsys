use super::{DataChannel, datachannel::BufferResult, global};
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
    archived: Option<SA>,
    live: Option<SL>,
    pending: HashMap<i32, (DataChannel, f64)>,
}

// Useful combinator that assembles the internal stream type.
// This uses a closure to create a type-erased stream that composes
// different stream implementations without boxing.

#[inline(never)]
pub fn merge<SA, SL>(
    archived: Option<SA>, live: Option<SL>,
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
    pub fn new(archived: Option<SA>, live: Option<SL>) -> Self {
        DataMerge {
            archived,
            live,
            pending: HashMap::new(),
        }
    }

    /// Consumes an iterator of data points, filters out stale entries, and
    /// returns the surviving items as a `Vec`. Updates `latest` with the
    /// highest real-data timestamp seen.
    ///
    /// The single pass applies these rules to each item in order:
    ///
    /// - **Stale data** (`timestamp <= latest`): silently discarded.
    /// - **Fatal status** (negative `status`): emitted, watermark advanced to
    ///   its timestamp, iteration stops — everything after it is dropped.
    /// - **Warning status** (positive `status`): emitted as-is; the watermark
    ///   is NOT advanced because the timestamp is a synthetic wall-clock value
    ///   unrelated to the device's data timeline.
    /// - **Normal data**: emitted and the watermark is advanced.
    fn filter_and_update_latest(
        iter: impl Iterator<Item = global::DataInfo>, latest: &mut f64,
    ) -> Vec<global::DataInfo> {
        let mut done = false;

        iter.scan(&mut *latest, |watermark, item| {
            if done {
                return None;
            }
            match &item.result {
                global::DataType::StatusReply(global::StatusReply {
                    status,
                }) if *status < 0 => {
                    // Fatal: emit, advance watermark, stop.
                    **watermark = watermark.max(item.timestamp);
                    done = true;
                    Some(Some(item))
                }
                global::DataType::StatusReply(global::StatusReply {
                    status,
                }) if *status > 0 => {
                    // Warning: emit without touching the watermark.
                    Some(Some(item))
                }
                _ if item.timestamp <= **watermark => {
                    // Stale: discard.
                    Some(None)
                }
                _ => {
                    // Normal data: emit and advance watermark.
                    **watermark = watermark.max(item.timestamp);
                    Some(Some(item))
                }
            }
        })
        .flatten()
        .collect()
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

            if let Some(ref mut archived) = this.archived {
                match archived.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(global::DataReply { ref_id, data })) => {
                        let (chan, latest) = this
                            .pending
                            .entry(ref_id)
                            .or_insert_with(|| (DataChannel::new(), 0.0));

                        if let Some(raw) = chan.process_archive_data(data) {
                            let data = Self::filter_and_update_latest(
                                raw.into_iter(),
                                latest,
                            );
                            if !data.is_empty() {
                                return Poll::Ready(Some(global::DataReply {
                                    ref_id,
                                    data,
                                }));
                            }
                        }
                        continue;
                    }
                    Poll::Ready(None) => this.archived = None,
                    Poll::Pending => (),
                }
            }

            if let Some(ref mut live) = this.live {
                match live.poll_next_unpin(ctxt) {
                    Poll::Ready(Some(global::DataReply { ref_id, data }))
                        if !data.is_empty() =>
                    {
                        let (chan, latest) = this
                            .pending
                            .entry(ref_id)
                            .or_insert_with(|| (DataChannel::new(), 0.0));

                        match chan
                            .process_live_data(data, this.archived.is_none())
                        {
                            BufferResult::Data(None) => {}
                            BufferResult::Data(Some(raw)) => {
                                let data = Self::filter_and_update_latest(
                                    raw.into_iter(),
                                    latest,
                                );
                                if !data.is_empty() {
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
                        warn!(
                            "received empty live data packet for ref_id {ref_id}"
                        );
                    }
                    Poll::Ready(None) => this.live = None,
                    Poll::Pending => {}
                }
            }

            // Both, archive and live stream, are done. Flush any pending data
            // in the channels before shutting down the stream.

            if this.archived.is_none() && this.live.is_none() {
                if let Some(&ref_id) = this.pending.keys().next() {
                    let (mut chan, mut latest) =
                        this.pending.remove(&ref_id).unwrap();
                    if let Some(raw) = chan.get_buffer() {
                        let data = Self::filter_and_update_latest(
                            raw.into_iter(),
                            &mut latest,
                        );
                        if !data.is_empty() {
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
    use super::{DataMerge, global};

    fn data_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: ts / 2.0,
            }),
        }
    }

    fn bad_status_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::StatusReply(global::StatusReply {
                status: -1,
            }),
        }
    }

    fn warn_status_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::StatusReply(global::StatusReply {
                status: 1,
            }),
        }
    }

    // -----------------------------------------------------------------------
    // filter_and_update_latest unit tests (white-box)

    type DM = DataMerge<
        futures::stream::Empty<global::DataReply>,
        futures::stream::Empty<global::DataReply>,
    >;

    fn filter(
        input: Vec<global::DataInfo>, latest: &mut f64,
    ) -> Vec<global::DataInfo> {
        DM::filter_and_update_latest(input.into_iter(), latest)
    }

    // An empty vec returns an empty vec and leaves `latest` unchanged.
    #[test]
    fn test_filter_empty_vec_returns_false() {
        let mut latest = 0.0_f64;
        assert!(filter(vec![], &mut latest).is_empty());
        assert_eq!(latest, 0.0);
    }

    // All data points are at or below the watermark → all filtered, empty vec.
    #[test]
    fn test_filter_all_duplicates_returns_false() {
        let mut latest = 110.0_f64;
        let data =
            filter(vec![data_info(100.0), data_info(110.0)], &mut latest);
        assert!(data.is_empty());
        // latest must not regress
        assert_eq!(latest, 110.0);
    }

    // Some points are below the watermark, some above → partial filter.
    #[test]
    fn test_filter_partial_dedupe() {
        let mut latest = 105.0_f64;
        let data = filter(
            vec![data_info(100.0), data_info(105.0), data_info(110.0)],
            &mut latest,
        );
        assert_eq!(data, vec![data_info(110.0)]);
        assert_eq!(latest, 110.0);
    }

    // All points are above the watermark → nothing filtered, watermark advances.
    #[test]
    fn test_filter_no_duplicates_advances_watermark() {
        let mut latest = 50.0_f64;
        let data = filter(
            vec![data_info(60.0), data_info(70.0), data_info(80.0)],
            &mut latest,
        );
        assert_eq!(
            data,
            vec![data_info(60.0), data_info(70.0), data_info(80.0)]
        );
        assert_eq!(latest, 80.0);
    }

    // A single-element status reply passes through and does NOT advance the
    // watermark (the regression guard).
    #[test]
    fn test_filter_status_reply_passes_through_without_advancing_watermark() {
        let mut latest = 50.0_f64;
        let data = filter(vec![bad_status_info(1000.0)], &mut latest);
        // Status must be preserved.
        assert_eq!(data, vec![bad_status_info(1000.0)]);
        // Watermark must NOT have advanced.
        assert_eq!(latest, 1000.0);
    }

    // A multi-element packet that happens to contain a status reply is NOT
    // treated as a pure status packet — the normal timestamp path applies.
    #[test]
    fn test_filter_multi_element_with_status_uses_normal_path() {
        {
            let mut latest = 0.0_f64;
            let data = filter(
                vec![
                    data_info(100.0),
                    bad_status_info(1000.0),
                    data_info(200.0),
                ],
                &mut latest,
            );
            // data_info(100.0) and bad_status_info(1000.0) survive; fatal
            // status truncates data_info(200.0).
            assert_eq!(data, vec![data_info(100.0), bad_status_info(1000.0)]);
            // Watermark advances to the fatal status's timestamp.
            assert_eq!(latest, 1000.0);
        }
        {
            let mut latest = 0.0_f64;
            let data = filter(
                vec![
                    data_info(100.0),
                    warn_status_info(1000.0),
                    data_info(200.0),
                ],
                &mut latest,
            );
            // All three survive; warning status does not truncate.
            assert_eq!(
                data,
                vec![
                    data_info(100.0),
                    warn_status_info(1000.0),
                    data_info(200.0)
                ]
            );
            // Watermark advances to the last real data point, not the warning.
            assert_eq!(latest, 200.0);
        }
    }

    // -----------------------------------------------------------------------
    // merge() integration tests

    // Both streams None → stream closes immediately.
    #[tokio::test]
    async fn test_merge_both_none() {
        use futures::stream::{self, StreamExt};

        let mut s = super::merge(
            None::<stream::Empty<global::DataReply>>,
            None::<stream::Empty<global::DataReply>>,
        );
        assert!(s.next().await.is_none());
    }

    // Archive-only (no live stream) — data passes through and stream closes.
    #[tokio::test]
    async fn test_merge_with_only_archive() {
        use futures::stream::{self, StreamExt};

        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(10.0), data_info(20.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(30.0)],
            },
        ];
        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            None::<stream::Empty<_>>,
        );

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(10.0), data_info(20.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(30.0)]);

        assert!(s.next().await.is_none());
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
        let mut s = super::merge(
            None::<stream::Empty<_>>,
            Some(stream::iter(live_input)),
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
        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            Some(stream::iter(live_input)),
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

    // Live data that arrives while the archive is still open must be buffered
    // and released (in order) once the archive sentinel arrives.
    #[tokio::test]
    async fn test_live_data_buffered_until_archive_sentinel() {
        use futures::stream::{self, StreamExt};

        // Archive: one data packet then the sentinel.
        // Live: two packets that arrive "concurrently" with the archive.
        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(10.0), data_info(20.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![], // sentinel
            },
        ];
        let live_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(30.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(40.0)],
            },
        ];
        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            Some(stream::iter(live_input)),
        );

        // Archive data comes first.
        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(10.0), data_info(20.0)]);

        // After the sentinel the buffered live data is released.
        // The two live packets may be merged into one or emitted separately
        // depending on how many poll cycles occur; collect all remaining items.
        let mut all_data: Vec<global::DataInfo> = vec![];
        while let Some(r) = s.next().await {
            all_data.extend(r.data);
        }
        assert_eq!(all_data, vec![data_info(30.0), data_info(40.0)]);
    }

    // Live data whose timestamps exactly equal the watermark boundary must be
    // filtered (the partition uses `<=`).
    #[tokio::test]
    async fn test_live_data_at_exact_watermark_is_filtered() {
        use futures::stream::{self, StreamExt};

        // Archive ends at 110.0; live sends 110.0 (duplicate) and 120.0 (new).
        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![], // sentinel
            },
        ];
        let live_input = [global::DataReply {
            ref_id: 0,
            data: vec![data_info(110.0), data_info(120.0)],
        }];
        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            Some(stream::iter(live_input)),
        );

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(100.0), data_info(110.0)]);

        // 110.0 must be filtered; only 120.0 survives.
        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(120.0)]);

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

        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            Some(stream::iter(live_input)),
        );

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

    // Multiple ref_ids with archive-only (no live stream).
    #[tokio::test]
    async fn test_merge_multiple_ref_ids_archive_only() {
        use futures::stream::{self, StreamExt};

        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0), data_info(2.0)],
            },
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(10.0), data_info(20.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(3.0)],
            },
        ];
        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            None::<stream::Empty<_>>,
        );

        let r0a = s.next().await.unwrap();
        assert_eq!(r0a.ref_id, 0);
        assert_eq!(r0a.data, vec![data_info(1.0), data_info(2.0)]);

        let r1 = s.next().await.unwrap();
        assert_eq!(r1.ref_id, 1);
        assert_eq!(r1.data, vec![data_info(10.0), data_info(20.0)]);

        let r0b = s.next().await.unwrap();
        assert_eq!(r0b.ref_id, 0);
        assert_eq!(r0b.data, vec![data_info(3.0)]);

        assert!(s.next().await.is_none());
    }

    // Multiple ref_ids with live-only (no archive stream).
    #[tokio::test]
    async fn test_merge_multiple_ref_ids_live_only() {
        use futures::stream::{self, StreamExt};

        let live_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0)],
            },
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(10.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(2.0)],
            },
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(20.0)],
            },
        ];
        let mut s = super::merge(
            None::<stream::Empty<_>>,
            Some(stream::iter(live_input)),
        );

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 0);
        assert_eq!(r.data, vec![data_info(1.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 1);
        assert_eq!(r.data, vec![data_info(10.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 0);
        assert_eq!(r.data, vec![data_info(2.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 1);
        assert_eq!(r.data, vec![data_info(20.0)]);

        assert!(s.next().await.is_none());
    }

    // Regression test: DPM sends a PEND status reply (with a synthetic
    // wall-clock timestamp) before it sends the actual device data (whose
    // hardware timestamp predates the synthetic one). The status packet must
    // NOT advance the `latest` watermark, otherwise the subsequent real data
    // points are silently discarded as "already seen".
    #[tokio::test]
    async fn test_status_reply_does_not_advance_watermark() {
        use futures::stream::{self, StreamExt};

        // Simulate: PEND status arrives at wall-clock time 1000.0, then
        // real data arrives with a hardware timestamp of 120.0 (which is
        // less than 1000.0 and would be filtered if the watermark were
        // incorrectly advanced).
        let live_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![warn_status_info(1000.0)], // synthetic "now()" timestamp
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)], // real hardware timestamp
            },
        ];

        let mut s = super::merge(
            None::<stream::Empty<_>>,
            Some(stream::iter(live_input)),
        );

        // The status reply must come through.
        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 0);
        assert_eq!(r.data, vec![warn_status_info(1000.0)]);

        // The real data must NOT be swallowed by the watermark.
        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 0);
        assert_eq!(r.data, vec![data_info(120.0)]);

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

        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            None::<stream::Empty<_>>,
        );

        assert_eq!(s.next().await.unwrap().data.len(), 2);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(115.0)]);
        assert!(s.next().await.is_none());
    }

    // A live packet whose entire data vec is at or below the watermark must
    // be silently dropped (filter returns false → nothing emitted).
    #[tokio::test]
    async fn test_live_data_entirely_below_watermark_is_dropped() {
        use futures::stream::{self, StreamExt};

        // Archive establishes watermark at 200.0; live sends stale data.
        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(200.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![], // sentinel
            },
        ];
        let live_input = [
            global::DataReply {
                ref_id: 0,
                // All of these are at or below the watermark of 200.0.
                data: vec![data_info(150.0), data_info(200.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(300.0)], // this one is new
            },
        ];
        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            Some(stream::iter(live_input)),
        );

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(100.0), data_info(200.0)]);

        // The stale live packet must be silently dropped; only 300.0 arrives.
        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(300.0)]);

        assert!(s.next().await.is_none());
    }

    // An earlier version of the filtering function used
    // `.partition_point()` which uses a binary search and,
    // therefore, requires the data to be in strict ascending order.
    // The timestamps, however, are not guaranteed to be sorted.
    // This test ensures that the filtering function does not rely
    // on sorted data and correctly filters out stale points even
    // when the input is unsorted.
    #[test]
    fn test_filter_unsorted_stale_point_is_removed() {
        let mut latest = 60.0_f64;
        let data = filter(
            vec![
                data_info(30.0),
                data_info(100.0),
                data_info(50.0),
                data_info(110.0),
            ],
            &mut latest,
        );
        // 30.0 and 50.0 are both ≤ 60.0 and must be filtered.
        assert_eq!(data, vec![data_info(100.0), data_info(110.0)]);
        assert_eq!(latest, 110.0);
    }

    // An archive stream that closes naturally (no sentinel) with no live
    // stream — all data passes through and the merge stream closes.
    #[tokio::test]
    async fn test_archive_closes_naturally_no_live() {
        use futures::stream::{self, StreamExt};

        let archive_input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(5.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(10.0)],
            },
        ];
        let mut s = super::merge(
            Some(stream::iter(archive_input)),
            None::<stream::Empty<_>>,
        );

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(5.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(10.0)]);

        assert!(s.next().await.is_none());
    }
}
