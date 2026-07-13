use super::global;
use futures::Stream;
use futures_util::StreamExt;
use std::{pin::Pin, task::Poll};

// This stream understands the format of our archive data stream. Archive
// data is sent as packets of `DataReply` structs, each containing an array
// of data points. If the array is empty, no more data will ever arrive.
// However, DPM doesn't close the stream because it allows a client to
// specify more than one device. In our case, we only ask for one device
// per stream. This wrapper Stream, once it sees and returns the empty
// array, will close the stream.
//
// A device that is in a PEND (or other error) state will send a status
// reply instead of the normal empty-array sentinel. We treat a status
// reply as a terminal condition as well, so that the live-data channel
// is not left waiting for a sentinel that will never arrive.

pub struct ArchiveStream<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    inner: Option<S>,
}

impl<S> ArchiveStream<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    pub fn new(archived: S) -> Self {
        ArchiveStream {
            inner: Some(archived),
        }
    }
}

#[inline(never)]
pub fn as_archive_stream<S>(
    s: S,
) -> impl Stream<Item = global::DataReply> + Send + 'static + Unpin
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    ArchiveStream::new(s)
}

impl<S> Stream for ArchiveStream<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    type Item = global::DataReply;

    fn poll_next(
        mut self: Pin<&mut Self>, ctxt: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(ref mut inner) = self.inner else {
            return Poll::Ready(None);
        };
        let reply = inner.poll_next_unpin(ctxt);

        if let Poll::Ready(Some(ref packet)) = reply {
            let is_terminal = packet.data.is_empty()
                || matches!(
                    packet.data.as_slice(),
                    [global::DataInfo {
                        result: global::DataType::StatusReply(_),
                        ..
                    }]
                );

            if is_terminal {
                self.inner = None;
            }
        }
        reply
    }
}

#[cfg(test)]
mod test {
    use super::global;
    use futures::stream::{self, StreamExt};

    fn data_info(ts: f64) -> global::DataInfo {
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
                scalar_array_value: vec![ts, ts + 1.0, ts + 2.0],
            }),
        }
    }

    fn status_info_code(ts: f64, code: i16) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::StatusReply(global::StatusReply {
                status: code,
            }),
        }
    }

    fn status_info(ts: f64) -> global::DataInfo {
        status_info_code(ts, -1)
    }

    fn sentinel(ref_id: i32) -> global::DataReply {
        global::DataReply {
            ref_id,
            data: vec![],
        }
    }

    // -----------------------------------------------------------------------
    // Terminal-condition tests

    #[tokio::test]
    async fn test_empty_sentinel_closes_stream() {
        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0), data_info(110.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![],
            }, // sentinel
            // This packet must never be delivered.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(120.0)],
            },
        ];

        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(100.0), data_info(110.0)]);

        // The empty sentinel is forwarded, then the stream closes.
        let r = s.next().await.unwrap();
        assert!(r.data.is_empty());

        assert!(s.next().await.is_none());
    }

    // Regression test: DPM sends a status reply (PEND) instead of the
    // normal empty-array sentinel when a device is unavailable. The
    // ArchiveStream must treat this as a terminal condition so that
    // DataChannel is not left in Buffering mode forever, silently
    // swallowing all subsequent live data.
    #[tokio::test]
    async fn test_status_reply_closes_stream() {
        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(100.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![status_info(200.0)], // PEND instead of sentinel
            },
            // This packet must never be delivered.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(300.0)],
            },
        ];

        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(100.0)]);

        // The status reply is forwarded, then the stream closes.
        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![status_info(200.0)]);

        assert!(s.next().await.is_none());
    }

    // The empty sentinel as the very first packet: no data precedes it.
    // The sentinel is still forwarded and the stream closes immediately.
    #[tokio::test]
    async fn test_sentinel_as_first_packet() {
        let input = [
            sentinel(0),
            // Must never be delivered.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0)],
            },
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert!(r.data.is_empty());

        assert!(s.next().await.is_none());
    }

    // A status reply as the very first packet (device immediately in PEND).
    #[tokio::test]
    async fn test_status_reply_as_first_packet() {
        let input = [
            global::DataReply {
                ref_id: 5,
                data: vec![status_info(0.0)],
            },
            // Must never be delivered.
            global::DataReply {
                ref_id: 5,
                data: vec![data_info(1.0)],
            },
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![status_info(0.0)]);

        assert!(s.next().await.is_none());
    }

    // A status reply with a specific non-(-1) ACNET code is still treated
    // as a terminal condition.
    #[tokio::test]
    async fn test_status_reply_with_specific_code_closes_stream() {
        // ACNET status: facility 17, error 42 → (42 * 256 + 17) as i16
        let code = (42i16 * 256) + 17;
        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![status_info_code(2.0, code)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(3.0)],
            },
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(1.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![status_info_code(2.0, code)]);

        assert!(s.next().await.is_none());
    }

    // A packet whose `data` vec contains a status reply alongside other
    // elements is NOT a terminal condition — the `matches!` guard only fires
    // on a single-element slice.
    #[tokio::test]
    async fn test_multi_element_packet_with_status_is_not_terminal() {
        let input = [
            global::DataReply {
                ref_id: 0,
                // Two elements: a status and a scalar — not a sentinel.
                data: vec![status_info(1.0), data_info(2.0)],
            },
            sentinel(0),
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        // The mixed packet must be forwarded as-is.
        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![status_info(1.0), data_info(2.0)]);

        // The sentinel closes the stream.
        let r = s.next().await.unwrap();
        assert!(r.data.is_empty());

        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Normal pass-through tests

    // An empty source stream produces no items.
    #[tokio::test]
    async fn test_empty_source_stream() {
        let input: Vec<global::DataReply> = vec![];
        let mut s = super::as_archive_stream(stream::iter(input));

        assert!(s.next().await.is_none());
    }

    // When the underlying source closes naturally (no sentinel), the
    // ArchiveStream closes too without panicking.
    #[tokio::test]
    async fn test_source_closes_naturally_without_sentinel() {
        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(2.0)],
            },
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(1.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(2.0)]);

        assert!(s.next().await.is_none());
    }

    // Multiple data packets before the sentinel are all forwarded.
    #[tokio::test]
    async fn test_multiple_data_packets_then_sentinel() {
        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0), data_info(2.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(3.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(4.0), data_info(5.0), data_info(6.0)],
            },
            sentinel(0),
            // Must never be delivered.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(99.0)],
            },
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(1.0), data_info(2.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(3.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(
            r.data,
            vec![data_info(4.0), data_info(5.0), data_info(6.0)]
        );

        let r = s.next().await.unwrap();
        assert!(r.data.is_empty());

        assert!(s.next().await.is_none());
    }

    // Waveform (ScalarArray) data passes through without being treated as
    // a terminal condition.
    #[tokio::test]
    async fn test_waveform_data_passes_through() {
        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(10.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![waveform_info(20.0)],
            },
            sentinel(0),
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![waveform_info(10.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![waveform_info(20.0)]);

        let r = s.next().await.unwrap();
        assert!(r.data.is_empty());

        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // ref_id and structural tests

    // The ref_id on every forwarded packet must be preserved exactly.
    #[tokio::test]
    async fn test_ref_id_is_preserved() {
        let input = [
            global::DataReply {
                ref_id: 7,
                data: vec![data_info(1.0)],
            },
            global::DataReply {
                ref_id: 7,
                data: vec![data_info(2.0)],
            },
            sentinel(7),
        ];
        let mut s = super::as_archive_stream(stream::iter(input));

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 7);

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 7);

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 7);
        assert!(r.data.is_empty());

        assert!(s.next().await.is_none());
    }

    // Polling the stream after it has been closed (inner = None) must
    // continue to return None without panicking.
    #[tokio::test]
    async fn test_polling_after_close_returns_none() {
        let input = [sentinel(0)];
        let mut s = super::as_archive_stream(stream::iter(input));

        // Consume the sentinel.
        let r = s.next().await.unwrap();
        assert!(r.data.is_empty());

        // Every subsequent poll must return None.
        assert!(s.next().await.is_none());
        assert!(s.next().await.is_none());
        assert!(s.next().await.is_none());
    }
}
