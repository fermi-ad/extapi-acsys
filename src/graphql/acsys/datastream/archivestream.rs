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

    fn status_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::StatusReply(global::StatusReply {
                status: -1,
            }),
        }
    }

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
}
