use super::global;
use futures::Stream;
use futures_util::StreamExt;
use std::{collections::HashMap, pin::Pin, task::Poll};

// Forwards a stream of DataReply types, removing entries that have a
// decreasing timestamp (i.e. data duplicated in archive and live data
// streams.

struct FilterDupes<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    s: S,
    latest: HashMap<i32, f64>,
}

// Friendly function to wrap a stream with the FilterDupes stream.

pub fn filter_dupes(
    s: impl Stream<Item = global::DataReply> + Send + 'static + Unpin,
) -> impl Stream<Item = global::DataReply> + Send + 'static + Unpin {
    FilterDupes::new(s)
}

impl<S> FilterDupes<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    pub fn new(s: S) -> Self {
        FilterDupes {
            s,
            latest: HashMap::new(),
        }
    }
}

impl<S> Stream for FilterDupes<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    type Item = global::DataReply;

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
                    if !v.data.is_empty() {
                        return Poll::Ready(Some(v));
                    }
                }
                v @ Poll::Ready(None) => return v,
                v @ Poll::Pending => return v,
            }
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
                scalar_value: (ts / 2.0) as f32,
            }),
        }
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
            global::DataReply {
                ref_id: 0,
                data: vec![],
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
        let mut s = super::filter_dupes(stream::iter(input.clone()));

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
        assert!(s.next().await.is_none());
    }

    #[test]
    fn test_pending() {
        use futures::stream::{self, StreamExt};
        use futures::FutureExt;

        let mut s = super::filter_dupes(stream::pending());

        assert!(s.next().now_or_never().is_none());
    }
}
