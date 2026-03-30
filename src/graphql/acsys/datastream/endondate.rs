use super::global;
use futures::{Stream, future::Either};
use futures_util::StreamExt;
use std::{collections::HashSet, pin::Pin, task::Poll};

pub struct EndOnDate<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    s: S,
    end_date: f64,
    remaining: HashSet<i32>,
}

impl<S> EndOnDate<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    pub fn new(s: S, end_date: f64, total: i32) -> Self {
        EndOnDate {
            s,
            end_date,
            remaining: (0..total).collect(),
        }
    }
}

pub fn end_stream_at(
    s: impl Stream<Item = global::DataReply> + Send + 'static + Unpin,
    total: i32, end_date: Option<f64>,
) -> impl Stream<Item = global::DataReply> + Send + 'static + Unpin {
    if let Some(ts) = end_date {
        Either::Left(EndOnDate::new(s, ts, total))
    } else {
        Either::Right(s)
    }
}

impl<S> Stream for EndOnDate<S>
where
    S: Stream<Item = global::DataReply> + Send + 'static + Unpin,
{
    type Item = global::DataReply;

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
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(130.0)],
            },
        ];

        {
            let mut s = super::end_stream_at(
                stream::iter(input.clone()),
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
        {
            let mut s =
                super::end_stream_at(stream::iter(input.clone()), 2, None);

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
                    ref_id: 0,
                    data: vec![data_info(120.0)]
                },
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
                    data: vec![data_info(110.0), data_info(120.0)]
                },
            );

            assert_eq!(
                s.next().await.unwrap(),
                global::DataReply {
                    ref_id: 0,
                    data: vec![data_info(130.0)]
                },
            );

            assert!(s.next().await.is_none());
        }
    }

    #[test]
    fn test_pending() {
        use futures::FutureExt;
        use futures::stream::{self, StreamExt};

        let mut s = super::end_stream_at(stream::pending(), 2, Some(115.0));

        assert!(s.next().now_or_never().is_none());
    }
}
