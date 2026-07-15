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

#[inline(never)]
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

                    v.data.truncate(start_index);

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

    // -----------------------------------------------------------------------
    // Original tests (kept intact)

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

    // -----------------------------------------------------------------------
    // end_date = None (Either::Right pass-through path)

    // With no end_date the stream is a transparent pass-through.
    #[tokio::test]
    async fn test_no_end_date_passes_all_data_through() {
        use futures::stream::{self, StreamExt};

        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0), data_info(2.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(3.0)],
            },
        ];
        let mut s = super::end_stream_at(stream::iter(input), 1, None);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(1.0), data_info(2.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(3.0)]);

        assert!(s.next().await.is_none());
    }

    // With no end_date and an empty source the stream closes immediately.
    #[tokio::test]
    async fn test_no_end_date_empty_source_closes_immediately() {
        use futures::stream::{self, StreamExt};

        let input: Vec<global::DataReply> = vec![];
        let mut s = super::end_stream_at(stream::iter(input), 1, None);

        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Empty source stream

    // An empty source with an end_date closes immediately.
    #[tokio::test]
    async fn test_empty_source_with_end_date_closes_immediately() {
        use futures::stream::{self, StreamExt};

        let input: Vec<global::DataReply> = vec![];
        let mut s = super::end_stream_at(stream::iter(input), 1, Some(100.0));

        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Single-device scenarios

    // Single device, all data within end_date → all forwarded; stream closes
    // when the source closes naturally.
    #[tokio::test]
    async fn test_single_device_all_within_end_date() {
        use futures::stream::{self, StreamExt};

        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(10.0), data_info(20.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(30.0)],
            },
        ];
        let mut s = super::end_stream_at(stream::iter(input), 1, Some(100.0));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(10.0), data_info(20.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(30.0)]);

        assert!(s.next().await.is_none());
    }

    // Single device, first packet entirely exceeds end_date → data truncated
    // to empty, device removed, stream closes immediately.
    #[tokio::test]
    async fn test_single_device_first_packet_entirely_beyond_end_date() {
        use futures::stream::{self, StreamExt};

        let input = [
            global::DataReply {
                ref_id: 0,
                // All timestamps exceed end_date of 5.0.
                data: vec![data_info(10.0), data_info(20.0)],
            },
            // Must never be delivered.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(30.0)],
            },
        ];
        let mut s = super::end_stream_at(stream::iter(input), 1, Some(5.0));

        // The packet is truncated to empty → device removed → stream closes.
        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Timestamp boundary tests

    // A timestamp exactly equal to end_date must be included (`<=`).
    #[tokio::test]
    async fn test_timestamp_exactly_at_end_date_is_included() {
        use futures::stream::{self, StreamExt};

        let input = [global::DataReply {
            ref_id: 0,
            data: vec![data_info(100.0), data_info(115.0)],
        }];
        let mut s = super::end_stream_at(stream::iter(input), 1, Some(115.0));

        let r = s.next().await.unwrap();
        // Both points must be present (115.0 == end_date).
        assert_eq!(r.data, vec![data_info(100.0), data_info(115.0)]);

        assert!(s.next().await.is_none());
    }

    // A timestamp one epsilon above end_date must be excluded.
    #[tokio::test]
    async fn test_timestamp_just_above_end_date_is_excluded() {
        use futures::stream::{self, StreamExt};

        let end = 115.0_f64;
        let just_above = end + f64::EPSILON * end; // next representable f64 above 115.0

        let input = [global::DataReply {
            ref_id: 0,
            data: vec![data_info(100.0), data_info(just_above)],
        }];
        let mut s = super::end_stream_at(stream::iter(input), 1, Some(end));

        let r = s.next().await.unwrap();
        // Only 100.0 survives; just_above is truncated.
        assert_eq!(r.data, vec![data_info(100.0)]);

        assert!(s.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Multi-device scenarios

    // Two devices: device 0 exceeds end_date first; stream stays open until
    // device 1 also exceeds it.
    #[tokio::test]
    async fn test_two_devices_one_exceeds_before_other() {
        use futures::stream::{self, StreamExt};

        let input = [
            // Device 0 exceeds end_date immediately.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(200.0)],
            },
            // Device 1 still has valid data.
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(50.0), data_info(100.0)],
            },
            // Device 1 now exceeds end_date → stream closes.
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(200.0)],
            },
            // Must never be delivered.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(300.0)],
            },
        ];
        let mut s = super::end_stream_at(stream::iter(input), 2, Some(150.0));

        // Device 0's packet is entirely beyond end_date → silently dropped.
        // Device 1's first packet passes through.
        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 1);
        assert_eq!(r.data, vec![data_info(50.0), data_info(100.0)]);

        // Device 1's second packet exceeds end_date → both devices done → close.
        assert!(s.next().await.is_none());
    }

    // A packet that is partially within and partially beyond end_date is
    // truncated at the boundary.
    #[tokio::test]
    async fn test_partial_packet_truncated_at_end_date() {
        use futures::stream::{self, StreamExt};

        let input = [global::DataReply {
            ref_id: 0,
            data: vec![
                data_info(10.0),
                data_info(20.0),
                data_info(30.0), // beyond end_date of 25.0
                data_info(40.0), // beyond end_date of 25.0
            ],
        }];
        let mut s = super::end_stream_at(stream::iter(input), 1, Some(25.0));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(10.0), data_info(20.0)]);

        // Device 0 is now done → stream closes.
        assert!(s.next().await.is_none());
    }

    // Source closes naturally before any device exceeds end_date → stream
    // closes too (the `Poll::Ready(None)` arm).
    #[tokio::test]
    async fn test_source_closes_naturally_before_end_date() {
        use futures::stream::{self, StreamExt};

        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(1.0)],
            },
            global::DataReply {
                ref_id: 1,
                data: vec![data_info(2.0)],
            },
        ];
        // end_date is far in the future — source will close first.
        let mut s = super::end_stream_at(stream::iter(input), 2, Some(9999.0));

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 0);
        assert_eq!(r.data, vec![data_info(1.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.ref_id, 1);
        assert_eq!(r.data, vec![data_info(2.0)]);

        // Source exhausted → stream closes.
        assert!(s.next().await.is_none());
    }

    // A device sends multiple packets all within end_date, then one that
    // exceeds it — only the exceeding packet triggers device removal.
    #[tokio::test]
    async fn test_device_sends_multiple_valid_packets_then_exceeds() {
        use futures::stream::{self, StreamExt};

        let input = [
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(10.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(20.0)],
            },
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(30.0)],
            },
            // This packet exceeds end_date of 25.0.
            global::DataReply {
                ref_id: 0,
                data: vec![data_info(40.0)],
            },
        ];
        let mut s = super::end_stream_at(stream::iter(input), 1, Some(25.0));

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(10.0)]);

        let r = s.next().await.unwrap();
        assert_eq!(r.data, vec![data_info(20.0)]);

        // 30.0 > 25.0 → truncated to empty → device removed → stream closes.
        assert!(s.next().await.is_none());
    }
}
