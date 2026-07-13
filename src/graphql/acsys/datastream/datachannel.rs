// Defines a stream type called `DataChannel`. This stream takes two
// streams as input; one returns archive data and the other live data.
// While the archive stream returns data, any data from the live
// stream is queued up. Once the archived data stream is empty, then
// the live data is returned.

use super::global;
use tracing::warn;

#[derive(Debug, PartialEq)]
pub enum BufferResult {
    Overflow,
    Data(Option<Vec<global::DataInfo>>),
}

// Implements the merge logic for a data channel. When the channel is
// in buffering mode, it adds any new live data to its buffer. In feed
// through mode, all live data is simply forwarded on.
pub enum DataChannel {
    Buffering {
        buffered_data: Vec<global::DataInfo>,
    },
    FeedThrough,
}

impl DataChannel {
    // Creates a new data channel. Channels start in buffering mode with
    // an empty buffer.

    pub fn new() -> Self {
        DataChannel::Buffering {
            buffered_data: vec![],
        }
    }

    // Returns the buffered data, if any.

    pub fn get_buffer(&mut self) -> Option<Vec<global::DataInfo>> {
        match self {
            Self::FeedThrough => None,
            Self::Buffering { buffered_data } => {
                let result = std::mem::take(buffered_data);

                if result.is_empty() {
                    None
                } else {
                    Some(result)
                }
            }
        }
    }

    // Processes a chunk of live data.

    pub fn process_live_data(
        &mut self, mut live_data: Vec<global::DataInfo>, archive_done: bool,
    ) -> BufferResult {
        // If there's no live data to process, just return None. This should
        // never happen. If it does, we'll log the incident but won't update
        // the channel's state.

        if live_data.is_empty() {
            warn!("received empty live data packet");
            return BufferResult::Data(None);
        }

        match self {
            // In feedthrough mode, we simply pass on the live data.
            Self::FeedThrough => BufferResult::Data(Some(live_data)),

            // If in buffering mode, we append the data and return
            // `None` so the caller knows there's nothing to emit yet.
            Self::Buffering { buffered_data } => {
                if archive_done {
                    let mut result = std::mem::take(buffered_data);
                    *self = Self::FeedThrough;
                    BufferResult::Data(if result.is_empty() {
                        Some(live_data)
                    } else {
                        result.append(&mut live_data);
                        Some(result)
                    })
                } else {
                    if buffered_data.is_empty() {
                        *buffered_data = live_data;
                    } else if buffered_data.len() + live_data.len() <= 1000 {
                        buffered_data.append(&mut live_data);
                    } else {
                        warn!("live data buffer overflowed; dropping data");
                        buffered_data.clear();
                        return BufferResult::Overflow;
                    }
                    BufferResult::Data(None)
                }
            }
        }
    }

    // Process a chunk of archive data.

    pub fn process_archive_data(
        &mut self, archive_data: Vec<global::DataInfo>,
    ) -> Option<Vec<global::DataInfo>> {
        match self {
            // We shouldn't get archived data once we've entered
            // feed-through mode. The producer made a mistake. Generate
            // a log message and pass on the data; the timestamps will
            // probably be earlier and will get filtered by a later stage.
            Self::FeedThrough => {
                warn!("received archived data after end was specified");
                if archive_data.is_empty() {
                    None
                } else {
                    Some(archive_data)
                }
            }

            // If we're in buffer mode, the contents of this archive
            // packet determines what comes next.
            Self::Buffering { buffered_data } => {
                if archive_data.is_empty() {
                    let result = std::mem::take(buffered_data);

                    *self = Self::FeedThrough;
                    if result.is_empty() {
                        None
                    } else {
                        Some(result)
                    }
                } else {
                    Some(archive_data)
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{BufferResult, DataChannel};
    use crate::graphql::types as global;

    fn data_info(ts: f64) -> global::DataInfo {
        global::DataInfo {
            timestamp: ts,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: ts / 2.0,
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Original integration test (kept intact)

    #[test]
    fn test_data_channel() {
        let mut chan = DataChannel::new();

        // Assert a new channel is in buffer mode.

        assert!(matches!(chan, DataChannel::Buffering { .. }));

        // Run an archive packet through. The channel should return
        // it, as is.
        assert_eq!(
            chan.process_archive_data(vec![data_info(100.0)]),
            Some(vec![data_info(100.0)])
        );

        // Add some live data to the channel. Since we're in buffer
        // mode, live data is saved and `None` should be returned.
        assert!(matches!(
            chan.process_live_data(
                vec![data_info(200.0), data_info(210.0),],
                false
            ),
            BufferResult::Data(None)
        ));

        // Add some more archived data. The array should still be
        // returned.

        assert_eq!(
            chan.process_archive_data(vec![data_info(110.0), data_info(120.0)]),
            Some(vec![data_info(110.0), data_info(120.0)])
        );

        // Send an empty archive packet. This signifies no more archive
        // data will be received. The channel should return the buffered
        // data and switch to feed-through mode.
        assert_eq!(
            chan.process_archive_data(vec![]),
            Some(vec![data_info(200.0), data_info(210.0)])
        );

        // Assert the channel is now in feed-through mode.
        assert!(matches!(chan, DataChannel::FeedThrough));

        // Send another empty archive packet. It should warn and return None.
        // (The warning is logged, but the return value is None because there's no data).
        assert_eq!(chan.process_archive_data(vec![]), None);

        // Send an empty archive packet with data. It should warn and return Some(data).
        assert_eq!(
            chan.process_archive_data(vec![data_info(999.0)]),
            Some(vec![data_info(999.0)])
        );

        // Now add live data. It should get passed through.

        match chan
            .process_live_data(vec![data_info(220.0), data_info(230.0)], false)
        {
            BufferResult::Data(Some(data)) => {
                assert_eq!(data, vec![data_info(220.0), data_info(230.0)])
            }
            _ => panic!("unexpected result"),
        }
    }

    // -----------------------------------------------------------------------
    // DataChannel::new / initial state

    #[test]
    fn test_new_channel_is_buffering() {
        let chan = DataChannel::new();
        assert!(matches!(chan, DataChannel::Buffering { .. }));
    }

    // -----------------------------------------------------------------------
    // get_buffer tests

    // A fresh channel has an empty buffer → get_buffer returns None.
    #[test]
    fn test_get_buffer_on_empty_buffering_channel_returns_none() {
        let mut chan = DataChannel::new();
        assert_eq!(chan.get_buffer(), None);
        // Channel must still be in Buffering state.
        assert!(matches!(chan, DataChannel::Buffering { .. }));
    }

    // After buffering live data, get_buffer returns it and clears the buffer.
    #[test]
    fn test_get_buffer_returns_buffered_data_and_clears() {
        let mut chan = DataChannel::new();
        chan.process_live_data(vec![data_info(1.0), data_info(2.0)], false);

        let buf = chan.get_buffer();
        assert_eq!(buf, Some(vec![data_info(1.0), data_info(2.0)]));

        // Buffer must now be empty.
        assert_eq!(chan.get_buffer(), None);
    }

    // get_buffer on a FeedThrough channel always returns None.
    #[test]
    fn test_get_buffer_on_feedthrough_returns_none() {
        let mut chan = DataChannel::new();
        // Transition to FeedThrough via the empty-archive sentinel.
        chan.process_archive_data(vec![]);
        assert!(matches!(chan, DataChannel::FeedThrough));
        assert_eq!(chan.get_buffer(), None);
    }

    // -----------------------------------------------------------------------
    // process_archive_data tests

    // Non-empty archive data in Buffering mode is returned as-is.
    #[test]
    fn test_process_archive_data_nonempty_in_buffering_returns_data() {
        let mut chan = DataChannel::new();
        let result =
            chan.process_archive_data(vec![data_info(10.0), data_info(20.0)]);
        assert_eq!(result, Some(vec![data_info(10.0), data_info(20.0)]));
        // Still in Buffering mode.
        assert!(matches!(chan, DataChannel::Buffering { .. }));
    }

    // Empty archive sentinel with no buffered live data → None, switches to
    // FeedThrough.
    #[test]
    fn test_process_archive_sentinel_with_empty_buffer_returns_none() {
        let mut chan = DataChannel::new();
        let result = chan.process_archive_data(vec![]);
        assert_eq!(result, None);
        assert!(matches!(chan, DataChannel::FeedThrough));
    }

    // Empty archive sentinel with buffered live data → returns the buffer,
    // switches to FeedThrough.
    #[test]
    fn test_process_archive_sentinel_with_buffered_data_returns_buffer() {
        let mut chan = DataChannel::new();
        chan.process_live_data(vec![data_info(50.0), data_info(60.0)], false);

        let result = chan.process_archive_data(vec![]);
        assert_eq!(result, Some(vec![data_info(50.0), data_info(60.0)]));
        assert!(matches!(chan, DataChannel::FeedThrough));
    }

    // Non-empty archive data in FeedThrough mode is returned (with a warning).
    #[test]
    fn test_process_archive_data_nonempty_in_feedthrough_returns_data() {
        let mut chan = DataChannel::new();
        chan.process_archive_data(vec![]); // → FeedThrough
        let result = chan.process_archive_data(vec![data_info(99.0)]);
        assert_eq!(result, Some(vec![data_info(99.0)]));
    }

    // Empty archive data in FeedThrough mode returns None (with a warning).
    #[test]
    fn test_process_archive_sentinel_in_feedthrough_returns_none() {
        let mut chan = DataChannel::new();
        chan.process_archive_data(vec![]); // → FeedThrough
        let result = chan.process_archive_data(vec![]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // process_live_data tests

    // Empty live data in Buffering mode returns Data(None) without changing
    // state (the guard branch).
    #[test]
    fn test_process_live_data_empty_in_buffering_returns_none() {
        let mut chan = DataChannel::new();
        assert_eq!(
            chan.process_live_data(vec![], false),
            BufferResult::Data(None)
        );
        // Buffer must still be empty.
        assert_eq!(chan.get_buffer(), None);
    }

    // Empty live data in FeedThrough mode also returns Data(None).
    #[test]
    fn test_process_live_data_empty_in_feedthrough_returns_none() {
        let mut chan = DataChannel::new();
        chan.process_archive_data(vec![]); // → FeedThrough
        assert_eq!(
            chan.process_live_data(vec![], false),
            BufferResult::Data(None)
        );
    }

    // Live data in Buffering mode with archive_done=false is buffered.
    #[test]
    fn test_process_live_data_buffering_archive_not_done_buffers_data() {
        let mut chan = DataChannel::new();
        let result =
            chan.process_live_data(vec![data_info(1.0), data_info(2.0)], false);
        assert_eq!(result, BufferResult::Data(None));
        // Data must be in the buffer.
        assert_eq!(
            chan.get_buffer(),
            Some(vec![data_info(1.0), data_info(2.0)])
        );
    }

    // Multiple live packets accumulate in the buffer.
    #[test]
    fn test_process_live_data_multiple_packets_accumulate() {
        let mut chan = DataChannel::new();
        chan.process_live_data(vec![data_info(1.0)], false);
        chan.process_live_data(vec![data_info(2.0)], false);
        chan.process_live_data(vec![data_info(3.0)], false);

        assert_eq!(
            chan.get_buffer(),
            Some(vec![data_info(1.0), data_info(2.0), data_info(3.0)])
        );
    }

    // Live data in Buffering mode with archive_done=true and an empty buffer
    // → returns the live data directly (no prepend), transitions to FeedThrough.
    #[test]
    fn test_process_live_data_archive_done_empty_buffer_returns_live() {
        let mut chan = DataChannel::new();
        let result = chan
            .process_live_data(vec![data_info(10.0), data_info(20.0)], true);
        assert_eq!(
            result,
            BufferResult::Data(Some(vec![data_info(10.0), data_info(20.0)]))
        );
        assert!(matches!(chan, DataChannel::FeedThrough));
    }

    // Live data in Buffering mode with archive_done=true and a non-empty buffer
    // → prepends the buffer to the live data, transitions to FeedThrough.
    #[test]
    fn test_process_live_data_archive_done_nonempty_buffer_prepends() {
        let mut chan = DataChannel::new();
        // Buffer two items first.
        chan.process_live_data(vec![data_info(1.0), data_info(2.0)], false);

        // Now archive is done; live data arrives.
        let result =
            chan.process_live_data(vec![data_info(3.0), data_info(4.0)], true);
        assert_eq!(
            result,
            BufferResult::Data(Some(vec![
                data_info(1.0),
                data_info(2.0),
                data_info(3.0),
                data_info(4.0),
            ]))
        );
        assert!(matches!(chan, DataChannel::FeedThrough));
    }

    // Live data in FeedThrough mode is passed through regardless of
    // archive_done.
    #[test]
    fn test_process_live_data_feedthrough_passes_through() {
        let mut chan = DataChannel::new();
        chan.process_archive_data(vec![]); // → FeedThrough

        let result =
            chan.process_live_data(vec![data_info(5.0), data_info(6.0)], false);
        assert_eq!(
            result,
            BufferResult::Data(Some(vec![data_info(5.0), data_info(6.0)]))
        );
    }

    // -----------------------------------------------------------------------
    // Buffer overflow tests

    // Exactly 1000 items in the buffer is still accepted (boundary).
    #[test]
    fn test_process_live_data_buffer_at_capacity_is_accepted() {
        let mut chan = DataChannel::new();
        // Fill the buffer to exactly 1000 items across two packets.
        let first: Vec<_> = (0..500).map(|i| data_info(i as f64)).collect();
        let second: Vec<_> = (500..1000).map(|i| data_info(i as f64)).collect();

        assert_eq!(
            chan.process_live_data(first, false),
            BufferResult::Data(None)
        );
        assert_eq!(
            chan.process_live_data(second, false),
            BufferResult::Data(None)
        );

        // Buffer must hold all 1000 items.
        let buf = chan.get_buffer().expect("buffer should not be empty");
        assert_eq!(buf.len(), 1000);
    }

    // Adding one more item beyond 1000 triggers Overflow and clears the buffer.
    #[test]
    fn test_process_live_data_overflow_clears_buffer_and_returns_overflow() {
        let mut chan = DataChannel::new();
        // Fill to 1000.
        let first: Vec<_> = (0..1000).map(|i| data_info(i as f64)).collect();
        chan.process_live_data(first, false);

        // One more item pushes it over the limit.
        let result = chan.process_live_data(vec![data_info(9999.0)], false);
        assert_eq!(result, BufferResult::Overflow);

        // Buffer must have been cleared.
        assert_eq!(chan.get_buffer(), None);
    }
}
