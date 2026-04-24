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
}
