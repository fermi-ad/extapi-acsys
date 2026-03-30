// Defines a stream type called `DataChannel`. This stream takes two
// streams as input; one returns archive data and the other live data.
// While the archive stream returns data, any data from the live
// stream is queued up. Once the archived data stream is empty, then
// the live data is returned.

use super::global;
use tracing::warn;

// Implements the merge logic for a data channel. When the channel is
// in buffering mode, it adds any new live data to its buffer. In feed
// through mode, all live data is simply forwarded on.

pub enum DataChannel {
    Buffering(Vec<global::DataInfo>),
    FeedThrough,
}

impl DataChannel {
    // Creates a new data channel. Channels start in buffering mode with
    // an empty buffer.

    pub fn new() -> Self {
        DataChannel::Buffering(vec![])
    }

    // Returns the buffered data, if any.

    pub fn get_buffer(&mut self) -> Option<Vec<global::DataInfo>> {
        match self {
            Self::FeedThrough => None,
            Self::Buffering(data) => {
                if data.is_empty() {
                    None
                } else {
                    let mut tmp = vec![];

                    std::mem::swap(data, &mut tmp);
                    Some(tmp)
                }
            }
        }
    }

    // Processes a chunk of live data.

    pub fn process_live_data(
        &mut self, mut live_data: Vec<global::DataInfo>, archive_done: bool,
    ) -> Option<Vec<global::DataInfo>> {
        match self {
            // In feedthrough mode, we simply pass on the live data.
            Self::FeedThrough => Some(live_data),

            // If in buffering mode, we append the data and return
            // `None` so the caller knows there's nothing to do.
            Self::Buffering(data) => {
                data.append(&mut live_data);
                if archive_done {
                    let mut tmp = vec![];

                    std::mem::swap(data, &mut tmp);
                    *self = Self::FeedThrough;
                    Some(tmp)
                } else {
                    None
                }
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

#[cfg(test)]
mod test {
    use super::DataChannel;
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

        assert!(matches!(chan, DataChannel::Buffering(_)));

        // Run an archive packet through. The channel should return
        // it, as is.

        assert_eq!(
            chan.process_archive_data(vec![data_info(100.0)]),
            vec![data_info(100.0)]
        );

        // Add some live data to the channel. Since we're in buffer
        // mode, live data is saved and `None` should be returned.

        assert_eq!(
            chan.process_live_data(
                vec![data_info(200.0), data_info(210.0),],
                false
            ),
            None
        );

        // Add some more archived data. The array should still be
        // returned.

        assert_eq!(
            chan.process_archive_data(
                vec![data_info(110.0), data_info(120.0),]
            ),
            vec![data_info(110.0), data_info(120.0),]
        );

        // Send an empty archive packet. This signifies no more archive
        // data will be received. The channel should return the buffered
        // data and switch to feed-through mode.

        assert_eq!(
            chan.process_archive_data(vec![]),
            vec![data_info(200.0), data_info(210.0)]
        );

        // Now add live data. It should get passed through.

        assert_eq!(
            chan.process_live_data(
                vec![data_info(220.0), data_info(230.0)],
                false
            ),
            Some(vec![data_info(220.0), data_info(230.0)])
        );
    }
}
