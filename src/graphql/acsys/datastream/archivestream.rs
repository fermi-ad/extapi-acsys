use super::{global, DataStream};
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

pub struct ArchiveStream {
    archived: DataStream,
    done: bool,
}

impl ArchiveStream {
    pub fn new(archived: DataStream) -> Self {
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
    type Item = global::DataReply;

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
