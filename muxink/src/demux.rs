//! Stream demultiplexing
//!
//! Demultiplexes a Stream of Bytes into multiple channels. Up to 256 channels are supported, and
//! if messages are present on a channel but there isn't an associated DemultiplexerHandle for that
//! channel, then the Stream will never poll as Ready.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{stream::Fuse, Stream, StreamExt};

/// A frame demultiplexer.
///
/// A demultiplexer is not used directly, but used to spawn demultiplexing handles.
///
/// TODO What if someone sends data to a channel for which there is no handle?
pub struct Demultiplexer<S> {
    stream: Fuse<S>,
    next_frame: Option<(u8, Bytes)>,
}

impl<S: Stream> Demultiplexer<S> {
    /// Creates a new demultiplexer with the given underlying stream.
    pub fn new(stream: S) -> Demultiplexer<S> {
        Demultiplexer {
            stream: stream.fuse(),
            next_frame: None,
        }
    }
}

impl<S> Demultiplexer<S> {
    /// Creates a handle listening for frames on the given channel.
    ///
    /// Any item on this channel sent to the `Stream` underlying the `Demultiplexer` we used to
    /// create this handle will be read only when all other messages for other channels have been
    /// read first. If one has handles on the same channel created via the same underlying
    /// `Demultiplexer`, each message on that channel will only be received by one of the handles.
    /// Unless this is desired behavior, this should be avoided.
    pub fn create_handle(demux: Arc<Mutex<Self>>, channel: u8) -> DemultiplexerHandle<S> {
        DemultiplexerHandle {
            channel,
            demux: demux.clone(),
        }
    }
}

/// A handle to a demultiplexer.
///
/// A handle is bound to a specific channel, see [`Demultiplexer::create_handle`] for details.
pub struct DemultiplexerHandle<S> {
    channel: u8,
    demux: Arc<Mutex<Demultiplexer<S>>>, // (probably?) make sure this is a stdmutex
}

impl<S> Stream for DemultiplexerHandle<S>
where
    S: Stream<Item = Bytes> + Unpin,
{
    // TODO Result<Bytes, Error>
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Lock the demultiplexer
        let mut demux = match self.demux.as_ref().try_lock() {
            Err(_err) => panic!("TODO"), // TODO return Err("TODO")
            Ok(guard) => guard,
        };

        // If next_frame has a suitable frame for this channel, return it in a Poll::Ready. If it has
        // an unsuitable frame, return Poll::Pending. Otherwise, we attempt to read from the stream.
        if let Some((ref channel, ref bytes)) = demux.next_frame {
            if *channel == self.channel {
                let bytes = bytes.clone();
                demux.next_frame = None;
                return Poll::Ready(Some(bytes));
            } else {
                return Poll::Pending;
            }
        }

        // Try to read from the stream, placing the frame into next_frame and returning
        // Poll::Pending if its in the wrong channel, otherwise returning it in a Poll::Ready.
        match demux.stream.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(bytes)) => {
                let channel: u8 = *&bytes[0..1][0];
                let frame = bytes.slice(1..).clone();
                if channel == self.channel {
                    Poll::Ready(Some(frame))
                } else {
                    demux.next_frame = Some((channel, frame));
                    Poll::Pending
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
        }

        // todo: figure out when we are being polled again, does it work correctly (see waker) or
        //       will it cause inefficient races? do we need to call wake? probably. (possibly
        //       necessary) can have table of wakers to only wake the right one.
    }
}

#[cfg(test)]
mod tests {
    use std::ops::DerefMut;

    use super::*;
    use futures::{FutureExt, Stream, StreamExt};
    use tokio_stream::iter;

    /* TODO
    struct TestStream<T> {
        items: Vec<T>,
        finished: bool,
    }

    impl<T> Stream for TestStream<T> {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.finished {
                panic!("polled a TestStream after completion");
            }
            if let Some(t) = self.items.pop() {
                return Poll::Ready(Some(t));
            } else {
                self.finished = true;
                return Poll::Ready(None);
            }
        }
    }
    */

    #[test]
    fn demultiplexing_two_channels() {
        let stream = iter(vec![
            Bytes::copy_from_slice(&[0, 1, 2, 3, 4]),
            Bytes::copy_from_slice(&[0, 4]),
            Bytes::copy_from_slice(&[1, 2]),
            Bytes::copy_from_slice(&[1, 5]),
        ]);
        let demux = Arc::new(Mutex::new(Demultiplexer::new(stream)));

        let mut zero_handle = Demultiplexer::create_handle(demux.clone(), 0);
        let mut one_handle = Demultiplexer::create_handle(demux.clone(), 1);

        assert_eq!(
            zero_handle
                .next()
                .now_or_never()
                .expect("not ready")
                .expect("stream ended")
                .as_ref(),
            &[1, 2, 3, 4]
        );

        assert!(one_handle.next().now_or_never().is_none());

        assert!(one_handle.next().now_or_never().is_none());

        assert_eq!(
            zero_handle
                .next()
                .now_or_never()
                .expect("not ready")
                .expect("stream ended")
                .as_ref(),
            &[4]
        );

        assert_eq!(
            one_handle
                .next()
                .now_or_never()
                .expect("not ready")
                .expect("stream ended")
                .as_ref(),
            &[2]
        );

        assert_eq!(
            one_handle
                .next()
                .now_or_never()
                .expect("not ready")
                .expect("stream ended")
                .as_ref(),
            &[5]
        );

        assert!(one_handle.next().now_or_never().unwrap().is_none());
        assert!(zero_handle.next().now_or_never().unwrap().is_none());
    }
}
