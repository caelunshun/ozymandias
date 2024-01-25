//! Provides an IO pipe for shuffling bytes between two threads.
//!
//! The implementation is backed by a wait-free SPSC ring buffer.
//!
//! Pipes support both asynchronous and synchronous operation.

use diatomic_waker::{WakeSink, WakeSource};
use rtrb::{Consumer, Producer, RingBuffer};
use std::io::{Error, Read, Write};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{io, mem};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

/// Constructs a pipe with the provided buffer size.
pub fn new(capacity: usize) -> (Writer, Reader) {
    let (producer, consumer) = RingBuffer::new(capacity);

    let reader_progress = WakeSink::new();
    let reader_drop = WakeSink::new();
    let writer_progress = WakeSink::new();
    let writer_drop = WakeSink::new();

    let reader_progress_source = reader_progress.source();
    let reader_drop_source = reader_drop.source();
    let writer_progress_source = writer_progress.source();
    let writer_drop_source = writer_drop.source();

    let reader_error = ErrorOption::default();
    let writer_error = ErrorOption::default();

    (
        Writer {
            producer,
            reader_progress,
            reader_drop,
            writer_progress: writer_progress_source,
            writer_drop: writer_drop_source,
            reader_error: Arc::clone(&reader_error),
            writer_error: Arc::clone(&writer_error),
        },
        Reader {
            consumer,
            writer_progress,
            reader_progress: reader_progress_source,
            writer_drop,
            reader_drop: reader_drop_source,
            reader_error,
            writer_error,
        },
    )
}

/// Synchronized storage for an error being propagated from one side of the pipe to the other.
type ErrorOption = Arc<Mutex<Option<io::Error>>>;

/// Producer side of a pipe.
pub struct Writer {
    producer: Producer<u8>,

    writer_progress: WakeSource,
    reader_progress: WakeSink,
    writer_drop: WakeSource,
    reader_drop: WakeSink,

    reader_error: ErrorOption,
    writer_error: ErrorOption,
}

impl Writer {
    /// Disconnects the writer with an error that will be propagated
    /// to the reader side.
    pub fn disconnect_with_error(self, error: io::Error) {
        *self.writer_error.lock().unwrap() = Some(error);
        drop(self);
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        pollster::block_on(<Self as AsyncWriteExt>::write(self, buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        // These registrations must take place before we check
        // the ring buffer; see comment in Reader::async_read.
        let waker = cx.waker();
        self.reader_progress.register(waker);
        self.reader_drop.register(waker);

        let producer = &mut self.producer;
        if producer.is_abandoned() {
            return if let Some(error) = self.reader_error.lock().unwrap().take() {
                Poll::Ready(Err(error))
            } else {
                // Indicate EOF: Ok(0)
                Poll::Ready(Ok(0))
            };
        }

        if producer.is_full() {
            // Wait for reader progress
            return Poll::Pending;
        }

        let num_bytes = producer.slots();
        let mut chunk = producer
            .write_chunk(num_bytes)
            .expect("num_bytes available for writing");
        let (chunk_a, chunk_b) = chunk.as_mut_slices();

        let mut bytes_written = 0;

        let bytes_to_write = chunk_a.len().min(buf.len());
        chunk_a[..bytes_to_write].copy_from_slice(&buf[..bytes_to_write]);
        bytes_written += bytes_to_write;
        let buf = &buf[bytes_to_write..];

        if bytes_to_write == chunk_a.len() {
            let bytes_to_write = chunk_b.len().min(buf.len());
            chunk_b.copy_from_slice(&buf[..bytes_to_write]);
            bytes_written += bytes_to_write;
        }

        chunk.commit(bytes_written);
        self.writer_progress.notify();
        Poll::Ready(Ok(bytes_written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // See comment in Reader::drop.
        let temp_producer = RingBuffer::new(0).0;
        let producer = mem::replace(&mut self.producer, temp_producer);
        drop(producer);

        self.writer_drop.notify();
    }
}

/// Consumer side of a pipe.
pub struct Reader {
    consumer: Consumer<u8>,

    writer_progress: WakeSink,
    reader_progress: WakeSource,
    writer_drop: WakeSink,
    reader_drop: WakeSource,

    writer_error: ErrorOption,
    reader_error: ErrorOption,
}

impl Reader {
    /// Disconnects the reader with an error that will be propagated to the writer side.
    pub fn disconnect_with_error(self, error: io::Error) {
        *self.reader_error.lock().unwrap() = Some(error);
        drop(self);
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        pollster::block_on(<Self as AsyncReadExt>::read(self, buf))
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        // `self.consumer` must be dropped _before_ notifying the writer
        // to avoid a race condition (see comment in AsyncRead impl).
        // This is a hack to replace the consumer with a "default" one.
        let temp_consumer = RingBuffer::new(0).1;
        let consumer = mem::replace(&mut self.consumer, temp_consumer);
        drop(consumer);

        self.reader_drop.notify();
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // These registrations must take place _before_ we check
        // the ring buffer. Otherwise, there is a race condition
        // where we check the buffer and find it is empty,
        // then the writer adds data before we call register(),
        // causing the reader not to receive any notification for the new data.
        let waker = cx.waker();
        self.writer_drop.register(waker);
        self.writer_progress.register(waker);

        let consumer = &mut self.consumer;

        if consumer.is_empty() {
            return if consumer.is_abandoned() {
                if let Some(error) = self.writer_error.lock().unwrap().take() {
                    Poll::Ready(Err(error))
                } else {
                    // EOF: return Ok(()) without writing any data to `buf`.
                    Poll::Ready(Ok(()))
                }
            } else {
                Poll::Pending
            };
        }

        let num_bytes = consumer.slots();
        let chunk = consumer.read_chunk(num_bytes).expect("num_bytes available");
        let (chunk_a, chunk_b) = chunk.as_slices();

        let mut bytes_consumed = 0;

        let bytes_to_consume = chunk_a.len().min(buf.remaining());
        buf.put_slice(&chunk_a[..bytes_to_consume]);
        bytes_consumed += bytes_to_consume;

        if bytes_to_consume == chunk_a.len() {
            let bytes_to_consume = chunk_b.len().min(buf.remaining());
            buf.put_slice(&chunk_b[..bytes_to_consume]);
            bytes_consumed += bytes_to_consume;
        }

        chunk.commit(bytes_consumed);
        self.reader_progress.notify();

        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use std::io::{Read, Write};
    use std::time::Duration;
    use std::{io, thread};

    #[test]
    fn blocking() {
        let (mut writer, mut reader) = super::new(256);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            writer.write_all(&[0u8; 2048]).unwrap();
        });

        let mut buffer = [0u8; 2048];
        reader.read_exact(&mut buffer).unwrap();

        assert_eq!(reader.read(&mut buffer).unwrap(), 0);
    }

    #[test]
    fn error_propagation() {
        let (writer, mut reader) = super::new(256);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            writer.disconnect_with_error(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                anyhow!(""),
            ));
        });

        let err = reader.read(&mut [0]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted);
    }

    #[test]
    fn immediate_disconnect() {
        let (writer, mut reader) = super::new(256);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            drop(writer);
        });
        let mut buf = [0u8; 256];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }
}
