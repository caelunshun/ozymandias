//! Provides an IO pipe for shuffling bytes between two threads.
//!
//! The implementation is backed by a wait-free SPSC ring buffer.
//!
//! Pipes support both asynchronous and synchronous operation.

use anyhow::anyhow;
use diatomic_waker::{WakeSink, WakeSource};
use rtrb::{Consumer, Producer, RingBuffer};
use std::{
    future, io,
    io::{Cursor, Error, IoSlice, Read, Write},
    mem,
    pin::{pin, Pin},
    sync::{Arc, Mutex},
    task::{ready, Context, Poll},
};
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

    /// This method allows putting data directly into the pipe
    /// from a `Read` instance. This avoids the need for an intermediate
    /// buffer and copy (as used by `io::copy`) when moving data into the pipe.
    ///
    /// Returns the number of bytes copied from the reader, which is 0 if an end
    /// of stream was encountered.
    #[allow(unused)]
    pub fn copy_from_reader(&mut self, reader: impl Read + Unpin) -> io::Result<usize> {
        pollster::block_on(self.async_copy_from_reader(NaiveAsyncReadAdapter::new(reader)))
    }

    /// Like `copy_from_reader`, but keeps copying until either an EOF
    /// is reached or an error occurs.
    ///
    /// Returns the total number of bytes written to the pipe.
    ///
    /// Returns `Ok` if the end of stream in `reader` is reached.
    /// Returns `Err` if the reader returns an error, or if an error
    /// is propagated from the other side of the pipe.
    pub fn copy_all_from_reader(&mut self, reader: impl Read + Unpin) -> io::Result<u64> {
        pollster::block_on(self.async_copy_all_from_reader(NaiveAsyncReadAdapter::new(reader)))
    }

    /// Async version of `copy_from_reader`.
    pub async fn async_copy_from_reader(&mut self, reader: impl AsyncRead) -> io::Result<usize> {
        let mut reader = pin!(reader);

        future::poll_fn(|cx| self.poll_copy_from_reader(cx, reader.as_mut())).await
    }

    /// Async version of `copy_all_from_reader`.
    pub async fn async_copy_all_from_reader(&mut self, reader: impl AsyncRead) -> io::Result<u64> {
        let mut reader = pin!(reader);
        let mut bytes_written = 0;
        loop {
            match self.async_copy_from_reader(reader.as_mut()).await {
                Ok(0) => return Ok(bytes_written), // EOF
                Ok(n) => bytes_written += u64::try_from(n).unwrap(),
                Err(e) => return Err(e),
            }
        }
    }

    fn poll_copy_from_reader<R: AsyncRead>(
        &mut self,
        cx: &mut Context,
        reader: Pin<&mut R>,
    ) -> Poll<io::Result<usize>> {
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
            .write_chunk_uninit(num_bytes)
            .expect("num_bytes available for writing");
        let (chunk_a, _) = chunk.as_mut_slices();

        let mut buf = ReadBuf::uninit(chunk_a);
        ready!(reader.poll_read(cx, &mut buf))?;
        let bytes_written = buf.filled().len();
        unsafe {
            // SOUNDNESS: `ReadBuf` guarantees that all
            // bytes in the filled portion of the buffer are initialized.
            // Since bytes_written == filled.len(), all bytes
            // being committed are initialized.
            chunk.commit(bytes_written);
        }

        self.writer_progress.notify();
        Poll::Ready(Ok(bytes_written))
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        pollster::block_on(<Self as AsyncWriteExt>::write(self, buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        pollster::block_on(<Self as AsyncWriteExt>::flush(self))
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        self.poll_copy_from_reader(cx, Pin::new(&mut buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let waker = cx.waker();
        self.reader_progress.register(waker);
        self.reader_drop.register(waker);

        let num_bytes_in_buffer = self.producer.buffer().capacity() - self.producer.slots();
        if num_bytes_in_buffer == 0 {
            return Poll::Ready(Ok(()));
        }

        if self.producer.is_abandoned() {
            let error = self.reader_error.lock().unwrap().take().unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    anyhow!("flush() called, but reader disconnected"),
                )
            });
            return Poll::Ready(Err(error));
        }

        Poll::Pending
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

    /// This method allows writing data directly from the pipe's
    /// internal buffer to a provided `Write` instance.
    /// Compared to `io::copy`, this avoids the use of an intermediate
    /// buffer and copy.
    ///
    /// Returns the number of bytes written to the writer.
    /// Use `copy_all_to_writer` to copy all bytes from the pipe
    /// until the stream becomes empty.
    #[allow(unused)]
    pub fn copy_to_writer<W: Write + Unpin>(&mut self, writer: W) -> io::Result<usize> {
        pollster::block_on(self.async_copy_to_writer(NaiveAsyncWriteAdapter::new(writer)))
    }

    /// Copies data from the pipe to the provided writer
    /// until the reader disconnects or an error is encountered
    /// on either side of the pipe.
    ///
    /// Returns the total number of bytes read from the pipe.
    pub fn copy_all_to_writer<W: Write + Unpin>(&mut self, writer: W) -> io::Result<u64> {
        pollster::block_on(self.async_copy_all_to_writer(NaiveAsyncWriteAdapter::new(writer)))
    }

    /// Async version of `copy_to_writer`.
    pub async fn async_copy_to_writer<W: AsyncWrite>(&mut self, writer: W) -> io::Result<usize> {
        let mut writer = pin!(writer);
        let fut = future::poll_fn(|cx| self.poll_copy_to_writer(cx, writer.as_mut()));
        fut.await
    }

    ///  Async version of `copy_all_to_writer`.
    pub async fn async_copy_all_to_writer<W: AsyncWrite>(&mut self, writer: W) -> io::Result<u64> {
        let mut writer = pin!(writer);
        let mut bytes_read = 0;
        loop {
            match self.async_copy_to_writer(writer.as_mut()).await {
                Ok(0) => return Ok(bytes_read),
                Ok(n) => bytes_read += u64::try_from(n).unwrap(),
                Err(e) => return Err(e),
            }
        }
    }

    fn poll_copy_to_writer<W: AsyncWrite>(
        &mut self,
        cx: &mut Context,
        writer: Pin<&mut W>,
    ) -> Poll<io::Result<usize>> {
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
                    // EOF, zero bytes read
                    Poll::Ready(Ok(0))
                }
            } else {
                Poll::Pending
            };
        }

        let num_bytes = consumer.slots();
        let chunk = consumer.read_chunk(num_bytes).expect("num_bytes available");
        let (chunk_a, chunk_b) = chunk.as_slices();

        let bytes_consumed = ready!(
            writer.poll_write_vectored(cx, &[IoSlice::new(chunk_a), IoSlice::new(chunk_b)])
        )?;

        chunk.commit(bytes_consumed);
        self.reader_progress.notify();

        Poll::Ready(Ok(bytes_consumed))
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
        let old_filled = buf.filled().len();
        let writer = Cursor::new(buf.initialize_unfilled());
        let writer = pin!(writer);
        let bytes_read = ready!(self.poll_copy_to_writer(cx, writer))?;
        buf.set_filled(old_filled + bytes_read);
        Poll::Ready(Ok(()))
    }
}

/// Wrapper over a `Read` instance that naively implements `AsyncRead`.
/// This should never be used inside an actual async runtime
/// as it will block the runtime thread on IO operations.
///
/// We use this in the blocking (non-async) pipe methods
/// to adapt a `Read` to an `AsyncRead` so that the async implementation
/// can be reused for sync operations.
struct NaiveAsyncReadAdapter<R> {
    reader: R,
}

impl<R> NaiveAsyncReadAdapter<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R> AsyncRead for NaiveAsyncReadAdapter<R>
where
    R: Read + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let old_filled = buf.filled().len();
        let num_bytes = self.reader.read(buf.initialize_unfilled())?;
        buf.set_filled(old_filled + num_bytes);
        Poll::Ready(Ok(()))
    }
}

/// Analagous to `NaiveAsyncReadAdapter`.
struct NaiveAsyncWriteAdapter<W> {
    writer: W,
}

impl<W> NaiveAsyncWriteAdapter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W> AsyncWrite for NaiveAsyncWriteAdapter<W>
where
    W: Write + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Poll::Ready(self.writer.write(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(self.writer.flush())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(self.writer.flush())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use std::{
        io,
        io::{Read, Write},
        thread,
        time::Duration,
    };

    #[test]
    fn blocking() {
        let (mut writer, mut reader) = super::new(256);
        let (finished_tx, finished) = flume::bounded(1);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            writer.write_all(&[0u8; 2048]).unwrap();
            writer.flush().unwrap();
            finished_tx.send(()).unwrap();
        });

        let mut buffer = [0u8; 2048];
        reader.read_exact(&mut buffer).unwrap();

        assert_eq!(reader.read(&mut buffer).unwrap(), 0);

        finished.recv().unwrap();
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
