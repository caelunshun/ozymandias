//! An efficient structure to stream byte streams across two threads.
//!
//! This is backed by an efficient SPSC ring buffer.

use anyhow::anyhow;
use event_listener::{Event, EventListener};
use rtrb::{Consumer, Producer, RingBuffer};
use std::io;
use std::io::{Cursor, Read, Write};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tokio::io::ReadBuf;

/// Creates a new pipe with the specified buffer capacity.
///
/// # Panics
/// Panics if `capacity == 0`.
pub fn new(capacity: usize) -> (Writer, Reader) {
    assert_ne!(capacity, 0);

    let (producer, consumer) = RingBuffer::new(capacity);
    let shared = Arc::new(Shared::default());

    (
        Writer {
            producer,
            shared: Arc::clone(&shared),
            bytes_written: 0,
        },
        Reader { consumer, shared },
    )
}

#[derive(Default, Debug)]
struct Shared {
    wake_reader: Event,
    wake_writer: Event,
    writer_error: Mutex<Option<io::Error>>,
}

/// The writer side of a pipe.
///
/// Data can be written using the `io::Write` implementation, or by calling
/// `copy_from` to read data directly from a `Read` instance into the underlying buffer.
///
/// If the underlying pipe is full, writing will block
/// until the reader has consumed some data and made space for more.
#[derive(Debug)]
pub struct Writer {
    producer: Producer<u8>,
    shared: Arc<Shared>,
    bytes_written: u64,
}

impl Writer {
    /// Returns the number of bytes that have been written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Consumes the given `Read` instance, writing all of its bytes
    /// to the pipe until one of the following conditions:
    /// * The reader returns `Ok(0)`, indicating an end of stream.
    /// * The reader returns `Err`. The error is propagated to the Reader side of the pipe,
    ///   and this function returns.
    /// * The reader side of the pipe disconnects.
    ///
    /// Returns the number of bytes written.
    ///
    /// This is equivalent to calling `std::io::copy(read, writer)`,
    /// but it is more efficient as it does not require an intermediate buffer;
    /// bytes are read directly into the pipe.
    pub fn copy_from<R: Read>(mut self, mut read: R) -> usize {
        let mut bytes_written = 0;
        loop {
            match self.copy_chunk_from(&mut read) {
                Ok(0) => return bytes_written,
                Ok(n) => {
                    bytes_written += n;
                }
                Err(e) => {
                    self.disconnect_with_error(e);
                    return bytes_written;
                }
            }
        }
    }

    /// Propagates an error to the reader side of the stream, disconnecting
    /// the pipe.
    pub fn disconnect_with_error(self, error: io::Error) {
        *self.shared.writer_error.lock().unwrap() = Some(error);
        self.shared.wake_reader.notify(1);
    }

    /// Reads some data from the given `Read` instance into the underlying
    /// buffer, making it available to the `Reader` side of the buffer.
    ///
    /// * If the buffer is full, this method blocks until the reader makes
    ///   space available.
    /// * If reading from `read` returns an error, we return the error.
    /// * If the `Reader` side of the pipe has been dropped, we return a `BrokenPipe` error.
    /// * Returns the number of bytes read into the buffer.
    fn copy_chunk_from<R: Read>(&mut self, mut read: R) -> io::Result<usize> {
        // To avoid race conditions, the listener must be initialized before we read
        // any other shared state.
        let mut listener = pin!(EventListener::new(&self.shared.wake_writer));
        listener.as_mut().listen();

        let mut available_slots = self.producer.slots();
        if available_slots == 0 {
            listener.wait();
            available_slots = self.producer.slots();
        }

        if self.producer.is_abandoned() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                anyhow!("reader disconnected"),
            ));
        }

        let mut chunk = self
            .producer
            .write_chunk(available_slots)
            .expect("available_slots");
        let (slice_a, slice_b) = chunk.as_mut_slices();
        let mut n = read.read(slice_a)?;
        if n == slice_a.len() {
            n += read.read(slice_b)?;
        }

        chunk.commit(n);
        self.bytes_written += n as u64;

        if n != 0 {
            self.shared.wake_reader.notify(1);
        }

        Ok(n)
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.copy_chunk_from(Cursor::new(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.shared.wake_reader.notify(1);
    }
}

/// The reader side of a pipe.
#[derive(Debug)]
pub struct Reader {
    consumer: Consumer<u8>,
    shared: Arc<Shared>,
}

impl Reader {
    /// Returns the number of available bytes for reading.
    pub fn available(&self) -> usize {
        self.consumer.slots()
    }

    /// Copies data from the pipe directly to the given `Write`
    /// instance, without using an intermediate buffer.
    ///
    /// This function is equivalent to calling `io::copy(reader, write)`
    /// but does not use an intermediate buffer.
    ///
    /// Returns the number of bytes written.
    pub fn copy_to<W: Write>(mut self, mut write: W) -> io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            match self.copy_chunk_to(&mut write) {
                Ok(0) => return Ok(bytes_read),
                Ok(n) => {
                    bytes_read += n;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn copy_chunk_to<W: Write>(&mut self, mut write: W) -> io::Result<usize> {
        let mut listener = pin!(EventListener::new(&self.shared.wake_reader));
        listener.as_mut().listen();

        let mut available_slots = self.consumer.slots();
        if available_slots == 0 {
            if self.consumer.is_abandoned() {
                return Ok(0);
            }

            listener.wait();
            if let Some(error) = self.shared.writer_error.lock().unwrap().take() {
                return Err(error);
            } else {
                available_slots = self.consumer.slots();
            }
        }

        let chunk = self
            .consumer
            .read_chunk(available_slots)
            .expect("slots available");
        let (slice_a, slice_b) = chunk.as_slices();

        let mut n = write.write(slice_a)?;
        if n == slice_a.len() {
            n += write.write(slice_b)?;
        }

        chunk.commit(n);
        if n != 0 {
            self.shared.wake_writer.notify(1);
        }

        Ok(n)
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.copy_chunk_to(Cursor::new(buf))
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.shared.wake_writer.notify(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn blocking() {
        let (mut writer, mut reader) = new(256);
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
        let (writer, mut reader) = new(256);
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
        let (writer, mut reader) = new(256);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            drop(writer);
        });
        let mut buf = [0u8; 256];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }
}
