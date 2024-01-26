use crate::IO_BUFFER_SIZE;
use std::io;
use std::io::{BufReader, Read};

/// Reads chunks of a provided size from an `io::Read`.
///
/// The chunk size needs to fit in memory.
pub struct ChunksReader<R> {
    reader: BufReader<R>,
    chunk_size: usize,
}

impl<R> ChunksReader<R>
where
    R: Read,
{
    pub fn new(reader: R, chunk_size: usize) -> Self {
        Self {
            reader: BufReader::with_capacity(IO_BUFFER_SIZE, reader),
            chunk_size,
        }
    }

    /// Reads the next chunk from the reader, or `None`
    /// if no chunks remain.
    ///
    /// The final chunk may have a size that is less than the chunk size.
    pub fn read_chunk(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut chunk = vec![0u8; self.chunk_size];
        let mut bytes_read = 0;
        loop {
            let num_bytes = self.reader.read(&mut chunk[bytes_read..])?;
            bytes_read += num_bytes;

            if num_bytes == 0 {
                return if bytes_read == 0 {
                    // No bytes in this chunk
                    Ok(None)
                } else {
                    chunk.truncate(bytes_read);
                    Ok(Some(chunk))
                };
            }

            if num_bytes == self.chunk_size {
                return Ok(Some(chunk));
            }
        }
    }
}
