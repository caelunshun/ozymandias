use crate::medium::Medium;
use crate::model::BlockId;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use std::io;
use std::io::{Cursor, Read, Write};

/// A `Medium` composition that adds compression.
///
/// This adds a small header (to indicate the compression type)
/// to all saved versions and blocks.
///
/// All new data saved to the medium is compressed
/// with the provided compression algorithm.
/// Loaded data is decompressed according to the stored header.
pub struct CompressingMedium<M> {
    inner: M,
    compression_type: CompressionType,
}

impl<M> CompressingMedium<M> {
    pub fn new(inner: M, compression_type: CompressionType) -> Self {
        Self {
            inner,
            compression_type,
        }
    }
}

impl<M> Medium for CompressingMedium<M>
where
    M: Medium,
{
    fn load_version(&self, n: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let inner_version = self.inner.load_version(n)?;
        let inner_version = match inner_version {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut reader = Cursor::new(inner_version);
        let compression_type = decode_header(&mut reader)?;
        let mut reader = compression_type.wrap_reader(reader)?;

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(Some(buf))
    }

    fn save_version(&self, version_bytes: Vec<u8>, timestamp: DateTime<Utc>) -> anyhow::Result<()> {
        let mut encoded_buf = Vec::new();
        encode_header(self.compression_type, &mut encoded_buf)?;

        {
            let mut writer = self.compression_type.wrap_writer(&mut encoded_buf)?;
            writer.write_all(&version_bytes)?;
        }

        self.inner.save_version(encoded_buf, timestamp)
    }

    fn load_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Read + Send>> {
        let mut reader = self.inner.load_block(block_id)?;
        let compression_type = decode_header(&mut reader)?;
        compression_type.wrap_reader(reader)
    }

    fn save_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Write + Send>> {
        let mut writer = self.inner.save_block(block_id)?;
        encode_header(self.compression_type, &mut writer)?;
        self.compression_type.wrap_writer(writer)
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum CompressionType {
    None,
    Zstd { compression_level: i32 },
}

impl CompressionType {
    fn wrap_reader<'a>(
        self,
        reader: impl Read + Send + 'a,
    ) -> anyhow::Result<Box<dyn Read + Send + 'a>> {
        match self {
            CompressionType::None => Ok(Box::new(reader)),
            CompressionType::Zstd { .. } => Ok(Box::new(zstd::Decoder::new(reader)?)),
        }
    }

    fn wrap_writer<'a>(
        self,
        writer: impl Write + Send + 'a,
    ) -> anyhow::Result<Box<dyn Write + Send + 'a>> {
        match self {
            CompressionType::None => Ok(Box::new(writer)),
            CompressionType::Zstd { compression_level } => {
                let encoder = zstd::Encoder::new(writer, compression_level)?.auto_finish();
                Ok(Box::new(encoder))
            }
        }
    }

    fn id(self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Zstd { .. } => 1,
        }
    }

    fn from_id(id: u8) -> anyhow::Result<Self> {
        match id {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Zstd {
                // note: compression level is not used for decoding,
                // so value does not matter
                compression_level: zstd::DEFAULT_COMPRESSION_LEVEL,
            }),
            _ => Err(anyhow!("invalid compression type: {id}")),
        }
    }
}

fn encode_header(compression_type: CompressionType, mut writer: impl Write) -> io::Result<()> {
    writer.write_all(&[compression_type.id()])?;
    Ok(())
}

fn decode_header(mut reader: impl Read) -> anyhow::Result<CompressionType> {
    let mut buf = [0];
    reader.read_exact(&mut buf)?;
    CompressionType::from_id(buf[0])
}
