//! Implements the backup storage index format.
//!
//! # Format
//! Backups consist of two components:
//! 1. The index, which contains:
//!     * Backup revisions keyed by timestamp.
//!     * File node data (see below).
//!    Currently, the entire index is stored in a single file, which must be overwritten
//!    (atomically) after each backup.
//! 2. The data block, which stores the binary contents of each file node.
//!    Data is fragmented into many files, each of which is named by a random ID.
//!    Several small files' contents can be stored in one data file, and very large
//!    files may be chunked into many smaller data files.
//!
//! ## File nodes
//! A file node consists of a BLAKE3 hash and a pointer to the data block location.
//! Note that as file paths are not a part of this hash,
//! multiple identical backed-up files can be mapped to the same file node,
//! which is particularly important in reducing storage size
//! when multiple versions of a backup are made.
//!
//! ## File trees
//! A file tree encodes the complete directory structure of a backed-up dataset.
//! It is effectively a map from file path to file node pointer (i.e. hash).
//!
//! ## Revisions
//! A backup revision consists of:
//! 1. The timestamp when the backup was initiated.
//! 2. The file tree at this point in time.
//!
//! # Durability and consistency concerns
//! The storage implementation must ensure the following properties:
//! 1. If the process or system is killed in the middle of writing to the backup storage,
//!    all of the data that has previously been backed up should remain readable without
//!    requiring any manual repair.
//! 2. Concurrent mutation to a backup either causes an error or writes the most
//!    recent backup, but does not corrupt the data in any way.
//!
//! In order to satisfy the first requirement, the backup implementation first writes
//! all data blocks for a backup, only committing the file tree after all data blocks
//! have been durably persisted. As an optimization, we do periodically commit completed file nodes
//! to the index file so that the backup can be resumed without re-uploading data blocks.
//! This optimization cannot, however, cause corruption.
//!
//! The second requirement must be satisfied by the underlying `Medium`. Read
//! the documentation for the `medium` module for the current status.

use crate::path::SimplifiedPath;
use anyhow::{anyhow, bail};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::io::Read;
use time::OffsetDateTime;

const MAGIC_STRING: &[u8] = b"OZYMANDIAS";

/// Current version of the index file format. Must be incremented
/// whenever the index format is changed in any way.
///
/// Note that for any types in this module that derive `Serialize` or `Deserialize`,
/// renaming a field is a breaking change, as field names are encoded by CBOR.
const CURRENT_STORAGE_FORMAT_VERSION: u32 = 0;

/// Randomly-generated ID of a block.
///
/// Currently 32 bytes in size, giving us a robust
/// 128 bits of collision resistance.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataBlockId([u8; Self::SIZE]);

impl DataBlockId {
    const SIZE: usize = 32;

    pub fn new_random() -> Self {
        Self(rand::random())
    }

    pub fn as_bytes(&self) -> &[u8; Self::SIZE] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.as_bytes())
    }
}

/// Identifier for a file node, represented as the 256-bit
/// BLAKE3 hash of the file contents (excluding metadata, path contents, etc.)
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileNodeId([u8; blake3::OUT_LEN]);

impl FileNodeId {
    pub fn from_hash(hash: blake3::Hash) -> Self {
        Self(*hash.as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; blake3::OUT_LEN] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.as_bytes())
    }
}

/// Representation of an index file.
///
/// # Encoding
/// An index file begins with a header:
/// * The magic byte sequence "OZYMANDIAS".
/// * Format version, encoded as a 32-bit little-endian unsigned integer.
/// * The `CompressionFormat`, encoded as a single byte.
///
/// The remaining bytes contain the optionally-compressed, CBOR-encoded
/// `IndexData`.
#[derive(Debug, Clone)]
pub struct Index {
    pub version: u32,
    pub data: IndexData,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            version: CURRENT_STORAGE_FORMAT_VERSION,
            data: IndexData::default(),
        }
    }
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes an index from binary data.
    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        let mut cursor = Cursor::new(data);

        let mut magic = [0u8; MAGIC_STRING.len()];
        cursor.read_exact(&mut magic)?;
        if magic != MAGIC_STRING {
            bail!("did not find magic string at start of index");
        }

        let mut version = [0u8; 4];
        cursor.read_exact(&mut version)?;
        let version = u32::from_le_bytes(version);
        if version != CURRENT_STORAGE_FORMAT_VERSION {
            // TODO support upgrading
            bail!("found mismatched version: this binary supports {CURRENT_STORAGE_FORMAT_VERSION} but found {version}");
        }

        let mut compression_format = [0u8; 1];
        cursor.read_exact(&mut compression_format)?;
        let compression_format = CompressionFormat::from_u8(compression_format[0])?;

        let data: IndexData = match compression_format {
            CompressionFormat::None => ciborium::from_reader(cursor)?,
            CompressionFormat::Zstd => {
                let decoder = zstd::Decoder::new(cursor)?;
                ciborium::from_reader(decoder)?
            }
        };

        Ok(Self { version, data })
    }

    /// Encodes the index to binary data.
    pub fn serialize(&self, compression_format: CompressionFormat) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(MAGIC_STRING);
        buffer.extend(self.version.to_le_bytes());
        buffer.push(compression_format as u8);

        match compression_format {
            CompressionFormat::None => {
                ciborium::into_writer(&self.data, &mut buffer).expect("serialization failed");
            }
            CompressionFormat::Zstd => {
                let mut writer = zstd::Encoder::new(buffer, zstd::DEFAULT_COMPRESSION_LEVEL)
                    .expect("failed to initialize zstd");
                ciborium::into_writer(&self.data, &mut writer)
                    .expect("serialization/compression failed");
                buffer = writer.finish().expect("compression failed");
            }
        }

        buffer
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum CompressionFormat {
    None = 0,
    Zstd = 1,
}

impl CompressionFormat {
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(x: u8) -> anyhow::Result<Self> {
        match x {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            _ => Err(anyhow!("unsupported compression type {x}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexData {
    pub revisions: Vec<Revision>,
    pub file_nodes: IndexMap<FileNodeId, FileNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Revision {
    pub timestamp: OffsetDateTime,
    pub file_tree: BTreeMap<SimplifiedPath, FileInstance>,
}

/// An instance of a file in a tree. Currently
/// contains only a pointer to the `FileNode`
/// that contains the file's contents.
///
/// In the future, this struct may be extended to store
/// file metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInstance {
    pub node: FileNodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNode {
    /// Data blocks containing each chunk of the file.
    /// Reading from these blocks in sequence and concatenating
    /// the bytes together yields the file contents.
    pub chunks: Vec<FileDataChunkLocation>,
}

impl FileNode {
    /// Returns the total byte size of the file contents.
    pub fn size(&self) -> u64 {
        self.chunks.iter().map(|c| c.size).sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDataChunkLocation {
    /// Block containing the chunk.
    pub block: DataBlockId,
    /// Byte offset from the start of the block.
    pub offset: u64,
    /// Number of bytes in this chunk.
    pub size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(CompressionFormat::None)]
    #[case(CompressionFormat::Zstd)]
    fn index_roundtrip_encoding(#[case] compression: CompressionFormat) {
        let index = Index::new();
        let bytes = index.serialize(compression);
        Index::deserialize(&bytes).unwrap();
    }

    #[test]
    fn index_future_version_returns_err() {
        let index = Index {
            version: CURRENT_STORAGE_FORMAT_VERSION + 1,
            ..Index::default()
        };
        let bytes = index.serialize(CompressionFormat::None);
        Index::deserialize(&bytes).unwrap_err();
    }
}
