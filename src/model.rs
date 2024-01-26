//! Defines the serialization format for backups.

use crate::Permissions;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use uuid::Uuid;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlockId(Uuid);

impl BlockId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.hyphenated())
    }
}

/// Hash of a file chunk.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkHash(blake3::Hash);

impl ChunkHash {
    pub fn from_blake3(hash: blake3::Hash) -> Self {
        Self(hash)
    }
}

/// Metadata for a backup version. A backup version
/// specifies the full state of the backup filesystem tree
/// at a point in time.
///
/// Data is not stored in the metadata but rather as pointers to blocks
/// (in the form of `BlockId`s).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    timestamp: DateTime<Utc>,
    tree: Tree,
}

impl Version {
    pub fn new(tree: Tree) -> Self {
        Self {
            timestamp: Utc::now(),
            tree,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn tree(&self) -> &Tree {
        &self.tree
    }
}

/// Stores the filesystem tree in a particular backup version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tree {
    root_children: Vec<TreeEntry>,
}

impl Tree {
    pub fn new(root_children: Vec<TreeEntry>) -> Self {
        Self { root_children }
    }

    pub fn root_children(&self) -> impl Iterator<Item = &TreeEntry> {
        self.root_children.iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TreeEntry {
    File(FileEntry),
    Directory(DirectoryEntry),
}

impl TreeEntry {
    pub fn name(&self) -> &str {
        match self {
            TreeEntry::File(f) => &f.name,
            TreeEntry::Directory(d) => &d.name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub name: String,
    /// All data chunks in the file in the order they appear.
    pub chunks: Vec<ChunkMetadata>,
    pub permissions: Permissions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    pub hash: ChunkHash,
    pub location: ChunkLocation,
    pub uncompressed_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkLocation {
    pub block: BlockId,
    /// Byte offset within the (uncompressed) chunk.
    pub uncompressed_byte_offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub permissions: Permissions,
    pub children: Vec<TreeEntry>,
}
