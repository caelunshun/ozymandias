//! Defines the serialization format for backups.

use crate::Permissions;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use uuid::Uuid;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
}

/// Stores the filesystem tree in a particular backup version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tree {
    root_children: Vec<DirectoryEntry>,
}

impl Tree {
    pub fn new(root_children: Vec<DirectoryEntry>) -> Self {
        Self { root_children }
    }

    pub fn root_children(&self) -> impl Iterator<Item = &DirectoryEntry> {
        self.root_children.iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TreeEntry {
    File(FileEntry),
    Directory(DirectoryEntry),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub name: String,
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
    pub byte_offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub permissions: Permissions,
    pub children: Vec<TreeEntry>,
}