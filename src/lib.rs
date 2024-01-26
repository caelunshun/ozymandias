extern crate fs_err as fs;

use anyhow::anyhow;
use std::path::Path;

mod chunks_reader;
mod medium;
mod model;
mod pipe;

/// File permissions. `None` when files are sourced from non-Unix systems.
type Permissions = Option<u32>;

const KiB: usize = 1024;
const MiB: usize = 1024 * KiB;

/// General buffer size for filesystem IO operations.
const IO_BUFFER_SIZE: usize = 2 * MiB;

/// Size of a chunk in a file being backed up.
/// Files are split into chunks to support a limited degree
/// of diffing across multiple versions of a large file.
const CHUNK_SIZE: usize = 1 * MiB;

/// Approximate maximum size of a block.
/// A block is a collection of file chunks from
/// one or more files.
///
/// (This is "approximate" because the actual size
/// is allowed to exceed this value by less
/// than the chunk size.)
const APPROX_MAX_BLOCK_SIZE: usize = 64 * MiB;

fn get_file_name(path: &Path) -> anyhow::Result<String> {
    path.file_name()
        .to_str()
        .ok_or_else(|| anyhow!("invalid UTF-8 in path: {}", path.display()))
        .map(str::to_owned)
}
