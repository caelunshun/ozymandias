extern crate fs_err as fs;

use anyhow::{anyhow, Context};
use std::path::Path;

mod backup;
mod chunks_reader;
mod medium;
mod model;
mod pipe;
mod restore;

/// File permissions. `None` when files are sourced from non-Unix systems.
type Permissions = Option<u32>;

#[allow(non_upper_case_globals)]
const KiB: usize = 1024;
#[allow(non_upper_case_globals)]
const MiB: usize = 1024 * KiB;

/// General buffer size for filesystem IO operations.
const IO_BUFFER_SIZE: usize = 2 * MiB;

/// Size of a chunk in a file being backed up.
/// Files are split into chunks to support a limited degree
/// of diffing across multiple versions of a large file.
const CHUNK_SIZE: usize = 1 * MiB;

/// Maximum chunk size that will be accepted by the restore system.
/// This limit exists to avoid OOM situations.
const MAX_RESTORE_CHUNK_SIZE: usize = 256 * MiB;

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
        .context("missing file name")?
        .to_str()
        .ok_or_else(|| anyhow!("invalid UTF-8 in path: {}", path.display()))
        .map(str::to_owned)
}

#[cfg(unix)]
fn get_file_permissions(path: &Path) -> anyhow::Result<Permissions> {
    let metadata = fs::metadata(path)?;
    use std::os::unix::fs::MetadataExt;
    Ok(Some(metadata.mode()))
}

#[cfg(not(unix))]
fn get_file_permissions(_path: &Path) -> anyhow::Result<Permissions> {
    Ok(None)
}
