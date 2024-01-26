use crate::model::{BlockId, Version};
use anyhow::bail;
use chrono::{DateTime, Utc};
use std::io::{Read, Write};

pub mod local;

/// Recommended directory name for versions.
const VERSIONS_DIR: &str = "versions";

/// Recommended directory name for data blocks.
const BLOCKS_DIR: &str = "blocks";

/// Returns the file name that should be used to store the given version.
fn version_file_name(version: &Version) -> String {
    let random_bytes: [u8; 8] = rand::random();
    let timestamp_part = version
        .timestamp()
        .to_rfc3339()
        .replace(':', "_")
        .to_owned();
    let random_part = hex::encode(random_bytes);
    format!("{timestamp_part}.{random_part}")
}

fn version_timestamp_from_file_name(file_name: &str) -> anyhow::Result<DateTime<Utc>> {
    let parts: Vec<&str> = file_name.split('.').collect();
    if parts.len() != 2 {
        bail!("expected x.y format in version file name");
    }

    let timestamp = parts[0];
    DateTime::parse_from_rfc3339(timestamp)
        .map_err(anyhow::Error::from)
        .map(|ts| ts.to_utc())
}

/// Returns the file name that should be used to store the given data block.
fn block_file_name(block_id: BlockId) -> String {
    block_id.to_string()
}

/// An interface to the underlying storage medium for backups.
pub trait Medium: Send + Sync + 'static {
    /// Synchronously loads the `n`th *most recent* version.
    fn load_version(&self, n: u64) -> anyhow::Result<Option<Version>>;

    /// Synchronously saves a new version.
    fn save_version(&self, version: Version) -> anyhow::Result<()>;

    /// Asynchronously opens the data block with the given ID
    /// for reading.
    ///
    /// Errors should be indicated by the returned `Read` object.
    fn load_block(&self, block_id: BlockId) -> Box<dyn Read + Send>;

    /// Asynchronously saves a new data block.
    ///
    /// Data to be written to the new block should be written
    /// to the returned writer.
    fn save_block(&self, block_id: BlockId) -> Box<dyn Write + Send>;
}
