use crate::medium::{
    block_file_name, version_file_name, version_timestamp_from_file_name, Medium, BLOCKS_DIR,
    VERSIONS_DIR,
};
use crate::model::{BlockId, Version};
use crate::{get_file_name, pipe, IO_BUFFER_SIZE};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::{io, thread};

/// A medium that stores backups on the local filesystem.
pub struct LocalMedium {
    dir: PathBuf,
    compression_level: i32,
}

impl LocalMedium {
    pub fn new(backups_dir: impl AsRef<Path>, backup_name: &str, compression_level: i32) -> Self {
        Self {
            dir: backups_dir.as_ref().join(backup_name),
            compression_level,
        }
    }
}

impl Medium for LocalMedium {
    fn load_version(&self, n: u64) -> anyhow::Result<Version> {
        let mut versions = Vec::new();
        for entry in fs::read_dir(&self.dir.join(VERSIONS_DIR))? {
            let entry = entry?;
            let version_timestamp =
                version_timestamp_from_file_name(&get_file_name(&entry.path())?)?;
            versions.push((version_timestamp, entry.path()));
        }

        // Note: path will be used to resolve ties in the event of duplicate timestamps (unlikely...)
        versions.sort();

        let path = versions[(n as usize).min(versions.len() - 1)].1.clone();

        let version = ciborium::from_reader(zstd::Decoder::new(fs::File::open(path)?)?)?;
        Ok(version)
    }

    fn save_version(&self, version: Version) -> anyhow::Result<()> {
        let path = self
            .dir
            .join(VERSIONS_DIR)
            .join(version_file_name(&version));
        ciborium::into_writer(
            &version,
            zstd::Encoder::new(fs::File::create(path)?, self.compression_level)?,
        )?;
        Ok(())
    }

    fn load_block(&self, block_id: BlockId) -> Box<dyn Read + Send> {
        let path = self.dir.join(BLOCKS_DIR).join(block_file_name(block_id));
        let (mut writer, reader) = pipe::new(IO_BUFFER_SIZE);
        thread::spawn(move || {
            if let Err(e) = stream_from_file(&mut writer, &path) {
                writer.disconnect_with_error(e);
            }
        });
        Box::new(reader)
    }

    fn save_block(&self, block_id: BlockId) -> Box<dyn Write + Send> {
        let path = self.dir.join(BLOCKS_DIR).join(block_file_name(block_id));
        let (writer, mut reader) = pipe::new(IO_BUFFER_SIZE);
        thread::spawn(move || {
            if let Err(e) = stream_to_file(&mut reader, &path) {
                reader.disconnect_with_error(e);
            }
        });
        Box::new(writer)
    }
}

fn stream_from_file(writer: &mut pipe::Writer, path: &Path) -> io::Result<()> {
    let mut file = fs::File::open(path)?;
    io::copy(&mut file, writer)?;
    Ok(())
}

fn stream_to_file(reader: &mut pipe::Reader, path: &Path) -> io::Result<()> {
    let mut file = fs::File::create(path)?;
    io::copy(reader, &mut file)?;
    Ok(())
}
