use crate::{
    get_file_name,
    medium::{
        block_file_name, version_file_name, version_timestamp_from_file_name, Medium, BLOCKS_DIR,
        VERSIONS_DIR,
    },
    model::BlockId,
    pipe, IO_BUFFER_SIZE,
};
use anyhow::bail;
use chrono::{DateTime, Utc};
use std::{
    cmp, io,
    io::{Read, Write},
    path::{Path, PathBuf},
    thread,
};

/// A medium that stores backups on the local filesystem.
pub struct LocalMedium {
    dir: PathBuf,
}

impl LocalMedium {
    pub fn new(backups_dir: impl AsRef<Path>, backup_name: &str) -> anyhow::Result<Self> {
        create_dir_if_not_exists(backups_dir.as_ref())?;
        let dir = backups_dir.as_ref().join(backup_name);
        create_dir_if_not_exists(&dir)?;
        create_dir_if_not_exists(dir.join(VERSIONS_DIR))?;
        create_dir_if_not_exists(dir.join(BLOCKS_DIR))?;
        Ok(Self { dir })
    }
}

impl Medium for LocalMedium {
    fn load_version(&self, n: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let mut versions = Vec::new();
        for entry in fs::read_dir(self.dir.join(VERSIONS_DIR))? {
            let entry = entry?;
            let version_timestamp =
                version_timestamp_from_file_name(&get_file_name(&entry.path())?)?;
            versions.push((version_timestamp, entry.path()));
        }

        versions.sort_by_key(|(date, _)| cmp::Reverse(*date));

        if n as usize >= versions.len() {
            return Ok(None);
        }

        let path = versions[n as usize].1.clone();

        let version = fs::read(path)?;
        Ok(Some(version))
    }

    fn save_version(&self, version: Vec<u8>, timestamp: DateTime<Utc>) -> anyhow::Result<()> {
        let path = self
            .dir
            .join(VERSIONS_DIR)
            .join(version_file_name(timestamp));
        fs::write(path, version)?;
        Ok(())
    }

    fn load_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Read + Send>> {
        let path = self.dir.join(BLOCKS_DIR).join(block_file_name(block_id));
        let (mut writer, reader) = pipe::new(IO_BUFFER_SIZE);
        thread::spawn(move || {
            if let Err(e) = stream_from_file(&mut writer, &path) {
                writer.disconnect_with_error(e);
            }
        });
        Ok(Box::new(reader))
    }

    fn save_block(&self, block_id: BlockId) -> anyhow::Result<Box<dyn Write + Send>> {
        let path = self.dir.join(BLOCKS_DIR).join(block_file_name(block_id));
        let (writer, mut reader) = pipe::new(IO_BUFFER_SIZE);
        thread::spawn(move || {
            if let Err(e) = stream_to_file(&mut reader, &path) {
                reader.disconnect_with_error(e);
            }
        });
        Ok(Box::new(writer))
    }

    fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn stream_from_file(writer: &mut pipe::Writer, path: &Path) -> io::Result<()> {
    let file = fs::File::open(path)?;
    writer.copy_all_from_reader(file).map(|_| ())
}

fn stream_to_file(reader: &mut pipe::Reader, path: &Path) -> io::Result<()> {
    let file = fs::File::create(path)?;
    reader.copy_all_to_writer(file)?;
    Ok(())
}

fn create_dir_if_not_exists(path: impl AsRef<Path>) -> anyhow::Result<()> {
    match fs::create_dir(path.as_ref()) {
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            if fs::metadata(path)?.is_dir() {
                Ok(())
            } else {
                bail!("could not create directory as a non-directory file already exists here")
            }
        }
        Err(e) => Err(e.into()),
        Ok(_) => Ok(()),
    }
}
