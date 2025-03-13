//! This module implements the creation of backups from a local filesystem tree.
//!
//! The current approach is single-threaded and does not utilize IO parallelism,
//! which could be a beneficial improvement for backups of many small files.

use crate::{
    chunks_reader::ChunksReader,
    get_file_name, get_file_permissions,
    medium::Medium,
    model::{
        BlockId, ChunkHash, ChunkLocation, ChunkMetadata, DirectoryEntry, FileEntry, Tree,
        TreeEntry, Version,
    },
    APPROX_MAX_BLOCK_SIZE, CHUNK_SIZE,
};
use anyhow::anyhow;
use indicatif::{HumanBytes, ProgressBar};
use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};

pub struct Config<'a> {
    pub source_dir: PathBuf,
    pub medium: &'a dyn Medium,
}

pub fn run(config: Config) -> anyhow::Result<()> {
    let mut driver = Driver::new(config)?;
    let root_entry = driver.back_up_dir(&driver.config.source_dir.clone())?;
    driver.finish(root_entry)?;
    Ok(())
}

struct Driver<'a> {
    config: Config<'a>,
    current_block: Option<CurrentBlock>,
    known_chunks: HashMap<ChunkHash, ChunkLocation>,
    progress: ProgressBar,
    total_bytes_stored: u64,
    total_bytes_processed: u64,
}

struct CurrentBlock {
    id: BlockId,
    writer: Box<dyn Write>,
    bytes_written: usize,
}

impl CurrentBlock {
    pub fn is_full(&self) -> bool {
        self.bytes_written >= APPROX_MAX_BLOCK_SIZE
    }
}

impl<'a> Driver<'a> {
    pub fn new(config: Config<'a>) -> anyhow::Result<Self> {
        let latest_version = config
            .medium
            .load_version(0)?
            .map(|v| Version::decode(&v[..]));
        let known_chunks = match latest_version {
            Some(v) => load_known_chunks_from_version(&v?),
            None => HashMap::new(),
        };

        Ok(Self {
            config,
            current_block: None,
            known_chunks,
            progress: ProgressBar::new_spinner(),
            total_bytes_processed: 0,
            total_bytes_stored: 0,
        })
    }

    fn update_progress(&mut self) {
        self.progress.set_message(format!(
            "{} fresh / {} total",
            HumanBytes(self.total_bytes_stored),
            HumanBytes(self.total_bytes_processed)
        ));
    }

    fn back_up_path(&mut self, path: &Path) -> TreeEntry {
        loop {
            let result = match fs::metadata(path).map(|m| m.file_type()) {
                Ok(f) if f.is_file() => self.back_up_file(path).map(TreeEntry::File),
                Ok(f) if f.is_dir() => self.back_up_dir(path).map(TreeEntry::Directory),
                Ok(_) => Err(anyhow!("unsupported file type at {}", path.display())),
                Err(e) => Err(e.into()),
            };
            self.update_progress();
            match result {
                Ok(res) => break res,
                Err(e) => {
                    eprintln!("Error for path {}, retrying in 15s: {e:?}", path.display());
                    sleep(Duration::from_secs(15));
                }
            }
        }
    }

    pub fn back_up_dir(&mut self, path: &Path) -> anyhow::Result<DirectoryEntry> {
        let mut children = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry = self.back_up_path(&entry.path());
            children.push(entry);
        }

        Ok(DirectoryEntry {
            name: get_file_name(path)?,
            permissions: get_file_permissions(path)?,
            children,
        })
    }

    pub fn finish(mut self, root_entry: DirectoryEntry) -> anyhow::Result<()> {
        if let Some(mut current_block) = self.current_block.take() {
            current_block.writer.flush()?;
        }
        // Ensure all data is durably written before committing the new version file.
        self.config.medium.flush()?;

        let tree = Tree::new(root_entry.children);
        let version = Version::new(tree);
        let version_bytes = version.encode_to_bytes();
        self.config
            .medium
            .save_version(version_bytes, version.timestamp())?;

        Ok(())
    }

    fn back_up_file(&mut self, path: &Path) -> anyhow::Result<FileEntry> {
        let mut reader = ChunksReader::new(fs::File::open(path)?, CHUNK_SIZE);
        let mut chunks = Vec::new();

        while let Some(chunk_bytes) = reader.read_chunk()? {
            let hash = ChunkHash::from_blake3(blake3::hash(&chunk_bytes));

            let location = match self.known_chunks.get(&hash) {
                Some(location) => location.clone(),
                None => {
                    let current_block = match &mut self.current_block {
                        Some(b) => b,
                        None => self.start_new_block()?,
                    };

                    current_block.writer.write_all(&chunk_bytes)?;

                    let location = ChunkLocation {
                        block: current_block.id,
                        uncompressed_byte_offset: current_block.bytes_written,
                    };
                    current_block.bytes_written += chunk_bytes.len();
                    self.total_bytes_stored += u64::try_from(chunk_bytes.len()).unwrap();

                    self.known_chunks.insert(hash, location.clone());
                    location
                }
            };

            chunks.push(ChunkMetadata {
                hash,
                location,
                uncompressed_size: chunk_bytes.len(),
            });
            self.total_bytes_processed += u64::try_from(chunk_bytes.len()).unwrap();

            if self
                .current_block
                .as_ref()
                .map(|b| b.is_full())
                .unwrap_or(false)
            {
                self.start_new_block()?;
            }

            self.update_progress();
        }

        Ok(FileEntry {
            name: get_file_name(path)?,
            permissions: get_file_permissions(path)?,
            chunks,
        })
    }

    fn start_new_block(&mut self) -> anyhow::Result<&mut CurrentBlock> {
        if let Some(mut old_block) = self.current_block.take() {
            old_block.writer.flush()?;
        }

        let id = BlockId::new();
        let writer = self.config.medium.save_block(id)?;
        Ok(self.current_block.insert(CurrentBlock {
            id,
            writer,
            bytes_written: 0,
        }))
    }
}

fn load_known_chunks_from_version(version: &Version) -> HashMap<ChunkHash, ChunkLocation> {
    let mut map = HashMap::new();
    for entry in version.tree().root_children() {
        load_known_chunks_from_tree(entry, &mut map);
    }
    map
}

fn load_known_chunks_from_tree(
    tree: &TreeEntry,
    known_chunks: &mut HashMap<ChunkHash, ChunkLocation>,
) {
    match tree {
        TreeEntry::File(f) => {
            for ChunkMetadata { hash, location, .. } in &f.chunks {
                known_chunks.insert(*hash, location.clone());
            }
        }
        TreeEntry::Directory(dir) => {
            for child in &dir.children {
                load_known_chunks_from_tree(child, known_chunks);
            }
        }
    }
}
