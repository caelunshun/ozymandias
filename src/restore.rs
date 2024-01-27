//! This module implements restoring a backup version onto the local filesystem.
//!
//! The steps are as follows:
//! 1. Construct a `Plan` determining in which order files / chunks
//!    will be retrieved from the backup medium. In this step,
//!    we also check for chunks whose hashes on the local filesystem
//!    already match the backup - these do not need to be loaded from the medium.
//! 2. Initialize the directory structure in the `Tree` of the backup version.
//! 3. For each file in the `Plan`, retrieve its chunks from the medium and write
//!    them to disk.
//!
//! TODO: remaining issue: correctly handle when an existing file is larger than the stored file

use crate::medium::Medium;
use crate::model::{BlockId, ChunkHash, ChunkLocation, FileEntry, Tree, TreeEntry, Version};
use crate::MAX_RESTORE_CHUNK_SIZE;
use anyhow::bail;
use indicatif::{HumanBytes, ProgressBar};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::{io, iter};

pub fn run(
    medium: &dyn Medium,
    version: &Version,
    target_root: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let target_root = target_root.as_ref();

    let progress = ProgressBar::new_spinner();
    progress.set_message("scanning");

    let plan = create_plan(version.tree(), target_root)?;
    create_directory_structure(version.tree(), target_root)?;
    drop(progress);

    Driver::new(medium, &plan).do_restore()?;

    Ok(())
}

/// Describes the order in which chunks will be loaded
/// from the medium. Optimizing the order such that
/// chunks in the same block are loaded together is important
/// to reduce the number of accesses to the backup medium.
struct Plan {
    restore_chunks: Vec<RestoreChunk>,
}

impl Plan {
    /// Optimizes the plan such that chunks in the same block are loaded together
    /// when possible.
    ///
    /// Additionally, chunks within a block will be ordered according to their offset
    /// within the block.
    pub fn post_process(&mut self) {
        self.restore_chunks.sort_unstable_by_key(|restore_chunk| {
            (
                restore_chunk.location.block,
                restore_chunk.location.uncompressed_byte_offset,
            )
        });
    }

    pub fn total_bytes_to_restore(&self) -> u64 {
        self.restore_chunks
            .iter()
            .map(|chunk| u64::try_from(chunk.num_bytes).unwrap())
            .sum()
    }
}

/// Describes a chunk to restore.
struct RestoreChunk {
    /// Location in the backup.
    location: ChunkLocation,
    /// Target file the chunk should be written to.
    target_path: PathBuf,
    /// Byte offset within the target file at which the chunk will be written.
    /// If `None`, then the chunk is appended to the end of the file.
    target_byte_offset: Option<u64>,
    num_bytes: usize,
}

/// Creates a `Plan` for restoring a backup version to the provided
/// target directory.
fn create_plan(backup_tree: &Tree, target_root: &Path) -> anyhow::Result<Plan> {
    let mut plan = Plan {
        restore_chunks: Vec::new(),
    };
    for root_child in backup_tree.root_children() {
        add_entry_to_plan(root_child, &target_root.join(root_child.name()), &mut plan)?;
    }
    plan.post_process();
    Ok(plan)
}

fn add_entry_to_plan(
    entry: &TreeEntry,
    current_path: &Path,
    plan: &mut Plan,
) -> anyhow::Result<()> {
    match entry {
        TreeEntry::File(file_entry) => {
            add_file_entry_to_plan(file_entry, current_path, plan)?;
        }
        TreeEntry::Directory(dir) => {
            for child in &dir.children {
                add_entry_to_plan(child, &current_path.join(child.name()), plan)?;
            }
        }
    }
    Ok(())
}

fn add_file_entry_to_plan(
    file_entry: &FileEntry,
    target_path: &Path,
    plan: &mut Plan,
) -> anyhow::Result<()> {
    let restore_chunks = match fs::File::open(target_path) {
        Ok(file) => diff_file_chunks(file_entry, file, target_path)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => all_file_chunks(file_entry, target_path)?,
        Err(e) => return Err(e.into()),
    };
    plan.restore_chunks.extend(restore_chunks);
    Ok(())
}

fn diff_file_chunks(
    file_entry: &FileEntry,
    mut file: fs::File,
    file_path: &Path,
) -> anyhow::Result<Vec<RestoreChunk>> {
    let mut restore_chunks = Vec::new();

    let mut buffer = Vec::new();

    let mut byte_offset = Some(0);

    for chunk in &file_entry.chunks {
        if chunk.uncompressed_size > MAX_RESTORE_CHUNK_SIZE {
            bail!("chunk size is too large, potential OOM risk");
        }

        buffer.extend(iter::repeat(0).take(chunk.uncompressed_size));

        let chunk_needs_restore = match file.read_exact(&mut buffer) {
            Ok(_) => {
                // File has enough data to maybe have the chunk; check the hash
                let hash = ChunkHash::from_blake3(blake3::hash(&buffer));
                hash != chunk.hash
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Chunk not fully present. Set `byte_offset to `None`
                // so that remaining chunks are written to the end of the file.
                byte_offset = None;
                true
            }
            Err(e) => return Err(e.into()),
        };

        if chunk_needs_restore {
            restore_chunks.push(RestoreChunk {
                location: chunk.location.clone(),
                target_path: file_path.to_path_buf(),
                target_byte_offset: byte_offset,
                num_bytes: chunk.uncompressed_size,
            });
        }

        if let Some(byte_offset) = &mut byte_offset {
            *byte_offset += u64::try_from(chunk.uncompressed_size).unwrap();
        }

        buffer.clear();
    }
    Ok(restore_chunks)
}

fn all_file_chunks(file_entry: &FileEntry, file_path: &Path) -> anyhow::Result<Vec<RestoreChunk>> {
    let mut restore_chunks = Vec::<RestoreChunk>::new();
    for chunk in &file_entry.chunks {
        restore_chunks.push(RestoreChunk {
            location: chunk.location.clone(),
            target_path: file_path.to_path_buf(),
            target_byte_offset: None,
            num_bytes: chunk.uncompressed_size,
        });
    }
    Ok(restore_chunks)
}

/// Initializes the directory structure present in the given tree
/// on the local filesystem.
fn create_directory_structure(tree: &Tree, target_root: &Path) -> anyhow::Result<()> {
    fs::create_dir(target_root).ok();
    for root_child in tree.root_children() {
        create_directory_structure_recursive(root_child, &target_root.join(root_child.name()))?;
    }
    Ok(())
}

fn create_directory_structure_recursive(
    entry: &TreeEntry,
    current_path: &Path,
) -> anyhow::Result<()> {
    if let TreeEntry::Directory(dir) = entry {
        match fs::create_dir(current_path) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if !fs::metadata(current_path)?.file_type().is_dir() {
                    bail!(
                        "file in local filesystem at {} expected to be a directory",
                        current_path.display()
                    );
                }
            }
            Err(e) => return Err(e.into()),
        }

        for child in &dir.children {
            create_directory_structure_recursive(child, &current_path.join(child.name()))?;
        }
    }
    Ok(())
}

/// Driver for performing the restore operation, given an optimized `Plan`
/// and an initialized directory structure.
struct Driver<'a> {
    medium: &'a dyn Medium,
    plan: &'a Plan,
    /// Current block fetched from the medium.
    /// Cached so it can be used across multiple chunks.
    current_block: Option<CurrentBlock>,
    current_open_file: Option<CurrentOpenFile>,
    progress: ProgressBar,
    total_bytes_restored: u64,
}

impl<'a> Driver<'a> {
    pub fn new(medium: &'a dyn Medium, plan: &'a Plan) -> Self {
        Self {
            medium,
            plan,
            current_block: None,
            current_open_file: None,
            progress: ProgressBar::new(plan.total_bytes_to_restore()),
            total_bytes_restored: 0,
        }
    }

    fn update_progress(&mut self) {
        self.progress.set_position(self.total_bytes_restored);
        self.progress.set_message(format!(
            "restoring: {} / {}",
            HumanBytes(self.total_bytes_restored),
            HumanBytes(self.plan.total_bytes_to_restore())
        ));
    }

    pub fn do_restore(&mut self) -> anyhow::Result<()> {
        for chunk in &self.plan.restore_chunks {
            self.restore_chunk(chunk)?;
            self.update_progress();
        }
        Ok(())
    }

    fn restore_chunk(&mut self, chunk: &RestoreChunk) -> anyhow::Result<()> {
        let target_file = match &mut self.current_open_file {
            Some(cached) if cached.path == chunk.target_path => &mut cached.file,
            _ => {
                let file = open_or_create_file(&chunk.target_path)?;
                &mut self
                    .current_open_file
                    .insert(CurrentOpenFile {
                        file,
                        path: chunk.target_path.clone(),
                    })
                    .file
            }
        };

        let source_block = match &mut self.current_block {
            Some(cached) if cached.id == chunk.location.block => cached,
            _ => {
                let mut reader = self.medium.load_block(chunk.location.block)?;
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes)?;

                self.current_block.insert(CurrentBlock {
                    bytes,
                    id: chunk.location.block,
                })
            }
        };

        let chunk_size = chunk.num_bytes;
        self.total_bytes_restored += u64::try_from(chunk_size).unwrap();

        match chunk.target_byte_offset {
            Some(offset) => {
                target_file.seek(SeekFrom::Start(offset))?;
            }
            None => {
                target_file.seek(SeekFrom::End(0))?;
            }
        }
        let bytes = &source_block.bytes[chunk.location.uncompressed_byte_offset..][..chunk_size];
        target_file.write_all(bytes)?;

        Ok(())
    }
}

struct CurrentBlock {
    id: BlockId,
    bytes: Vec<u8>,
}

struct CurrentOpenFile {
    file: fs::File,
    path: PathBuf,
}

fn open_or_create_file(path: &Path) -> io::Result<fs::File> {
    fs::OpenOptions::new().write(true).create(true).open(path)
}
