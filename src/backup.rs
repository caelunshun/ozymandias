//! Implements the backup process.

use crate::medium::Medium;
use crate::path::SimplifiedPath;
use crate::pipe;
use crate::storage::{
    CompressionFormat, DataBlockId, FileDataChunkLocation, FileInstance, FileNode, FileNodeId,
    Index, Revision,
};
use anyhow::Context;
use flume::Sender;
use fs_err as fs;
use std::collections::BTreeMap;
use std::io;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use threadpool::ThreadPool;
use time::OffsetDateTime;

#[allow(non_upper_case_globals)]
const KiB: usize = 1024;
#[allow(non_upper_case_globals)]
const MiB: usize = 1024 * KiB;

/// Desired approximate size of a single data block.
const TARGET_BLOCK_SIZE: u64 = 32 * MiB as u64;

/// Size of the pipe buffer for each data block.
pub const PIPE_BUFFER_SIZE: usize = 256 * KiB;

const NUM_IO_THREADS: usize = 8;

#[derive(Debug)]
enum MessageToDriver {
    NewBlock { id: DataBlockId, data: pipe::Reader },
    NewFileNode { id: FileNodeId, node: FileNode },
    Error(anyhow::Error),
}

/// The main coordinator thread for a backup process.
pub struct Driver {
    medium: Box<dyn Medium>,
    io_thread_pool: ThreadPool,
}

impl Driver {
    pub fn new(medium: Box<dyn Medium>) -> Self {
        Self {
            medium,
            io_thread_pool: ThreadPool::new(NUM_IO_THREADS),
        }
    }

    /// Executes a backup of the given directory to the underlying storage medium.
    pub fn run(&mut self, dir: impl AsRef<Path>) -> anyhow::Result<()> {
        let (messages_tx, messages) = flume::unbounded();

        let mut index = self
            .medium
            .load_index()?
            .map(|bytes| Index::deserialize(&bytes))
            .unwrap_or_else(|| Ok(Index::new()))?;

        let dir = dir.as_ref();

        tracing::info!("Scanning files to back up");
        let files = self.enumerate_files(dir)?;
        let total_size = files.iter().map(|f| f.len).sum::<u64>();
        tracing::info!(
            "Found {} files, totaling {} of data",
            files.len(),
            humansize::format_size(total_size, humansize::BINARY)
        );

        let mut files_to_create = Vec::new();
        for file in &files {
            let id = FileNodeId::from_hash(file.hash);
            if !index.data.file_nodes.contains_key(&id) {
                files_to_create.push(file.clone());
            }
        }
        let total_new_size = files_to_create.iter().map(|f| f.len).sum::<u64>();
        tracing::info!(
            "{} files need updated, totaling {} of data to upload",
            files_to_create.len(),
            humansize::format_size(total_new_size, humansize::BINARY)
        );

        let small_files_to_create: Vec<_> = files_to_create
            .iter()
            .filter(|f| f.len <= TARGET_BLOCK_SIZE)
            .cloned()
            .collect();
        let large_files_to_create: Vec<_> = files_to_create
            .iter()
            .filter(|f| f.len > TARGET_BLOCK_SIZE)
            .cloned()
            .collect();

        let mut partitions = partition_files(small_files_to_create, |f| f.len, TARGET_BLOCK_SIZE);
        partitions.extend(large_files_to_create.into_iter().map(|f| vec![f]));

        for partition in partitions {
            let messages_tx = messages_tx.clone();
            self.io_thread_pool.execute(move || {
                if let Err(e) = process_partition(partition, &messages_tx) {
                    messages_tx.send(MessageToDriver::Error(e)).ok();
                }
            });
        }

        // Drop the sender so that once all tasks complete,
        // our iteration over `messages` stops.
        drop(messages_tx);

        for message in messages {
            match message {
                MessageToDriver::NewBlock { id, data } => {
                    self.medium.queue_save_data_block(id, data);
                }
                MessageToDriver::NewFileNode { id, node } => {
                    index.data.file_nodes.insert(id, node);
                }
                MessageToDriver::Error(e) => return Err(e),
            }
        }

        self.io_thread_pool.join();
        self.medium.flush()?;

        // Write the final index state.
        let mut tree = BTreeMap::new();
        for file in files {
            let id = FileNodeId::from_hash(file.hash);
            assert!(index.data.file_nodes.contains_key(&id));
            tree.insert(file.simplified_path, FileInstance { node: id });
        }
        index.data.revisions.push(Revision {
            timestamp: OffsetDateTime::now_utc(),
            file_tree: tree,
        });

        self.medium
            .queue_save_index(index.serialize(CompressionFormat::Zstd));
        self.medium.flush()?;

        Ok(())
    }

    /// Enumerates files in the backup directory, evaluating their metadata
    /// (in parallel).
    fn enumerate_files(&self, dir: &Path) -> anyhow::Result<Vec<FileMetadata>> {
        let (sink, receiver) = flume::unbounded();

        self.enumerate_files_recursive(dir, &SimplifiedPath::default(), &sink)?;

        let mut files = Vec::new();
        for result in receiver {
            let metadata = result?;
            files.push(metadata);
        }

        Ok(files)
    }

    fn enumerate_files_recursive(
        &self,
        dir: &Path,
        dir_simplified_path: &SimplifiedPath,
        sink: &Sender<anyhow::Result<FileMetadata>>,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;

            let mut entry_simplified_path = dir_simplified_path.clone();
            entry_simplified_path.push(entry.file_name());

            let entry_type = entry.file_type()?;
            if entry_type.is_symlink() {
                todo!("follow symlinks")
            } else if entry_type.is_dir() {
                self.enumerate_files_recursive(&entry.path(), &entry_simplified_path, sink)?;
            } else if entry_type.is_file() {
                let sink = sink.clone();
                let path = entry.path();
                self.io_thread_pool.execute(move || {
                    sink.send(FileMetadata::read(&path, entry_simplified_path))
                        .ok();
                });
            }
        }

        Ok(())
    }
}

/// Metadata about a file to be backed up.
///
/// Because files can be mutated by other processes after
/// we read this metadata, this data may become outdated.
/// In particular, the `hash` should be recomputed when we
/// read the file data for final storage.
#[derive(Debug, Clone)]
struct FileMetadata {
    path: PathBuf,
    simplified_path: SimplifiedPath,
    len: u64,
    hash: blake3::Hash,
}

impl FileMetadata {
    pub fn read(path: impl AsRef<Path>, simplified_path: SimplifiedPath) -> anyhow::Result<Self> {
        let mut hasher = blake3::Hasher::new();
        let mut len = 0;
        let mut buffer = vec![0u8; PIPE_BUFFER_SIZE];

        let mut file = fs::File::open(path.as_ref())?;
        loop {
            match file.read(&mut buffer)? {
                0 => break,
                n => {
                    hasher.update(&buffer[..n]);
                    len += n as u64;
                }
            }
        }

        let hash = hasher.finalize();

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            simplified_path,
            len,
            hash,
        })
    }
}

/// Given a list of files with known sizes,
/// returns an approximately optimal portioning of those files
/// into groups of at most the given size.
///
/// # Panics
/// Panics if any file has a size greater than `maximum_block_size`.
fn partition_files<T>(
    mut files: Vec<T>,
    get_size: impl Fn(&T) -> u64,
    maximum_block_size: u64,
) -> Vec<Vec<T>> {
    files.sort_unstable_by_key(&get_size);

    let mut partitions = Vec::new();

    let mut iter = files.drain(..).peekable();
    while let Some(file) = iter.next() {
        let mut partition_size = get_size(&file);
        assert!(partition_size <= maximum_block_size);
        let mut partition = vec![file];

        while let Some(next) = iter.peek() {
            let next_size = get_size(next);
            if partition_size + next_size <= maximum_block_size {
                partition.push(iter.next().expect("already peeked"));
                partition_size += next_size;
            } else {
                break;
            }
        }

        partitions.push(partition);
    }

    partitions
}

fn process_partition(
    partition: Vec<FileMetadata>,
    driver: &Sender<MessageToDriver>,
) -> anyhow::Result<()> {
    let (mut current_block, reader) = BlockWriter::new();
    driver
        .send(MessageToDriver::NewBlock {
            id: current_block.id,
            data: reader,
        })
        .ok()
        .context("driver disconnected")?;

    for file_info in partition {
        let mut file = match fs::File::open(&file_info.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                tracing::info!(
                    "{} was deleted between scan and backup; skipping",
                    file_info.path.display()
                );
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let mut hasher = blake3::Hasher::new();
        let mut chunks = Vec::new();

        loop {
            let chunk_offset = current_block.bytes_written();
            let (status, chunk_len) = current_block.append(&mut file, |data| {
                hasher.update(data);
            })?;

            chunks.push(FileDataChunkLocation {
                block: current_block.id,
                offset: chunk_offset,
                size: chunk_len,
            });

            match status {
                FileWriteStatus::Complete => {
                    let hash = hasher.finalize();
                    driver
                        .send(MessageToDriver::NewFileNode {
                            id: FileNodeId::from_hash(hash),
                            node: FileNode { chunks },
                        })
                        .ok()
                        .context("driver disconnected")?;
                    break;
                }
                FileWriteStatus::Incomplete => {
                    current_block.finish();
                    let (new_current_block, new_reader) = BlockWriter::new();
                    current_block = new_current_block;
                    driver
                        .send(MessageToDriver::NewBlock {
                            id: current_block.id,
                            data: new_reader,
                        })
                        .ok()
                        .context("driver disconnected")?;
                }
            }
        }
    }

    Ok(())
}

struct BlockWriter {
    id: DataBlockId,
    writer: zstd::Encoder<'static, pipe::Writer>,
    read_buffer: Vec<u8>,
}

impl BlockWriter {
    const READ_BUFFER_SIZE: usize = 64 * KiB;

    fn bytes_written(&self) -> u64 {
        self.writer.get_ref().bytes_written()
    }

    pub fn new() -> (Self, pipe::Reader) {
        let (writer, reader) = pipe::new(PIPE_BUFFER_SIZE);
        let writer = zstd::Encoder::new(writer, zstd::DEFAULT_COMPRESSION_LEVEL)
            .expect("failed to initialize zstd");
        (
            Self {
                writer,
                read_buffer: vec![0u8; Self::READ_BUFFER_SIZE],
                id: DataBlockId::new_random(),
            },
            reader,
        )
    }

    /// Appends a file to the writer.
    ///
    /// If we exceed the block size, this function returns
    /// `Ok(FileWriteStatus::Incomplete)`.
    pub fn append(
        &mut self,
        mut data: impl Read,
        mut preview_data: impl FnMut(&[u8]),
    ) -> anyhow::Result<(FileWriteStatus, u64)> {
        let mut bytes_written = 0;
        loop {
            match data.read(&mut self.read_buffer) {
                Ok(0) => break,
                Ok(n) => {
                    preview_data(&self.read_buffer[..n]);
                    self.writer.write_all(&self.read_buffer[..n])?;
                    bytes_written += n as u64;
                    if self.bytes_written() > TARGET_BLOCK_SIZE {
                        return Ok((FileWriteStatus::Incomplete, bytes_written));
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok((FileWriteStatus::Complete, bytes_written))
    }

    pub fn finish(self) {
        self.writer.finish().expect("failed to flush zstd");
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum FileWriteStatus {
    Complete,
    Incomplete,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_files_small() {
        let files = vec![5, 8, 2, 3, 10, 1];
        let mut partitions = partition_files(files, |x| *x, 10);
        partitions.iter_mut().for_each(|p| p.sort());
        partitions.sort();
        assert_eq!(partitions, vec![vec![1, 2, 3], vec![5], vec![8], vec![10]]);
    }

    #[test]
    fn partition_files_empty() {
        assert!(partition_files::<u64>(vec![], |x| *x, 10).is_empty());
    }

    #[test]
    fn partition_files_all_one_block() {
        let mut files = vec![5, 8, 2, 3, 8, 1];
        let mut partitions = partition_files(files.clone(), |x| *x, 1000);
        partitions.iter_mut().for_each(|p| p.sort());

        files.sort();
        assert_eq!(partitions, vec![files]);
    }

    #[test]
    #[ignore] // (TODO)
    fn partition_files_tricky_optimization_case() {
        // Naive approach of sorting the files and greedily assigning them to partitions will not work here.
        let mut files = vec![1, 1, 1, 8, 8];
        let mut partitions = partition_files(files, |x| *x, 10);
        partitions.iter_mut().for_each(|p| p.sort());
        partitions.sort();
        assert_eq!(partitions, vec![vec![1, 1, 8], vec![1, 8],]);
    }

    #[test]
    #[should_panic]
    fn partition_files_too_large() {
        partition_files(vec![100], |x| *x, 99);
    }
}
