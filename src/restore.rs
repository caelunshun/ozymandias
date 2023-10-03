use crate::medium::Medium;
use crate::path::SimplifiedPath;
use crate::storage::{DataBlockId, FileDataChunkLocation, Index, RevisionId};
use crate::{pipe, MAX_IO_PARALLELISM, TARGET_BLOCK_SIZE};
use anyhow::{bail, Context};
use crossbeam_queue::SegQueue;
use event_listener::{Event, EventListener};
use fs_err as fs;
use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use threadpool::ThreadPool;
use time::OffsetDateTime;

/// Coordinates a restore-from-backup operation to a target directory.
pub struct Driver<'a> {
    medium: &'a mut dyn Medium,
    target_dir: PathBuf,
    io_thread_pool: ThreadPool,
    index: Index,

    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    errors: SegQueue<anyhow::Error>,
    running_jobs: AtomicUsize,
    job_completed: Event,
}

impl<'a> Driver<'a> {
    /// Prepares for a restore operation, loading the index.
    ///
    /// Returns an error if `target_dir` exists.
    pub fn new(medium: &'a mut dyn Medium, target_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let target_dir = target_dir.as_ref().to_path_buf();
        fs::create_dir(&target_dir)?;

        tracing::info!("Loading index");
        let index = medium.load_index()?.context("no backup index found")?;
        let index = Index::deserialize(&index)?;

        Ok(Self {
            medium,
            target_dir,
            io_thread_pool: ThreadPool::new(MAX_IO_PARALLELISM),
            index,
            shared: Arc::new(Shared::default()),
        })
    }

    /// Returns the available revisions to restore from.
    pub fn available_revisions(&self) -> impl Iterator<Item = (RevisionId, OffsetDateTime)> + '_ {
        self.index
            .data
            .revisions
            .iter()
            .enumerate()
            .map(|(i, r)| (i, r.timestamp))
    }

    /// Restores the given revision to the target directory.
    ///
    /// # Panics
    /// Panics if `revision_id` is invalid.
    pub fn run(mut self, revision_id: RevisionId) -> anyhow::Result<()> {
        let plan = self.create_plan(revision_id)?;
        plan.log_info();
        self.execute_plan(plan)?;
        self.wait_for_job_completion()?;
        tracing::info!("Restore complete.");
        Ok(())
    }

    fn create_plan(&mut self, revision_id: RevisionId) -> anyhow::Result<Plan> {
        let revision = &self.index.data.revisions[revision_id];

        let mut steps = Vec::new();
        let mut block_group_mapping: HashMap<
            DataBlockId,
            Vec<(SimplifiedPath, FileDataChunkLocation)>,
        > = HashMap::new();

        for (path, file_instance) in &revision.file_tree {
            let node = self
                .index
                .data
                .file_nodes
                .get(&file_instance.node)
                .context("missing file node entry")?;
            match node.chunks.as_slice() {
                [] => bail!("file node with zero chunks is unsupported"),
                [single_chunk] => {
                    block_group_mapping
                        .entry(single_chunk.block)
                        .or_default()
                        .push((path.clone(), single_chunk.clone()));
                }
                chunks => {
                    steps.push(Step::LoadChunked {
                        file: path.clone(),
                        chunks: chunks.to_vec(),
                    });
                }
            }
        }

        for (block, files) in block_group_mapping {
            steps.push(Step::LoadSingleBlock { block, files });
        }

        Ok(Plan { steps })
    }

    fn execute_plan(&mut self, plan: Plan) -> anyhow::Result<()> {
        for step in plan.steps {
            self.execute_step(step)?;
        }
        Ok(())
    }

    fn execute_step(&mut self, step: Step) -> anyhow::Result<()> {
        self.wait_for_new_job()?;
        match step {
            Step::LoadSingleBlock { block, files } => self.execute_load_single_block(block, files),
            Step::LoadChunked { file, chunks } => self.execute_load_chunked(file, chunks),
        }
    }

    fn execute_load_single_block(
        &mut self,
        block: DataBlockId,
        files: Vec<(SimplifiedPath, FileDataChunkLocation)>,
    ) -> anyhow::Result<()> {
        assert!(files.iter().all(|(_, loc)| loc.block == block));
        let mut block_reader = self.open_data_block(block)?;
        let base_path = self.target_dir.clone();

        self.execute_job(move || {
            // Future optimization: stream the data. This is error-prone, however,
            // especially when considering the various edge cases (e.g., the format
            // does not forbid overlapping chunks in a data block)
            let mut buf = Vec::with_capacity(TARGET_BLOCK_SIZE.try_into().unwrap());
            block_reader.read_to_end(&mut buf)?;

            for (rel_path, location) in files {
                let path = rel_path.instantiate_with_root(&base_path);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).ok();
                }

                let mut file = fs::File::create(&path)?;
                file.write_all(&buf[location.offset as usize..][..location.size as usize])?;
            }

            Ok(())
        });
        Ok(())
    }

    fn execute_load_chunked(
        &mut self,
        _file: SimplifiedPath,
        _chunks: Vec<FileDataChunkLocation>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn execute_job(&self, job: impl FnOnce() -> anyhow::Result<()> + Send + 'static) {
        let shared = Arc::clone(&self.shared);
        self.io_thread_pool.execute(move || {
            if let Err(e) = job() {
                shared.errors.push(e);
            }
            shared.running_jobs.fetch_sub(1, Ordering::SeqCst);
            shared.job_completed.notify(1);
        });
    }

    fn open_data_block(
        &mut self,
        id: DataBlockId,
    ) -> anyhow::Result<zstd::Decoder<'static, BufReader<pipe::Reader>>> {
        self.medium
            .load_data_block(id)
            .and_then(|opt| opt.context("missing data block"))
            .and_then(|reader| zstd::Decoder::new(reader).map_err(From::from))
    }

    fn wait_for_new_job(&mut self) -> anyhow::Result<()> {
        let mut listener = pin!(EventListener::new(&self.shared.job_completed));
        listener.as_mut().listen();
        if self.shared.running_jobs.load(Ordering::Acquire) >= MAX_IO_PARALLELISM {
            listener.wait();
        }

        if let Some(err) = self.shared.errors.pop() {
            return Err(err);
        }

        self.shared.running_jobs.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn wait_for_job_completion(&self) -> anyhow::Result<()> {
        let mut listener = pin!(EventListener::new(&self.shared.job_completed));
        listener.as_mut().listen();
        while self.shared.running_jobs.load(Ordering::Acquire) > 0 {
            listener.as_mut().wait();
        }

        if let Some(error) = self.shared.errors.pop() {
            return Err(error);
        }

        Ok(())
    }
}

struct Plan {
    steps: Vec<Step>,
}

impl Plan {
    pub fn log_info(&self) {
        let mut num_files = 0;
        let mut num_bytes = 0;
        for step in &self.steps {
            match step {
                Step::LoadChunked { chunks, .. } => {
                    num_files += 1;
                    for chunk in chunks {
                        num_bytes += chunk.size;
                    }
                }
                Step::LoadSingleBlock { files, .. } => {
                    for (_, loc) in files {
                        num_files += 1;
                        num_bytes += loc.size;
                    }
                }
            }
        }

        tracing::info!(
            "Restore plan will restore {num_files} files, totalling {} of data",
            humansize::format_size(num_bytes, humansize::BINARY)
        );
    }
}

enum Step {
    LoadSingleBlock {
        block: DataBlockId,
        files: Vec<(SimplifiedPath, FileDataChunkLocation)>,
    },
    LoadChunked {
        file: SimplifiedPath,
        chunks: Vec<FileDataChunkLocation>,
    },
}
