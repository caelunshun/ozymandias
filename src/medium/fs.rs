use crate::backup::PIPE_BUFFER_SIZE;
use crate::medium::Medium;
use crate::pipe;
use crate::pipe::Reader;
use crate::storage::DataBlockId;
use anyhow::Context;
use file_guard::{FileGuard, Lock};
use flume::{Receiver, Sender};
use fs_err as fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{
    io,
    thread::{self, JoinHandle},
};

const LOCKFILE_NAME: &str = "lock";
const INDEX_NAME: &str = "index";
const DATA_BLOCKS_DIR: &str = "data";

/// A medium backed by a directory mounted to the filesystem.
pub struct FilesystemMedium {
    dir: Arc<Path>,
    lock: FileGuard<fs::File>,
    errors_tx: Sender<anyhow::Error>,
    errors: Receiver<anyhow::Error>,
    join_handles: Vec<JoinHandle<()>>,
}

impl FilesystemMedium {
    pub fn new(dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir(dir)?;

        let data_dir = dir.join(DATA_BLOCKS_DIR);
        fs::create_dir(data_dir)?;

        let lockfile = dir.join(LOCKFILE_NAME);
        let lockfile = fs::File::create(lockfile)?;
        let lock = file_guard::lock(lockfile, Lock::Exclusive, 0, 1)?;

        let (errors_tx, errors) = flume::unbounded();

        Ok(Self {
            dir: dir.to_path_buf().into(),
            lock,
            errors_tx,
            errors,
            join_handles: Vec::new(),
        })
    }

    fn index_path(&self) -> PathBuf {
        self.dir.join(INDEX_NAME)
    }

    fn data_block_path(&self, id: DataBlockId) -> PathBuf {
        self.dir.join(DATA_BLOCKS_DIR).join(id.to_hex())
    }

    /// Runs a fallible operation on a thread, reporting
    /// any errors to the error queue.
    fn execute_threaded(&mut self, op: impl FnOnce() -> anyhow::Result<()> + Send + 'static) {
        let errors_tx = self.errors_tx.clone();
        self.join_handles.push(thread::spawn(move || {
            if let Err(e) = op() {
                errors_tx.send(e).ok();
            }
        }));
    }
}

impl Medium for FilesystemMedium {
    fn load_index(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        match fs::read(self.index_path()) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn queue_save_index(&mut self, index_data: Vec<u8>) {
        let path = self.index_path();
        self.execute_fallible(move || {
            fs::write(&path, &index_data)?;
            Ok(())
        });
    }

    fn load_data_block(&mut self, id: DataBlockId) -> anyhow::Result<Option<Reader>> {
        let (writer, reader) = pipe::new(PIPE_BUFFER_SIZE);
        let file = match fs::File::open(self.data_block_path(id)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        self.execute_threaded(move || {
            writer.consume_reader(file);
            Ok(())
        });
        Ok(Some(reader))
    }

    fn queue_save_data_block(&mut self, id: DataBlockId, mut data: Reader) {
        let path = self.data_block_path(id);
        self.execute_threaded(move || {
            let mut file = fs::File::create(path)?;
            io::copy(&mut data, &mut file)?;
            Ok(())
        });
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        for handle in self.join_handles.drain(..) {
            handle.join().ok().context("thread panicked")?;
        }

        if let Ok(err) = self.errors.try_recv() {
            Err(err)
        } else {
            Ok(())
        }
    }
}
