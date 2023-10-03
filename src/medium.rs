use crate::pipe;
use crate::storage::DataBlockId;

pub mod fs;
pub mod memory;
pub mod s3;

/// A storage medium for backup data.
///
/// Note that many functions here are specified to not block on I/O operations.
/// Medium implementations must use e.g. a thread spawner and a queue to avoid blocking
/// the driver thread.
///
/// # Locking contract
/// In order to ensure backup consistency, a `Medium` must guarantee that the underlying
/// storage will not be written to by other agents. For example, the filesystem-backed implementation
/// uses file locking to achieve this.
pub trait Medium: 'static {
    /// Blocks on loading the backup index, or `None` if no
    /// index has been written to the storage yet (i.e. when
    /// the storage has never been backed up to before).
    fn load_index(&mut self) -> anyhow::Result<Option<Vec<u8>>>;

    /// Queues the given index data to be written to the index file.
    ///
    /// After this operation completes (which is guaranteed after a call
    /// to `flush()` returns), a future call to `load_index()` must
    /// return the new index data.
    fn queue_save_index(&mut self, index_data: Vec<u8>);

    /// Loads the data block with the given name, returning a pipe of the streamed data.
    ///
    /// This function should avoid blocking on heavy I/O.
    fn load_data_block(&mut self, id: DataBlockId) -> anyhow::Result<Option<pipe::Reader>>;

    /// Queues the given data block to be written.
    ///
    /// This function accepts a pipe of the data. The pipe should
    /// be read from until it is closed.
    fn queue_save_data_block(&mut self, id: DataBlockId, data: pipe::Reader);

    /// Waits for all pending write operations to complete.
    /// Returns an error if any of them failed.
    ///
    /// After this function returns, any saved data must have
    /// been durably written to the underlying storage.
    fn flush(&mut self) -> anyhow::Result<()>;
}
