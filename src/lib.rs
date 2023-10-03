pub mod backup;
pub mod medium;
pub mod path;
pub mod pipe;
pub mod restore;
pub mod storage;

#[allow(non_upper_case_globals)]
const KiB: usize = 1024;
#[allow(non_upper_case_globals)]
const MiB: usize = 1024 * KiB;

/// Desired approximate size of a single data block.
const TARGET_BLOCK_SIZE: u64 = 32 * MiB as u64;

/// Size of pipe buffers responsible for streaming data between threads.
pub const PIPE_BUFFER_SIZE: usize = 128 * KiB;

/// Size of the read buffer for file I/O.
const READ_BUFFER_SIZE: usize = 64 * KiB;

/// Max number of concurrent file I/O operations.
const MAX_IO_PARALLELISM: usize = 4;
