use crate::medium::Medium;
use crate::pipe;
use crate::pipe::Reader;
use crate::storage::DataBlockId;
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;

/// A medium storing data in memory, used for testing.
#[derive(Default)]
pub struct MemoryMedium {
    data_blocks: HashMap<DataBlockId, Vec<u8>>,
    index: Option<Vec<u8>>,
    new_data_blocks: Arc<Mutex<VecDeque<(DataBlockId, Vec<u8>)>>>,
}

impl MemoryMedium {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn data_blocks(&self) -> impl Iterator<Item = (DataBlockId, &[u8])> {
        self.data_blocks
            .iter()
            .map(|(&id, data)| (id, data.as_slice()))
    }

    pub fn index(&self) -> Option<&[u8]> {
        self.index.as_ref().map(Vec::as_slice)
    }
}

impl Medium for MemoryMedium {
    fn load_index(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.index.clone())
    }

    fn queue_save_index(&mut self, index_data: Vec<u8>) {
        self.index = Some(index_data);
    }

    fn load_data_block(&mut self, id: DataBlockId) -> anyhow::Result<Option<Reader>> {
        let data_block = match self.data_blocks.get(&id) {
            Some(d) => d,
            None => return Ok(None),
        };
        let (mut writer, reader) = pipe::new(data_block.len());
        writer.write_all(data_block).unwrap();
        Ok(Some(reader))
    }

    fn queue_save_data_block(&mut self, id: DataBlockId, mut data: Reader) {
        let new_data_blocks = Arc::clone(&self.new_data_blocks);
        thread::spawn(move || {
            let mut buf = Vec::new();
            data.read_to_end(&mut buf).expect("read error");
            new_data_blocks.lock().unwrap().push_back((id, buf));
        });
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        for (id, data) in self.new_data_blocks.lock().unwrap().drain(..) {
            self.data_blocks.insert(id, data);
        }
        Ok(())
    }
}
