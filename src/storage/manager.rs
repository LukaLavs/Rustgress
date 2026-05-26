use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::storage::buffer::manager::BufferPoolManager;
use crate::storage::disk::manager::Table;
use crate::utils::debug::errors::{DiskError, LockError};

/// StorageManager is responsible for managing access to tables, 
/// preventing multiple concurrent write accesses.
pub struct StorageManager {
    bpm: Arc<BufferPoolManager>,
    tables: RwLock<HashMap<u32, Arc<RwLock<Table>>>>, // map from table oid to Table instance
}

impl StorageManager {
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            bpm,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Get Table access, if already open return existing instance.
    pub fn get_table(&self, oid: u32) -> Result<Arc<RwLock<Table>>, DiskError> {
        {   // already exist, return it 
            let tables_read = self.tables.read().map_err(|_| LockError)?;
            if let Some(table) = tables_read.get(&oid) {
                return Ok(Arc::clone(table));
            }
        }
        let mut tables_write = self.tables.write().map_err(|_| LockError)?;
        let table = Table::open(oid)?;
        Ok(
            tables_write.entry(oid)
                .or_insert_with(|| Arc::new(RwLock::new(table)))
                .clone()
        )
    }

    pub fn get_bpm(&self) -> Arc<BufferPoolManager> {
        Arc::clone(&self.bpm)
    }
}