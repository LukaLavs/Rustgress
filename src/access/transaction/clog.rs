use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::common::types::TransactionId;

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
/// InProgress, Commited or Aborted.
pub enum XidStatus {
    InProgress = 0x00,
    Committed  = 0x01,
    Aborted    = 0x02,
}

/// Commit Log (CLOG) tracks the status of transactions in compact form.
pub struct CLog { // as of now it is sotred in RAM (it does't have buffer pool management).
    pub data: Vec<u8>, // each byte stores the status of 4 transactions
    file_path: String, // sequence of bytes, where each 2 bits represent status of one transaction
}

impl CLog {
    /// Open or create a CLOG file and load its content into memory.
    pub fn open(path: &str) -> Self {
        let mut data = Vec::new();
        if Path::new(path).exists() {
            let mut file = File::open(path).expect("Ni mogoče odpreti CLOG datoteke");
            file.read_to_end(&mut data).unwrap();
        } else {
            data = vec![0; 65536]; // room for 256k transactions, can be expanded dynamically
        }
        Self { data, file_path: path.to_string(), }
    }

    pub fn get_status(&self, xid: u32) -> XidStatus {
        if xid == 0 { return XidStatus::Committed; } // 0 is system transactions, always considered committed
        let byte_idx = (xid / 4) as usize;
        let bit_shift = (xid % 4) * 2;
        if byte_idx >= self.data.len() {
            return XidStatus::InProgress;
        }
        let status_bits = (self.data[byte_idx] >> bit_shift) & 0x03;
        match status_bits {
            0x01 => XidStatus::Committed,
            0x02 => XidStatus::Aborted,
            _ => XidStatus::InProgress,
        }
    }

    /// Set the status of a transaction (e.g., on COMMIT).
    pub fn set_status(&mut self, xid: TransactionId, status: XidStatus) {
        let byte_idx = (xid / 4) as usize;
        let bit_shift = (xid % 4) * 2;
        if byte_idx >= self.data.len() {
            self.data.resize(byte_idx + 1, 0);
        }
        self.data[byte_idx] &= !(0x03 << bit_shift);
        self.data[byte_idx] |= (status as u8) << bit_shift;
    }

    /// Save CLOG data back to disk.
    pub fn flush(&self) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)
            .expect("Error opening CLOG file.");
        file.write_all(&self.data).unwrap();
        file.sync_all().unwrap();
    }
}
