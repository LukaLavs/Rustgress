use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use crate::access::transaction::clog::{CLog, XidStatus};
use crate::common::constants::CLOG_FILE_PATH;

pub type TransactionId = u64;

/// TransactionManager is responsible for managing transaction lifecycles, tracking active transactions, 
/// and determining visibility of transactions to ensure ACID properties.
pub struct TransactionManager { 
    next_xid: AtomicU64,
    active_xids: RwLock<HashSet<TransactionId>>, // list of active transaction IDs
    pub clog: RwLock<CLog>, // commit log to track transaction statuses
}

impl TransactionManager {
    pub fn new() -> Self {
        let clog = CLog::open(CLOG_FILE_PATH);
        let next_xid = clog.find_last_xid() + 1;

        println!("[DEBUG] CLOG last_xid found: {}", next_xid - 1);
        Self {
            next_xid: AtomicU64::new(next_xid),
            active_xids: RwLock::new(HashSet::new()),
            clog: RwLock::new(clog),
        }
    }

    /// Begin a new transaction and return its TransactionID (XID).
    pub fn begin(&self) -> TransactionId {
        let xid = self.next_xid.fetch_add(1, Ordering::SeqCst);
        let mut active = self.active_xids.write().unwrap();
        active.insert(xid);
        xid
    }

    /// Mark a transaction as successfully completed.
    pub fn commit(&self, xid: TransactionId) {
        {
            let mut clog = self.clog.write().unwrap();
            clog.set_status(xid, XidStatus::Committed);
            clog.flush(); // TODO: decide when to flush CLOG to disk for durability
        }
        let mut active = self.active_xids.write().unwrap();
        active.remove(&xid);
    }

    /// Cancels a transaction, prevents commit and makes it invisible to others.
    pub fn abort(&self, xid: TransactionId) {
        {
            let mut clog = self.clog.write().unwrap();
            clog.set_status(xid, XidStatus::Aborted);
        }
        let mut active = self.active_xids.write().unwrap();
        active.remove(&xid);
    }

    /// Make CLOG flush its data to disk.
    pub fn flush(&self) {
        let clog = self.clog.read().unwrap();
        clog.flush();
    }

    /// Check if a given XID is visible to a given snapshot.
    pub fn is_visible(&self, xid: TransactionId, snapshot: &Snapshot) -> bool {
        if xid == 0 { return true; } // system transactions are always visible
        if xid >= snapshot.max_xid { return false; }
        if snapshot.active_at_start.contains(&xid) {
            return false;
        }
        let clog = self.clog.read().unwrap();
        clog.get_status(xid) == XidStatus::Committed

    }

    /// Creates current snapshot of the transaction state.
    pub fn get_snapshot(&self) -> Snapshot {
        let active = self.active_xids.read().unwrap();
        Snapshot {
            max_xid: self.next_xid.load(Ordering::SeqCst),
            active_at_start: active.clone(),
        }
    }
}

impl CLog {
    pub fn find_last_xid(&self) -> u64 { // find last non null byte in CLOG.
        // TODO: maybe we could store the last assigned XID in some catalog.
        let last_non_zero_byte = self.data.iter().enumerate().rev().find(|&(_, byte)| *byte != 0);
        if let Some((byte_idx, &byte)) = last_non_zero_byte {
            for i in (0..4).rev() {
                let bit_shift = i * 2;
                let status = (byte >> bit_shift) & 0b11;
                if status != 0 {
                    let last_xid = (byte_idx as u64 * 4) + i as u64;
                    return last_xid;
                }
            }
            return byte_idx as u64 * 4;
        }
        0 // no transactions found.
    }
}
pub struct Snapshot {
    max_xid: TransactionId, // first XID that will be assigned to a new transaction
    active_at_start: HashSet<TransactionId>, // transactions active at the start of the snapshot
}
