use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use crate::access::transaction::clog::{CLog, XidStatus};
use crate::common::types::CLOG_FILE_PATH;

pub type TransactionId = u64;

pub struct TransactionManager { 
    next_xid: AtomicU64,
    active_xids: RwLock<HashSet<TransactionId>>, // list of active transaction IDs
    clog: RwLock<CLog>, // commit log to track transaction statuses
}

impl TransactionManager {
    pub fn new(start_xid: TransactionId) -> Self {
        Self {
            next_xid: AtomicU64::new(start_xid),
            active_xids: RwLock::new(HashSet::new()),
            clog: RwLock::new(CLog::open(CLOG_FILE_PATH)),
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
            // clog.flush(); // TODO: decide when to flush CLOG to disk for durability
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

pub struct Snapshot {
    max_xid: TransactionId, // first XID that will be assigned to a new transaction
    active_at_start: HashSet<TransactionId>, // transactions active at the start of the snapshot
}
