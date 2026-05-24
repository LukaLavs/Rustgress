use std::sync::{Arc, RwLock};
use crate::common::constants::USER_XID_START;
use crate::common::types::TransactionId;
use crate::storage::buffer::manager::{BufferPoolManager, BufferTag, BufferFrame};
use crate::storage::disk::manager::Table;
use crate::storage::page::page::Page;
use crate::access::tuple::tuple::{HeapTuple, HeapTupleView};
use super::super::transaction::manager::{TransactionManager, Snapshot};
use std::collections::HashMap;
use std::cell::RefCell;
use crate::access::transaction::context::{get_current_xid};

pub struct HeapScan {
    bpm: Arc<BufferPoolManager>, // pointer to buffer pool manager
    table: Arc<RwLock<Table>>,   // Updates will happen via buffer pool RwLocks
    tm: Arc<TransactionManager>, // pointer to transaction manager for visibility checks
    snapshot: Snapshot,          // snapshot of transaction state at the start of the scan
    current_page_idx: u32,       // page currently viewed by HeapScan
    current_slot_idx: u16,       // row number in currently viewed page
    active_frame: Option<Arc<BufferFrame>>, // current page pinned in RAM
    visibility_cache: RefCell<HashMap<TransactionId, bool>>, // cache of transaction visibility results to avoid repeated checks
    current_xid: Option<TransactionId>, // sometimes scan must also see its own uncommited transaction
}

impl HeapScan {
    pub fn new(bpm: Arc<BufferPoolManager>, table: Arc<RwLock<Table>>, tm: Arc<TransactionManager>) -> Self {
        let snapshot = tm.get_snapshot();
        Self {
            bpm,
            table,
            tm,
            snapshot,
            current_page_idx: 0,
            current_slot_idx: 1,
            active_frame: None,
            visibility_cache: RefCell::new(HashMap::new()),
            current_xid: None,
        }
    }
    pub fn add_current_xid(&mut self, xid: TransactionId) {
        self.current_xid = Some(xid);
    }
    pub fn drop_current_xid(&mut self) {
        self.current_xid = None;
    }
    pub fn is_xid_visible(&self, xid: TransactionId) -> bool {
        if xid < USER_XID_START { return true; } // system transactions are always visible
        if Some(xid) == self.current_xid {
            return true; // own uncommitted transaction is visible to itself
        }
        if xid == get_current_xid() {
            return true; // own transactions and system transactions are always visible
        }
        {
            let cache = self.visibility_cache.borrow();
            if let Some(&visible) = cache.get(&xid) {
                return visible;
            } // check own cache first
        }
        let visible = self.tm.is_visible(xid, &self.snapshot); // ask transaction manager
        self.visibility_cache.borrow_mut().insert(xid, visible);
        visible
    }
}

impl<'a> Iterator for HeapScan {
    type Item = HeapTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let num_pages = self.table.read().unwrap().num_pages();

        while self.current_page_idx < num_pages {
            // load the page
            if self.active_frame.is_none() {
                let tag = BufferTag {
                    table_oid: self.table.read().unwrap().oid,
                    page_idx: self.current_page_idx,
                };
                let mut table_write = self.table.write().unwrap();
                self.active_frame = Some(self.bpm.fetch_page(tag, &mut table_write));
            }
            let frame = self.active_frame.as_ref().unwrap();
            let data_lock = frame.data.read().unwrap();
            let page = Page::from_bytes(&data_lock.data);
            let num_slots = page.get_header().num_slots();

            // iterate through slots
            while self.current_slot_idx <= num_slots {
                let slot = self.current_slot_idx;
                self.current_slot_idx += 1;
                if let Some(raw_tuple_bytes) = page.get_item(slot) {
                    let mut view = HeapTupleView::new(raw_tuple_bytes);
                    let t_xmax = view.header.t_xmax;
                    let is_deleted_and_visible = t_xmax != 0 && self.is_xid_visible(t_xmax as TransactionId);
                    let is_visible = !is_deleted_and_visible && self.is_xid_visible(view.header.t_xmin as TransactionId);
                    if is_visible {
                        view.header.t_ctid_page = self.current_page_idx;
                        view.header.t_ctid_slot = slot;
                        return Some(view.to_tuple());
                    }
                }
            }
            // finnished with this page, move onto the next
            let buf_id = frame.id;
            drop(data_lock); // drop lock to enable unpinning
            self.bpm.unpin_page(buf_id);
            self.active_frame = None;
            self.current_page_idx += 1;
            self.current_slot_idx = 1;
        }

        None
    }
}

impl Drop for HeapScan {
    fn drop(&mut self) {
        if let Some(frame) = &self.active_frame {
            self.bpm.unpin_page(frame.id);
        }
    }
}
