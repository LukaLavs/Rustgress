use crate::utils::debug::errors::{AccessError, LockError};
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
use crate::access::transaction::context::{get_current_xid, set_thread_error};

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
    fn unpack_static<T, E>(res: Result<T, E>) -> Option<T> 
    where
        E: Into<AccessError>,
    {
        res.ok().map(|val| val)
    }
}

impl HeapScan {
    pub fn new(bpm: Arc<BufferPoolManager>, table: Arc<RwLock<Table>>, tm: Arc<TransactionManager>) -> Result<Self, LockError> {
        let snapshot = tm.get_snapshot()?;
        Ok(Self {
            bpm,
            table,
            tm,
            snapshot,
            current_page_idx: 0,
            current_slot_idx: 1,
            active_frame: None,
            visibility_cache: RefCell::new(HashMap::new()),
            current_xid: None,
        })
    }
    pub fn add_current_xid(&mut self, xid: TransactionId) {
        self.current_xid = Some(xid);
    }
    pub fn drop_current_xid(&mut self) {
        self.current_xid = None;
    }
    fn is_xid_visible(&self, xid: TransactionId) -> Result<bool, LockError> {
        if xid < USER_XID_START { return Ok(true); } // system transactions are always visible
        if Some(xid) == self.current_xid {
            return Ok(true); // own uncommitted transaction is visible to itself
        }
        if xid == get_current_xid() {
            return Ok(true); // own transactions and system transactions are always visible
        }
        {
            let cache = self.visibility_cache.borrow();
            if let Some(&visible) = cache.get(&xid) {
                return Ok(visible);
            } // check own cache first
        }
        let visible = self.tm.is_visible(xid, &self.snapshot)?; // ask transaction manager
        self.visibility_cache.borrow_mut().insert(xid, visible);
        Ok(visible)
    }
}

impl<'a> Iterator for HeapScan {
    type Item = HeapTuple;

    fn next(&mut self) -> Option<Self::Item> {
        // since Iterator can not propagate errors, the potential critical error is stored in thread shared memory
        // this error is then checked before transaction commit and gets recovered with restart.
        let num_pages = {
            let Ok(table) = self.table.read() else {
                set_thread_error(AccessError::Lock(LockError));
                return None;
            };
            let Ok(pages) = table.num_pages() else {
                set_thread_error(AccessError::Lock(LockError)); // ali ustrezen error
                return None;
            };
            pages 
        };
        while self.current_page_idx < num_pages {
            // load the page
            if self.active_frame.is_none() {
                let Some(table_guard) = Self::unpack_static(self.table.read().map_err(|_| LockError)) 
                    else { set_thread_error(AccessError::Lock(LockError)); return None; };
                let table_oid = table_guard.oid;
                drop(table_guard);
                let tag = BufferTag {
                    table_oid: table_oid, // critical error if lock is poisoned.
                    page_idx: self.current_page_idx,
                };
                let Some(mut table_write) = Self::unpack_static(self.table.write().map_err(|_| LockError)) 
                    else { set_thread_error(AccessError::Lock(LockError)); return None; };                
                let Some(fetched_page) = Self::unpack_static(self.bpm.fetch_page(tag, &mut table_write)) 
                    else { set_thread_error(AccessError::Lock(LockError)); return None; };
                drop(table_write);
                self.active_frame = Some(fetched_page);
            };
            let frame = self.active_frame.as_ref().unwrap(); // critical
            let Some(data_lock) = Self::unpack_static(frame.data.read().map_err(|_| LockError)) 
                else { set_thread_error(AccessError::Lock(LockError)); return None; };            
            let Some(page) = Self::unpack_static(Page::from_bytes(&data_lock.data)) 
                else { set_thread_error(AccessError::Lock(LockError)); return None; };
            let num_slots = match page.get_header() {
                Ok(h) => h.num_slots(),
                Err(e) => {
                    set_thread_error(AccessError::Page(e));
                    return None;
                }
            };

            // iterate through slots
            while self.current_slot_idx <= num_slots {
                let slot = self.current_slot_idx;
                self.current_slot_idx += 1;
                if let Some(raw_tuple_bytes) = page.get_item(slot) {
                    let mut view = HeapTupleView::new(raw_tuple_bytes);
                    let t_xmax = view.header.t_xmax;
                    let Some(xid_visible_max) = Self::unpack_static(self.is_xid_visible(t_xmax as TransactionId)) 
                        else { set_thread_error(AccessError::Lock(LockError)); return None; };
                    let Some(xid_visible_min) = Self::unpack_static(self.is_xid_visible(view.header.t_xmin as TransactionId)) 
                        else { set_thread_error(AccessError::Lock(LockError)); return None; };
                    let is_deleted_and_visible = t_xmax != 0 && xid_visible_max;
                    let is_visible = !is_deleted_and_visible && xid_visible_min;
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
