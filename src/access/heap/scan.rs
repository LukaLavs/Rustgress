use std::sync::Arc;
use crate::storage::buffer::manager::{BufferPoolManager, BufferTag, BufferFrame};
use crate::storage::disk::manager::Table;
use crate::storage::page::layout::Page;
use crate::access::tuple::header::{Tuple, HeapTupleView};
use super::super::transaction::manager::{TransactionManager, Snapshot};
use std::collections::HashMap;
use std::cell::RefCell;
use crate::catalog::schema::Schema;
use crate::catalog::rg_class::RGClass;
use crate::catalog::traits::RGSomething;

pub struct HeapScan<'a> {
    bpm: Arc<BufferPoolManager>, // pointer to buffer pool manager
    table: &'a mut Table, // TODO: make it imutable reference, updates will happen via buffer pool RwLocks
    tm: Arc<TransactionManager>, // pointer to transaction manager for visibility checks
    snapshot: Snapshot,          // snapshot of transaction state at the start of the scan
    current_page_idx: u32,       // page currently viewed by HeapScan
    current_slot_idx: u16,       // row number in currently viewed page
    active_frame: Option<Arc<BufferFrame>>, // current page pinned in RAM
    visibility_cache: RefCell<HashMap<u64, bool>>, // cache of transaction visibility results to avoid repeated checks
}

impl<'a> HeapScan<'a> {
    pub fn new(bpm: Arc<BufferPoolManager>, table: &'a mut Table, tm: Arc<TransactionManager>) -> Self {
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
        }
    }

    pub fn is_xid_visible(&self, xid: u64) -> bool {
        if xid == 0 { return true; } // system transactions are always visible
        {
            let cache = self.visibility_cache.borrow();
            if let Some(&visible) = cache.get(&xid) {
                return visible;
            }
        }
        let visible = self.tm.is_visible(xid, &self.snapshot);
        self.visibility_cache.borrow_mut().insert(xid, visible);
        visible
    }
}

impl<'a> Iterator for HeapScan<'a> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let num_pages = self.table.num_pages();

        while self.current_page_idx < num_pages {
            // load the page
            if self.active_frame.is_none() {
                let tag = BufferTag {
                    table_oid: self.table.oid,
                    page_idx: self.current_page_idx,
                };
                self.active_frame = Some(self.bpm.fetch_page(tag, self.table));
            }
            let frame = self.active_frame.as_ref().unwrap();
            let data_lock = frame.data.read().unwrap();
            let page = Page::from_bytes(&data_lock.data);
            let num_slots = page.get_header().num_slots();

            // iterate through slots
            while self.current_slot_idx <= num_slots {
                let slot = self.current_slot_idx;
                self.current_slot_idx += 1;
                if let Some(raw_tuple_bytes) = page.get_tuple_bytes(slot) {
                    let view = HeapTupleView::new(raw_tuple_bytes);
                    let is_visible = self.is_xid_visible(view.header.t_xmin as u64);
                    if is_visible {
                        return Some(Tuple {
                            header: view.header,
                            null_bitmap: view.null_bitmap().map(|b| b.to_vec()).unwrap_or_default(),
                            data: view.data().to_vec(),
                        });
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

impl<'a> Drop for HeapScan<'a> {
    fn drop(&mut self) {
        if let Some(frame) = &self.active_frame {
            self.bpm.unpin_page(frame.id);
        }
    }
}

impl<'a> HeapScan<'a> {
    pub fn get_table_oid(
        bpm: Arc<BufferPoolManager>, 
        tm: Arc<TransactionManager>,
        class_schema: &Schema, 
        target_table_name: &str
    ) -> Option<u32> {
        let mut rg_class_table = Table::open(RGClass::get_oid());
        let scan = HeapScan::new(bpm, &mut rg_class_table, tm);
        for tuple in scan {
            let values = class_schema.unpack_from_tuple(&tuple);
            // oid is first column, relname is second column in rg_class
            if let Some(name_value) = values.get(1) {
                if name_value.as_str() == target_table_name {
                    // Če se ujema, vrni OID (stolpec 0)
                    if let Some(oid_value) = values.get(0) {
                        return oid_value.as_u32();
                    }
                }
            }
        }
        None // table not found
    }
}
