use std::sync::Arc;
use crate::storage::buffer::manager::{BufferPoolManager, BufferTag, BufferFrame};
use crate::storage::disk::manager::Table;
use crate::storage::page::layout::Page;
use crate::access::tuple::header::{Tuple, HeapTupleView};

pub struct HeapScan<'a> {
    bpm: Arc<BufferPoolManager>, // pointer to buffer pool manager
    table: &'a mut Table,
    current_page_idx: u32, // page currently viewed by HeapScan
    current_slot_idx: u16, // row number in currently viewed page
    active_frame: Option<Arc<BufferFrame>>, // current page pinned in RAM
}

impl<'a> HeapScan<'a> {
    pub fn new(bpm: Arc<BufferPoolManager>, table: &'a mut Table) -> Self {
        Self {
            bpm,
            table,
            current_page_idx: 0,
            current_slot_idx: 1,
            active_frame: None,
        }
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

                    // remember: t_max is id of deleting transaction, 0 if alive
                    if view.header.t_xmax == 0 {
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
