// src/access/heap/heap_access.rs

use std::sync::Arc;
use crate::storage::buffer::manager::{BufferPoolManager, BufferTag};
use crate::storage::disk::manager::Table;
use crate::access::tuple::header::Tuple;
use crate::common::types::RowId;
use crate::storage::manager::StorageManager;
use crate::storage::buffer::manager::BufferFrame;
use crate::storage::page::layout::Page;

pub struct HeapAccess;

impl HeapAccess {
    pub fn insert(
        storage: Arc<StorageManager>,
        xid: u64,
        table_oid: u32,
        tuple: &mut Tuple,
    ) -> RowId {
        let bpm = storage.get_bpm();
        let table = storage.get_table(table_oid);

        tuple.header.t_xmin = xid as u32;
        tuple.header.t_xmax = 0; // not deleted yet

        // Try to find space on the last page of the table
        let mut t_lock = table.write().unwrap();
        let num_pages = t_lock.num_pages();
        
        let (frame, page_id) = if num_pages > 0 {
            let last_page_id = num_pages - 1;
            let tag = BufferTag { table_oid, page_idx: last_page_id };
            (bpm.fetch_page(tag, &mut t_lock), last_page_id)
        } else { // empty table, create first page
            Self::append_new_page(bpm.clone(), &mut t_lock, table_oid)
        };

        // try to insert into the current page
        let mut frame_data = frame.data.write().unwrap();    

        let page = Page::from_bytes(&frame_data.data); // TODO: Ugly
        let next_slot = page.get_header().num_slots() + 1;
        tuple.header.t_ctid_page = page_id;
        tuple.header.t_ctid_slot = next_slot;

        if let Some(slot_num) = frame_data.add_tuple(tuple) {
            bpm.mark_dirty(frame.id);
            let rid = RowId { page_id, slot_num };
            drop(frame_data);
            bpm.unpin_page(frame.id);
            return rid;
        }
        // If there is no space on the current page, we need to add a new page
        drop(frame_data); // Izpustimo staro stran
        bpm.unpin_page(frame.id);
        
        let (new_frame, new_page_id) = Self::append_new_page(bpm.clone(), &mut t_lock, table_oid);
        let mut new_frame_data = new_frame.data.write().unwrap();
        
        tuple.header.t_ctid_page = new_page_id; // TODO: Ugly
        tuple.header.t_ctid_slot = 1;

        let slot_num = new_frame_data.add_tuple(tuple)
            .expect("Tuple exceeds page size, cannot be inserted"); // TODO: Toast not implemented.
        bpm.mark_dirty(new_frame.id);
        let rid = RowId { page_id: new_page_id, slot_num };
        drop(new_frame_data);
        bpm.unpin_page(new_frame.id);
        
        rid
    }

    fn append_new_page(
        bpm: Arc<BufferPoolManager>, 
        table: &mut Table, 
        table_oid: u32
    ) -> (Arc<BufferFrame>, u32) {
        let new_page_id = table.extend(0); // TODO: hardcoded for now
        
        let tag = BufferTag { table_oid, page_idx: new_page_id };
        (bpm.fetch_page(tag, table), new_page_id)
    }
} 

impl HeapAccess {    
    pub fn delete(
        storage: Arc<StorageManager>,
        xid: u64,
        table_oid: u32,
        rid: RowId,
    ) -> bool {
        let bpm = storage.get_bpm();
        let table = storage.get_table(table_oid);
        let tag = BufferTag { 
            table_oid, 
            page_idx: rid.page_id 
        };
        let mut t_lock = table.write().unwrap();
        let frame = bpm.fetch_page(tag, &mut t_lock);
        let mut frame_data = frame.data.write().unwrap();
        let success = frame_data.set_xmax(rid.slot_num, xid as u32);
        if success {
            bpm.mark_dirty(frame.id);
        }
        drop(frame_data);
        bpm.unpin_page(frame.id);

        success
    }
}

impl HeapAccess {
    pub fn update(
        storage: Arc<StorageManager>,
        xid: u64,
        table_oid: u32,
        rid: RowId,
        new_tuple: &mut Tuple,
    ) -> RowId {
        // 1. "Izbrišemo" staro vrstico
        // Nastavimo xmax na trenutni xid, da stara verzija postane nevidna po commitu.
        let deleted = Self::delete(storage.clone(), xid, table_oid, rid);
        if !deleted {
            panic!("Update failed: could not find old tuple at {:?}", rid);
        }

        // 2. Vstavimo novo vrstico
        // Ta dobi isti xmin (naš xid), kar pomeni, da postane vidna v istem trenutku,
        // ko stara postane nevidna.
        let new_rid = Self::insert(storage.clone(), xid, table_oid, new_tuple);

        // 3. Povezovanje (Tuple Chaining)
        // V idealnem svetu bi zdaj posodobila 't_ctid' starega tupla, 
        // da kaže na 'new_rid'. To omogoča sledenje verzijam.
        Self::link_tuples(storage, table_oid, rid, new_rid);

        new_rid
    }

    /// Pomožna metoda, ki v glavo starega tupla zapiše lokacijo novega.
    fn link_tuples(storage: Arc<StorageManager>, table_oid: u32, old_rid: RowId, new_rid: RowId) {
        let bpm = storage.get_bpm();
        let table = storage.get_table(table_oid);
        let tag = BufferTag { table_oid, page_idx: old_rid.page_id };
        
        let mut t_lock = table.write().unwrap();
        let frame = bpm.fetch_page(tag, &mut t_lock);
        let mut page = frame.data.write().unwrap();

        // Potrebujemo metodo na Page, ki zna vpisati ctid
        page.set_ctid(old_rid.slot_num, new_rid);
        
        bpm.mark_dirty(frame.id);
        bpm.unpin_page(frame.id);
    }
}