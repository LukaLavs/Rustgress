use std::sync::{Arc, RwLock};
use crate::storage::buffer::manager::{BufferPoolManager, BufferTag};
use crate::storage::disk::manager::Table;
use crate::access::tuple::tuple::{HeapTupleView};
use super::super::transaction::manager::{TransactionManager, Snapshot};
use crate::access::tuple::hpx::HeapPageExt;
use crate::access::tuple::header::HeapTupleHeaderData;
use crate::access::tuple::header::TupleInfoMask;
use crate::access::transaction::clog::XidStatus;
use crate::storage::page::item::item_id_flags;

pub struct Vacuum {
    bpm: Arc<BufferPoolManager>,
    tm: Arc<TransactionManager>,
}

#[derive(Debug)]
enum VacuumAction {
    UpdateHints { to_add: TupleInfoMask, to_remove: TupleInfoMask, mark_dead: bool },
    MarkDead,
    None,
}

impl Vacuum {
    pub fn new(bpm: Arc<BufferPoolManager>, tm: Arc<TransactionManager>) -> Self {
        Self { bpm, tm }
    }

    /// Main method that runs vacuum over an entire table. It performs two passes:
    /// 1. Pass: Sets hint bits and marks tuples as LP_DEAD if they are no longer visible to any transaction.
    /// 2. Pass: Converts LP_DEAD slots to LP_UNUSED and compacts the page.
    pub fn vacuum_table(&self, table_oid: u32, table: Arc<RwLock<Table>>) {
        let num_pages = table.read().unwrap().num_pages();
        let horizon = self.tm.get_snapshot();
        // 1. Pass: Set hint bits and mark tuples as LP_DEAD if they are no longer visible to any transaction.
        for i in 0..num_pages {
            self.first_pass_page(table_oid, i, &horizon, table.clone());
        }
        // 2. Pass: Convert LP_DEAD slots to LP_UNUSED and compact the page.
        for i in 0..num_pages {
            self.second_pass_page(table_oid, i, table.clone());
        }
    }
    /// Frist pass: check visibility, set hint bits, and mark tuples as LP_DEAD if they are no longer visible to any transaction.
    fn first_pass_page(&self, table_oid: u32, page_idx: u32, horizon: &Snapshot, table: Arc<RwLock<Table>>) {
        let tag = BufferTag { table_oid, page_idx };
        let mut t_lock = table.write().unwrap();
        let frame = self.bpm.fetch_page(tag, &mut t_lock);
        let mut page_lock = frame.data.write().unwrap();
        
        let num_slots = page_lock.get_header().num_slots();
        let mut changed = false;

        for slot in 1..=num_slots {
            let action = if let Some(raw_bytes) = page_lock.get_item(slot) {
                let view = HeapTupleView::new(raw_bytes);
                self.decide_vacuum_action(&view.header, view.header.read_infomask(), horizon)
            } else {
                VacuumAction::None
            };

            match action {
                VacuumAction::UpdateHints { to_add, to_remove, mark_dead } => {
                    page_lock.heap_update_infomask(slot, to_add, to_remove);
                    if mark_dead {
                        page_lock.get_item_id_mut(slot).unwrap().set_lp_flags(item_id_flags::LP_DEAD);
                    }
                    changed = true;
                },
                VacuumAction::MarkDead => {
                    page_lock.get_item_id_mut(slot).unwrap().set_lp_flags(item_id_flags::LP_DEAD);
                    changed = true;
                },
                VacuumAction::None => {}
            }
        }

        if changed {
            // Phisical removal of dead tuples (those with LP_DEAD flag) and compaction of the item list are done in one pass to minimize overhead.
            page_lock.compact_page();
            self.bpm.mark_dirty(frame.id);
        }
        self.bpm.unpin_page(frame.id);
    }

    /// Second pass: Convert LP_DEAD slots to LP_UNUSED and compact the page.
    fn second_pass_page(&self, table_oid: u32, page_idx: u32, table: Arc<RwLock<Table>>) {
        let tag = BufferTag { table_oid, page_idx };
        let mut t_lock = table.write().unwrap();
        let frame = self.bpm.fetch_page(tag, &mut t_lock);
        let mut page_lock = frame.data.write().unwrap();
        
        let mut changed = false;
        let num_slots = page_lock.get_header().num_slots();

        for slot in 1..=num_slots {
            if let Some(item_id) = page_lock.get_item_id_mut(slot) {
                if item_id.lp_flags() == item_id_flags::LP_DEAD {
                    item_id.set_unused();
                    changed = true;
                }
            }
        }
        if changed {
            page_lock.truncate_items();
            self.bpm.mark_dirty(frame.id);
        }
        self.bpm.unpin_page(frame.id);
    }

    fn decide_vacuum_action(&self, header: &HeapTupleHeaderData, mask: TupleInfoMask, horizon: &Snapshot) -> VacuumAction {
        let mut to_add = TupleInfoMask::empty();
        let clog = self.tm.clog.read().unwrap();

        // XMIN hint bits
        if !mask.intersects(TupleInfoMask::HEAP_XMIN_COMMITTED | TupleInfoMask::HEAP_XMIN_INVALID) {
            match clog.get_status(header.t_xmin) {
                XidStatus::Committed => to_add.insert(TupleInfoMask::HEAP_XMIN_COMMITTED),
                XidStatus::Aborted => to_add.insert(TupleInfoMask::HEAP_XMIN_INVALID),
                _ => {}
            }
        }
        // XMAX hint bits
        if header.t_xmax != 0 && !mask.intersects(TupleInfoMask::HEAP_XMAX_COMMITTED | TupleInfoMask::HEAP_XMAX_INVALID) {
            match clog.get_status(header.t_xmax) {
                XidStatus::Committed => to_add.insert(TupleInfoMask::HEAP_XMAX_COMMITTED),
                XidStatus::Aborted => to_add.insert(TupleInfoMask::HEAP_XMAX_INVALID),
                _ => {}
            }
        }
        let current_mask = mask | to_add;
        let is_dead = self.should_reclaim(header, current_mask, horizon);

        if is_dead {
            if !to_add.is_empty() {
                VacuumAction::UpdateHints { to_add, to_remove: TupleInfoMask::empty(), mark_dead: true }
            } else {
                VacuumAction::MarkDead
            }
        } else if !to_add.is_empty() {
            VacuumAction::UpdateHints { to_add, to_remove: TupleInfoMask::empty(), mark_dead: false }
        } else {
            VacuumAction::None
        }
    }

    fn should_reclaim(&self, header: &HeapTupleHeaderData, mask: TupleInfoMask, horizon: &Snapshot) -> bool {
        // If XMIN is aborted, the tuple never existed
        if mask.contains(TupleInfoMask::HEAP_XMIN_INVALID) { return true; }
        // If XMAX is committed and the transaction is older than the horizon (everyone sees it as dead)
        if mask.contains(TupleInfoMask::HEAP_XMAX_COMMITTED) {
            if header.t_xmax < horizon.max_xid { 
                return true; 
            }
        }
        false
    }
}