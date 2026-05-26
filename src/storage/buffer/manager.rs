use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::storage::disk::manager::Table;
use crate::storage::page::page::Page;
use crate::storage::page::checksum::{PageChecksumExt};
use crate::utils::debug::errors::{BufferPoolError};
use std::collections::hash_map::Entry;
use crate::utils::debug::errors::LockError;

pub type BufferId = usize;

#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
/// Equivalent of an "ID card" for BufferPoolManager.
pub struct BufferTag {
    pub table_oid: u32, // table unique identifier
    pub page_idx: u32,  // page number
}

/// Box like structure holding the page bytes and offering some control buttons.
pub struct BufferFrame {
    pub id: BufferId,
    pub data: RwLock<Page>,     // multiple entities can view page data, only one can write
    pub pin_count: Mutex<u32>,  // number of entities reading this page
    pub is_dirty: Mutex<bool>,  // specifies if we wrote on that page, if so we must write on disk
    pub usage_bit: Mutex<bool>, // was the page used recently?
    pub tag: Mutex<Option<BufferTag>>, // specifies table and page number
}

/// Main object which coordinates cached pages in BufferFrames.
pub struct BufferPoolManager {
    frames: Vec<Arc<BufferFrame>>, // list of avalible cached data
    page_table: RwLock<HashMap<BufferTag, BufferId>>, // dictonary which maps table and page to BufferId
    free_list: Mutex<Vec<BufferId>>, // list of free (empty) BufferFrames
    clock_hand: AtomicUsize, // number pointing on BufferFrame which Clock algorithm will check first
}

impl BufferPoolManager {

    pub fn new(num_frames: usize) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        let mut free_list = Vec::with_capacity(num_frames);
        for i in 0..num_frames {
            frames.push(Arc::new(BufferFrame {
                id: i,
                data: RwLock::new(Page::empty()),
                pin_count: Mutex::new(0),
                is_dirty: Mutex::new(false),
                usage_bit: Mutex::new(false),
                tag: Mutex::new(None),
            }));
            free_list.push(i);
        }
        Self {
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            clock_hand: AtomicUsize::new(0),
        }
    }

    pub(crate) fn fetch_page(&self, tag: BufferTag, table: &mut Table) -> Result<Arc<BufferFrame>, BufferPoolError> {
        // check if table was read already
        let existing_buf_id = {
            let table_read = self.page_table.read()
                .map_err(|_| LockError)?;
            table_read.get(&tag).copied()
        };
        if let Some(buf_id) = existing_buf_id {
            let frame = Arc::clone(&self.frames[buf_id]);
            {
                let mut pins = frame.pin_count.lock()
                    .map_err(|_| LockError)?;
                let mut usage = frame.usage_bit.lock()
                    .map_err(|_| LockError)?;
                *pins += 1;
                *usage = true; 
            } // locks on pins and usage automatically drop
            return Ok(frame);
        }
        let buf_id = self.find_replacement_frame()?;
        let frame = Arc::clone(&self.frames[buf_id]);
        self.flush_if_dirty(buf_id, table)?;

        { // set tag, update the hash map
            let mut frame_tag_lock = frame.tag.lock()
                .map_err(|_| LockError)?;
            let old_tag = *frame_tag_lock;
            *frame_tag_lock = Some(tag);
            let mut table_write = self.page_table.write()
                .map_err(|_| LockError)?;
            if let Some(ot) = old_tag {
                table_write.remove(&ot);
            }
            table_write.insert(tag, buf_id);
        }
        { // load page from disk
            let mut page_lock = frame.data.write()
                .map_err(|_| LockError)?;
            let raw_data = table.read_page_raw(tag.page_idx)?;
            page_lock.data.copy_from_slice(&raw_data);
            if !page_lock.checksum_verified()? {
                return Err(BufferPoolError::ChecksumFailed { page_id: tag.page_idx, table_oid: tag.table_oid });
            }
        }
        {
            let mut pins = frame.pin_count.lock().map_err(|_| LockError)?;
            let mut usage = frame.usage_bit.lock().map_err(|_| LockError)?;
            let mut dirty = frame.is_dirty.lock().map_err(|_| LockError)?;
            *pins = 1;
            *usage = true;
            *dirty = false;
        }

        Ok(frame)
    }

    /// Implements [Clock Algorithm](https://cs.carleton.edu/faculty/dmusican/cs334w10/bufmgr/bufmgr.html)
    /// which is an improvement compared to FIFO, MRU and LRU systems.
    fn find_replacement_frame(&self) -> Result<BufferId, BufferPoolError> {
        // if any is free return its id
        let mut free = self.free_list.lock().map_err(|_| LockError)?;
        if let Some(id) = free.pop() {
            return Ok(id);
        }
        drop(free);

        // CLOCK ALGORITHM
        let num_frames = self.frames.len();
        loop {
            let curr_hand = self.clock_hand.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |h| {
                Some((h + 1) % num_frames)
            }).unwrap_or(0); // gets and updates current clock_hand
            let frame = &self.frames[curr_hand];
            let pins = frame.pin_count.lock()
                .map_err(|_| LockError)?;
            if *pins > 0 {
                continue; // if someone is still reading we dont take the frame
            }
            let mut usage = frame.usage_bit.lock()
                .map_err(|_| LockError)?;
            if *usage {
                *usage = false; // if it was recently used we give it another chance
            } else {
                return Ok(curr_hand);
            }
        }
    }

    pub(crate) fn unpin_page(&self, buf_id: BufferId) -> Result<(), BufferPoolError> {
        let frame = &self.frames[buf_id];
        let mut pins = frame.pin_count.lock()
            .map_err(|_| LockError)?;
        if *pins > 0 {
            *pins -= 1;
        }
        Ok(())
    }

    pub(crate) fn mark_dirty(&self, buf_id: BufferId) -> Result<(), BufferPoolError> {
        let frame = &self.frames[buf_id];
        let mut dirty = frame.is_dirty.lock()
            .map_err(|_| LockError)?;
        *dirty = true;
        Ok(())
    }

    fn flush_if_dirty(&self, buf_id: BufferId, table: &mut Table) -> Result<(), BufferPoolError> {
        let frame = &self.frames[buf_id];
        let mut dirty = frame.is_dirty.lock()
            .map_err(|_| LockError)?;
        if *dirty {
            let tag_lock = frame.tag.lock()
                .map_err(|_| LockError)?;
            if let Some(tag) = *tag_lock {
                let mut data_write = frame.data.write()
                    .map_err(|_| LockError)?;
                data_write.update_checksum(); // update checksum before writing
                table.write_page_raw(tag.page_idx, &data_write.data);
                *dirty = false;
                println!("FLUSSHED.");
            }
        }
        Ok(())
    }
}


impl BufferPoolManager {
    /// Writes all dirty pages from all frames to disk, using a local cache of tables to avoid repeated file openings.
    pub fn flush_all(&self) -> Result<(), BufferPoolError> {
        let page_table = self.page_table.read().map_err(|_| LockError)?;
        let mut table_cache: HashMap<u32, Table> = HashMap::new();

        for (tag, &buf_id) in page_table.iter() {
            let frame = &self.frames[buf_id];
            let mut is_dirty = frame.is_dirty.lock().map_err(|_| LockError)?;
            if !*is_dirty {
                continue;
            }
            // Get table from cache or open if not present
            let table = match table_cache.entry(tag.table_oid) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(Table::open(tag.table_oid)?),
            };
            let data_lock = frame.data.read()
                .map_err(|_| LockError)?;
            table.write_page_raw(tag.page_idx, &data_lock.data);
            *is_dirty = false;
        }
        println!("FLUSSHED ALL.");
        Ok(())
    }
}

impl BufferPoolManager {
    /// Popolnoma odstrani vse strani določene tabele iz pomnilnika (uporabno ob DROP TABLE).
    pub fn evict_table_pages(&self, table_oid: u32) -> Result<(), BufferPoolError> {
        // 1. Najprej zaklenemo celotno tabelo strani za pisanje
        let mut page_table = self.page_table.write()
            .map_err(|_| LockError)?;
        let mut free_list = self.free_list.lock()
            .map_err(|_| LockError)?;

        // Poiščemo vse tage, ki pripadajo tej tabeli
        let tags_to_remove: Vec<BufferTag> = page_table
            .keys()
            .filter(|tag| tag.table_oid == table_oid)
            .copied()
            .collect();

        for tag in tags_to_remove {
            // Odstranimo iz HashMap-a in dobimo BufferId okvirja
            if let Some(buf_id) = page_table.remove(&tag) {
                let frame = &self.frames[buf_id];
                
                // Ponastavimo okvir, da bo čist in pripravljen za druge tabele
                let mut tag_lock = frame.tag.lock()
                    .map_err(|_| LockError)?;
                *tag_lock = None;

                let mut dirty_lock = frame.is_dirty.lock()
                    .map_err(|_| LockError)?;
                *dirty_lock = false; // Ker je tabela dropana, sprememb ne želimo pisati na disk!

                let mut usage_lock = frame.usage_bit.lock()
                    .map_err(|_| LockError)?;
                *usage_lock = false;
                // Preverimo pin count za vsak slučaj (ob dropu bi moral biti 0, razen če nekdo ravno bere)

                let pins = frame.pin_count.lock()
                    .map_err(|_| LockError)?;
                if *pins > 0 {
                    println!("[WARNING] Dropping a table while pages are still pinned!");
                }
                // Okvir vrnemo na seznam prostih okvirjev
                free_list.push(buf_id);
            }
        }
        println!("[BPM] Vse strani za tabelo {} so bile uspešno izbrisane iz bufferja.", table_oid);
        Ok(())
    }
}
