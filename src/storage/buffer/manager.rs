use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::storage::disk::manager::Table;
use crate::storage::page::layout::Page;

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

    pub fn fetch_page(&self, tag: BufferTag, table: &mut Table) -> Arc<BufferFrame> {
        // check if table was read already
        let existing_buf_id = {
            let table_read = self.page_table.read().unwrap();
            table_read.get(&tag).copied()
        };
        if let Some(buf_id) = existing_buf_id {
            let frame = Arc::clone(&self.frames[buf_id]);
            {
                let mut pins = frame.pin_count.lock().unwrap();
                let mut usage = frame.usage_bit.lock().unwrap();
                *pins += 1;
                *usage = true; 
            } // locks on pins and usage automatically drop
            return frame;
        }
        let buf_id = self.find_replacement_frame();
        let frame = Arc::clone(&self.frames[buf_id]);
        self.flush_if_dirty(buf_id, table);

        { // set tag, update the hash map
            let mut frame_tag_lock = frame.tag.lock().unwrap();
            let old_tag = *frame_tag_lock;
            *frame_tag_lock = Some(tag);
            let mut table_write = self.page_table.write().unwrap();
            if let Some(ot) = old_tag {
                table_write.remove(&ot);
            }
            table_write.insert(tag, buf_id);
        }
        { // load page from disk
            let mut page_lock = frame.data.write().unwrap();
            let raw_data = table.read_page_raw(tag.page_idx);
            page_lock.data.copy_from_slice(&raw_data);
        }
        {
            let mut pins = frame.pin_count.lock().unwrap();
            let mut usage = frame.usage_bit.lock().unwrap();
            let mut dirty = frame.is_dirty.lock().unwrap();
            *pins = 1;
            *usage = true;
            *dirty = false;
        }

        frame
    }

    /// Implements [Clock Algorithm](https://cs.carleton.edu/faculty/dmusican/cs334w10/bufmgr/bufmgr.html)
    /// which is an improvement compared to FIFO, MRU and LRU systems.
    fn find_replacement_frame(&self) -> BufferId {
        // if any is free return its id
        let mut free = self.free_list.lock().unwrap();
        if let Some(id) = free.pop() {
            return id;
        }
        drop(free);

        // CLOCK ALGORITHM
        let num_frames = self.frames.len();
        loop {
            let curr_hand = self.clock_hand.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |h| {
                Some((h + 1) % num_frames)
            }).unwrap(); // gets and updates current clock_hand
            let frame = &self.frames[curr_hand];
            let pins = frame.pin_count.lock().unwrap();
            if *pins > 0 {
                continue; // if someone is still reading we dont take the frame
            }
            let mut usage = frame.usage_bit.lock().unwrap();
            if *usage {
                *usage = false; // if it was recently used we give it another chance
            } else {
                return curr_hand;
            }
        }
    }

    pub fn unpin_page(&self, buf_id: BufferId) {
        let frame = &self.frames[buf_id];
        let mut pins = frame.pin_count.lock().unwrap();
        if *pins > 0 {
            *pins -= 1;
        }
    }

    pub fn mark_dirty(&self, buf_id: BufferId) {
        let frame = &self.frames[buf_id];
        let mut dirty = frame.is_dirty.lock().unwrap();
        *dirty = true;
    }

    fn flush_if_dirty(&self, buf_id: BufferId, table: &mut Table) {
        let frame = &self.frames[buf_id];
        let mut dirty = frame.is_dirty.lock().unwrap();
        if *dirty {
            let tag_lock = frame.tag.lock().unwrap();
            if let Some(tag) = *tag_lock {
                let data_read = frame.data.read().unwrap();
                table.write_page_raw(tag.page_idx, &data_read.data);
                *dirty = false;
                println!("SUCCESS: Buffer {} flushed to disk (page {})", buf_id, tag.page_idx); // TODO: Remove this temp debug println!
            }
        }
    }
}
