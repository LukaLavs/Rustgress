use std::cell::{Cell, RefCell};
use crate::common::types::TransactionId;
use crate::utils::debug::errors::AccessError;

thread_local! {
    // Only one thread accesses this at a time. 0 is for InvalidTransactionId, 
    // meaning no active transaction or a regular scan.
    static CURRENT_XID: Cell<TransactionId> = Cell::new(0);
    static CURRENT_ERROR: RefCell<Option<AccessError>> = RefCell::new(None);
}

pub fn set_current_xid(xid: TransactionId) {
    CURRENT_XID.with(|ctx| ctx.set(xid));
}

pub fn get_current_xid() -> TransactionId {
    CURRENT_XID.with(|ctx| ctx.get())
}

pub fn clear_current_xid() {
    CURRENT_XID.with(|ctx| ctx.set(0));
}


pub fn set_thread_error(err: AccessError) { // store only first error encountered in the thread
    CURRENT_ERROR.with(|ctx| {
        let mut error_guard = ctx.borrow_mut();
        if error_guard.is_none() {
            *error_guard = Some(err);
        }
    });
}

pub fn get_thread_error() -> Option<AccessError> {
    CURRENT_ERROR.with(|ctx| {
        ctx.borrow_mut().take() // moves out
    })
}

pub fn clear_thread_error() {
    CURRENT_ERROR.with(|ctx| {
        *ctx.borrow_mut() = None;
    });
}
