use libc::{c_int}; 

#[repr(C)]
pub struct DatabaseHandle {
    _private: [u8; 0],
}

unsafe extern "C" {
    pub fn db_create(path_ptr: *const u8, path_len: usize) -> *mut DatabaseHandle;
    pub fn db_close(handle: *mut DatabaseHandle);
    pub fn db_set( 
        handle: *mut DatabaseHandle,
        key_ptr: *const u8,
        key_len: usize,
        val_ptr: *const u8,
        val_len: usize,
    ) -> c_int;
    pub fn db_get(
        handle: *mut DatabaseHandle, 
        key_ptr: *const u8,
        key_len: usize,
        out_buf: *mut u8,
        out_buf_len: usize
    ) -> usize;
 }