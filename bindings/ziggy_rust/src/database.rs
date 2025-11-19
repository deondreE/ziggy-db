use crate::ffi;
use std::fmt;
use std::ptr;
use std::str;

#[derive(Debug)]
pub enum DbError {
    CreationFailed,
    OperationFailed,
    Utf8Error,
    BufferTooSmall,
    InvalidArgument,
    FfiError(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::CreationFailed => write!(f, "Database creation failed"),
            DbError::OperationFailed => write!(f, "Database operation failed"),
            DbError::Utf8Error => write!(f, "Invalid UTF-8"),
            DbError::BufferTooSmall => {
                write!(f, "Provided buffer was too small to retrieve data")
            }
            DbError::InvalidArgument => write!(f, "Invalid argument passed to FFI function"),
            DbError::FfiError(msg) => write!(f, "FFI error: {}", msg),
        }
    }
}
impl std::error::Error for DbError {}

pub struct Database {
    handle: *mut ffi::DatabaseHandle,
}

impl Database {
    /// Create a new database connection
    pub fn open(path: &str) -> Result<Self, DbError> {
        unsafe {
            let bytes = path.as_bytes();
            let ptr = ffi::db_create(bytes.as_ptr(), bytes.len());
            if ptr.is_null() {
                Err(DbError::CreationFailed)
            } else {
                Ok(Database { handle: ptr })
            }
        }
    }

    /// Insert or update key/value pair.
    pub fn set(&self, key: &str, val: &str) -> Result<(), DbError> {
        unsafe {
            let k = key.as_bytes();
            let v = val.as_bytes();
            let result = ffi::db_set(self.handle, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            if result == 0 {
                Ok(())
            } else {
                Err(DbError::OperationFailed)
            }
        }
    }

    /// Retrieve a value as a string.
    pub fn get(&self, key: &str) -> Result<Option<String>, DbError> {
        unsafe {
            let k = key.as_bytes();
            let mut buf_size = 256;
            let mut buf = Vec::with_capacity(buf_size);
            buf.set_len(buf_size);

            loop {
                // n:
                // >0: bytes written to buf
                // 0: key not found
                // -1: generic error
                // -2: buffer too small
                let n = ffi::db_get(
                    self.handle,
                    k.as_ptr(),
                    k.len(),
                    buf.as_mut_ptr(),
                    buf.len(),
                );

                if n == 0 {
                    return Ok(None);
                } else if n == -1 {
                    return Err(Self::get_ffi_error(DbError::OperationFailed));
                }
            }
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        unsafe {
            if !self.handle.is_null() {
                ffi::db_close(self.handle);
                self.handle = ptr::null_mut();
            }
        }
    }
}
