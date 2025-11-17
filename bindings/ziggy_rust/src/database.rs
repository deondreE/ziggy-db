use crate::ffi;
use std::fmt;
use std::ptr;
use std::str;

#[derive(Debug)]
pub enum DbError {
    CreationFailed,
    OperationFailed,
    Utf8Error,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::CreationFailed => write!(f, "Database creation failed"),
            DbError::OperationFailed => write!(f, "Database operation failed"),    
            DbError::Utf8Error => write!(f, "Invalid UTF-8"),
        }
    }
}   
impl std::error::Error for DbError{}

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
            let mut buf = vec![0u8, 255];
            let n = ffi::db_get(self.handle, k.as_ptr(), k.len(), buf.as_mut_ptr(), buf.len());
            if n == 0 {
                return Ok(None);
            }
            let bytes = &buf[..n];
            match String::from_utf8(bytes.to_vec()) {
                Ok(s) => Ok(Some(s)),
                Err(_) => Err(DbError::Utf8Error),
            }
        }
    }
}

impl Drop for Database {
    fn drop (&mut self) {
        unsafe {
            if !self.handle.is_null() {
                ffi::db_close(self.handle);
                self.handle = ptr::null_mut();
            }
        }
    }
}