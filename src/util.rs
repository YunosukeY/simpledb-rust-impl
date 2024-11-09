#![allow(dead_code)]

use std::sync::{Condvar, Mutex};

use tracing::Level;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub const BOOL_BYTES: i32 = 1;
pub const SHORT_BYTES: i32 = 2;
pub const INTEGER_BYTES: i32 = 4;
pub const DOUBLE_BYTES: i32 = 8;

pub const MAX_WAIT_TIME_MILLIS: u128 = 100;

pub fn current_time_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub fn waiting_too_long(start_time: u128) -> bool {
    let current_time = current_time_millis();
    current_time - start_time > MAX_WAIT_TIME_MILLIS
}

pub fn init_log() {
    tracing_subscriber::fmt()
        .json()
        .with_file(true)
        .with_line_number(true)
        .with_max_level(Level::INFO)
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();
}

pub struct CondMutex<T> {
    m: Mutex<T>,
    cond: Condvar,
}

impl<T> CondMutex<T> {
    pub fn new(data: T) -> Self {
        Self {
            m: Mutex::new(data),
            cond: Condvar::new(),
        }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<T> {
        self.m.lock().unwrap()
    }

    pub fn wait_timeout<'a>(
        &self,
        guard: std::sync::MutexGuard<'a, T>,
        millis: u64,
    ) -> std::sync::MutexGuard<'a, T> {
        let (guard, _) = self
            .cond
            .wait_timeout(guard, std::time::Duration::from_millis(millis))
            .unwrap();
        guard
    }

    pub fn notify_all(&self) {
        self.cond.notify_all();
    }
}
