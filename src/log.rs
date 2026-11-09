//! Minimal leveled logger, compatible with the original RPS `-l 0..8` levels.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU8, Ordering};

pub const STDERR: u8 = 0;
pub const EMERG: u8 = 1;
pub const ALERT: u8 = 2;
pub const CRIT: u8 = 3;
pub const ERR: u8 = 4;
pub const WARN: u8 = 5;
pub const NOTICE: u8 = 6;
pub const INFO: u8 = 7;
pub const DEBUG: u8 = 8;

const NAMES: [&str; 9] = [
    "stderr", "emerg", "alert", "crit", "error", "warn", "notice", "info", "debug",
];

static LEVEL: AtomicU8 = AtomicU8::new(DEBUG);

pub fn set_level(level: u8) {
    LEVEL.store(level.min(DEBUG), Ordering::Relaxed);
}

pub fn level() -> u8 {
    LEVEL.load(Ordering::Relaxed)
}

pub fn log(level: u8, message: impl AsRef<str>) {
    if LEVEL.load(Ordering::Relaxed) < level {
        return;
    }
    let name = NAMES[level as usize];
    eprintln!("[{}] {}", name, message.as_ref());
}

pub fn emerg(message: impl AsRef<str>) {
    log(EMERG, message);
}
pub fn alert(message: impl AsRef<str>) {
    log(ALERT, message);
}
pub fn crit(message: impl AsRef<str>) {
    log(CRIT, message);
}
pub fn error(message: impl AsRef<str>) {
    log(ERR, message);
}
pub fn warn(message: impl AsRef<str>) {
    log(WARN, message);
}
pub fn notice(message: impl AsRef<str>) {
    log(NOTICE, message);
}
pub fn info(message: impl AsRef<str>) {
    log(INFO, message);
}
pub fn debug(message: impl AsRef<str>) {
    log(DEBUG, message);
}
