//! 分级日志：提供 0..8 级别，统一输出到 stderr。
#![allow(dead_code)]

use std::sync::atomic::{AtomicU8, Ordering};

// 日志级别：0 静默，1 最严重，8 最详细。
pub const STDERR: u8 = 0;
pub const EMERG: u8 = 1;
pub const ALERT: u8 = 2;
pub const CRIT: u8 = 3;
pub const ERR: u8 = 4;
pub const WARN: u8 = 5;
pub const NOTICE: u8 = 6;
pub const INFO: u8 = 7;
pub const DEBUG: u8 = 8;

// 级别名称，用于日志前缀。
const NAMES: [&str; 9] = [
    "stderr", "emerg", "alert", "crit", "error", "warn", "notice", "info", "debug",
];

// 全局日志级别。
static LEVEL: AtomicU8 = AtomicU8::new(DEBUG);

/// 设置全局日志级别。
pub fn set_level(level: u8) {
    LEVEL.store(level.min(DEBUG), Ordering::Relaxed);
}

/// 返回当前日志级别。
pub fn level() -> u8 {
    LEVEL.load(Ordering::Relaxed)
}

/// 按级别输出日志；低于当前级别的消息会被丢弃。
pub fn log(level: u8, message: impl AsRef<str>) {
    if LEVEL.load(Ordering::Relaxed) < level {
        return;
    }
    let name = NAMES[level as usize];
    eprintln!("[{}] {}", name, message.as_ref());
}

/// 输出 emerg 级别日志。
pub fn emerg(message: impl AsRef<str>) {
    log(EMERG, message);
}
/// 输出 alert 级别日志。
pub fn alert(message: impl AsRef<str>) {
    log(ALERT, message);
}
/// 输出 crit 级别日志。
pub fn crit(message: impl AsRef<str>) {
    log(CRIT, message);
}
/// 输出 error 级别日志。
pub fn error(message: impl AsRef<str>) {
    log(ERR, message);
}
/// 输出 warn 级别日志。
pub fn warn(message: impl AsRef<str>) {
    log(WARN, message);
}
/// 输出 notice 级别日志。
pub fn notice(message: impl AsRef<str>) {
    log(NOTICE, message);
}
/// 输出 info 级别日志。
pub fn info(message: impl AsRef<str>) {
    log(INFO, message);
}
/// 输出 debug 级别日志。
pub fn debug(message: impl AsRef<str>) {
    log(DEBUG, message);
}
