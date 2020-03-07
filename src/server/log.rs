use chrono::Utc;

// To be expanded to log to file
pub struct LogManager {}

impl LogManager {
    pub fn new() -> LogManager {
        LogManager {}
    }

    pub fn write(&self, s: &str) {
        println!("{}: {}", Utc::now().format("%Y-%m-%d %H:%M:%S"), s);
    }
}
