// To be expanded to log to file

pub struct LogManager {}

impl LogManager {
    pub fn new() -> LogManager {
        LogManager {}
    }

    pub fn write(&self, s: &str) {
        println!("{}", s);
    }
}
