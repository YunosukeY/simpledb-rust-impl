pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub const MAX_WAIT_TIME_MILLIS: u128 = 10_000;

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
