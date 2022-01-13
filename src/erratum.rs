use std::thread::JoinHandle;

pub fn join<T>(thread: JoinHandle<T>) -> T {
    match thread.join() {
        Ok(res) => res,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
