#[path = "outbound_progress/dispatch.rs"]
mod dispatch;

fn main() -> Result<(), fastly::Error> {
    dispatch::main(true)
}
