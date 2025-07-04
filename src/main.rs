use anyhow::{bail, Result};
use codecrafters_sqlite::execute_command;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let database_path = &args[1];
    let command = &args[2];

    execute_command(database_path, command)
}
