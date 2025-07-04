use anyhow::{bail, Result};
use sqlite_rust::{execute_command, run_tui};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    
    match args.len() {
        0 | 1 => bail!("Missing <database path>. Usage: {} <database_path> [command]", args.get(0).unwrap_or(&"program".to_string())),
        2 => {
            // Only database path provided - launch TUI
            let database_path = &args[1];
            println!("Launching SQLite TUI for database: {}", database_path);
            run_tui(database_path.to_string())
        }
        _ => {
            // Database path and command provided - use CLI mode
            let database_path = &args[1];
            let command = &args[2];
            execute_command(database_path, command)
        }
    }
}
