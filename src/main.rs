use anyhow::Result;
use clap::{Parser, Subcommand};

use syncd::cli::{
    ControlCommand, cmd_add, cmd_control, cmd_edit, cmd_list, cmd_remove, cmd_status, cmd_watch,
};
use syncd::daemon;

#[derive(Parser)]
#[command(
    name = "syncd",
    version,
    about = "Linux daemon and CLI for safe rsync-based sync over SSH"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Add,
    List {
        #[arg(long)]
        json: bool,
    },
    Status {
        project: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Watch,
    Edit {
        project: Option<String>,
    },
    Remove {
        project: Option<String>,
    },
    Start {
        project: Option<String>,
        #[arg(long, conflicts_with = "project")]
        all: bool,
    },
    Stop {
        project: Option<String>,
        #[arg(long, conflicts_with = "project")]
        all: bool,
    },
    Restart {
        project: Option<String>,
        #[arg(long, conflicts_with = "project")]
        all: bool,
    },
    Daemon,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Add => cmd_add(),
        Command::List { json } => cmd_list(json),
        Command::Status { project, json } => cmd_status(project.as_deref(), json),
        Command::Watch => cmd_watch(),
        Command::Edit { project } => cmd_edit(project.as_deref()),
        Command::Remove { project } => cmd_remove(project.as_deref()),
        Command::Start { project, all } => {
            cmd_control(ControlCommand::Start, project.as_deref(), all)
        }
        Command::Stop { project, all } => {
            cmd_control(ControlCommand::Stop, project.as_deref(), all)
        }
        Command::Restart { project, all } => {
            cmd_control(ControlCommand::Restart, project.as_deref(), all)
        }
        Command::Daemon => run_daemon(),
    }
}

fn run_daemon() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(daemon::run())?;
    Ok(())
}
