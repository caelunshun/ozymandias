use anyhow::{bail, Context};
use aws_config::AppName;
use clap::{Parser, Subcommand};
use ozymandias::medium::fs::FilesystemMedium;
use ozymandias::medium::s3::S3Medium;
use ozymandias::medium::Medium;
use std::path::PathBuf;
use url::Url;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Backup {
        source: PathBuf,
        #[arg(short, long)]
        target: String,
    },
    Restore {
        source: String,
        #[arg(short, long)]
        target: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Backup { source, target } => backup(source, target),
        Command::Restore { source, target } => restore(source, target),
    }
}

fn backup(source: PathBuf, target: String) -> anyhow::Result<()> {
    let mut medium = medium_from_url(&target)?;
    ozymandias::backup::Driver::new(&mut *medium, source).run()
}

fn restore(source: String, target: PathBuf) -> anyhow::Result<()> {
    let mut medium = medium_from_url(&source)?;
    let driver = ozymandias::restore::Driver::new(&mut *medium, target)?;
    let (revision_id, _) = driver
        .available_revisions()
        .last()
        .context("no revisions available")?;
    driver.run(revision_id)
}

fn medium_from_url(url: &str) -> anyhow::Result<Box<dyn Medium>> {
    let url = Url::parse(url).context("invalid backup location URL")?;

    match url.scheme() {
        "file" => {
            let path = url.path();
            FilesystemMedium::new(path).map(|fs| Box::new(fs) as Box<dyn Medium>)
        }
        "wasabi" => s3_medium_from_url(&url, "https://s3.wasabisys.com")
            .map(|s3| Box::new(s3) as Box<dyn Medium>),
        _ => bail!("invalid URL scheme for backup location, expected file:// or s3://"),
    }
}

fn s3_medium_from_url(url: &Url, endpoint_url: &str) -> anyhow::Result<S3Medium> {
    let bucket = url.domain().context("missing bucket specifier")?;
    let subdirectory = strip_slashes(url.path());
    let loader = aws_config::from_env()
        .app_name(AppName::new("ozymandias")?)
        .endpoint_url(endpoint_url);
    let config = tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(loader.load());
    Ok(S3Medium::new(
        config,
        bucket,
        match subdirectory {
            "" => None,
            s => Some(s),
        },
    ))
}

fn strip_slashes(s: &str) -> &str {
    let s = s.strip_prefix('/').unwrap_or(s);
    s.strip_suffix('/').unwrap_or(s)
}
