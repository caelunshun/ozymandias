use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use ozymandias::medium::compressing::{CompressingMedium, CompressionType};
use ozymandias::medium::local::LocalMedium;
use ozymandias::medium::s3::S3Medium;
use ozymandias::medium::Medium;
use ozymandias::model::Version;
use ozymandias::{backup, restore};
use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(short, long)]
    medium: MediumConfig,
    #[arg(short = 'c', long, default_value_t = zstd::DEFAULT_COMPRESSION_LEVEL)]
    compression_level: i32,

    #[arg(long)]
    s3_endpoint_url: Option<String>,
    #[arg(long)]
    bucket: Option<String>,
    #[arg(long)]
    storage_dir: Option<PathBuf>,
    #[arg(long)]
    backup_name: String,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, ValueEnum)]
enum MediumConfig {
    Local,
    S3,
}

#[derive(Debug, Subcommand)]
enum Command {
    Backup(BackupArgs),
    Restore(RestoreArgs),
}

#[derive(Debug, Args)]
struct BackupArgs {
    /// Directory to back up.
    dir: PathBuf,
}

#[derive(Debug, Args)]
struct RestoreArgs {
    /// Directory to restore to.
    dir: PathBuf,
}

pub fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let medium = create_medium(&cli)?;

    match &cli.command {
        Command::Backup(args) => do_backup(&*medium, args)?,
        Command::Restore(args) => do_restore(&*medium, args)?,
    }

    Ok(())
}

fn do_backup(medium: &dyn Medium, args: &BackupArgs) -> anyhow::Result<()> {
    backup::run(backup::Config {
        source_dir: args.dir.clone(),
        medium,
    })
}

fn do_restore(medium: &dyn Medium, args: &RestoreArgs) -> anyhow::Result<()> {
    let version = Version::decode(&medium.load_version(0)?.context("no versions to restore")?[..])?;
    restore::run(medium, &version, &args.dir)
}

fn create_medium(cli: &Cli) -> anyhow::Result<Box<dyn Medium>> {
    match &cli.medium {
        MediumConfig::Local => {
            let medium = LocalMedium::new(
                cli.storage_dir
                    .as_ref()
                    .context("missing target dir for local medium")?,
                &cli.backup_name,
            )?;
            Ok(wrap_medium(medium, cli))
        }
        MediumConfig::S3 => {
            let mut config_loader = aws_config::ConfigLoader::default();
            if let Some(endpoint_url) = &cli.s3_endpoint_url {
                config_loader = config_loader.endpoint_url(endpoint_url);
            }
            let config = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(config_loader.load());
            let medium = S3Medium::new(
                &config,
                cli.bucket
                    .as_ref()
                    .context("missing bucket for S3 medium")?,
                &cli.backup_name,
            );
            Ok(wrap_medium(medium, cli))
        }
    }
}

/// Wraps a medium in the default set of composing mediums.
fn wrap_medium(medium: impl Medium, cli: &Cli) -> Box<dyn Medium> {
    Box::new(CompressingMedium::new(
        medium,
        CompressionType::Zstd {
            compression_level: cli.compression_level,
        },
    ))
}
