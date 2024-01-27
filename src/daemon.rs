//! Daemon application that periodically runs one or more backups.

use anyhow::bail;
use chrono::{Duration, Utc};
use ozymandias::backup;
use ozymandias::medium::Medium;
use ozymandias::model::Version;
use serde::Deserialize;
use std::io::Read;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    backups: Vec<Backup>,
}

impl Config {
    pub fn load(reader: impl Read) -> anyhow::Result<Self> {
        ron::de::from_reader(reader).map_err(anyhow::Error::from)
    }
}

#[derive(Debug, Deserialize)]
struct Backup {
    name: String,
    source_dir: PathBuf,
    interval: Interval,
}

#[derive(Copy, Clone, Debug, Deserialize)]
enum Interval {
    Minutes(u32),
    Hours(u32),
    Days(u32),
}

impl Interval {
    pub fn to_duration(self) -> Duration {
        match self {
            Interval::Minutes(min) => Duration::minutes(min.into()),
            Interval::Hours(hrs) => Duration::hours(hrs.into()),
            Interval::Days(days) => Duration::days(days.into()),
        }
    }
}

pub fn run(
    config: Config,
    mut build_medium_with_backup_name: impl FnMut(&str) -> anyhow::Result<Box<dyn Medium>>,
) -> anyhow::Result<()> {
    if config.backups.is_empty() {
        bail!("no backups configured");
    }

    println!("Daemon starting, managing {} backups", config.backups.len());
    loop {
        if let Err(e) = run_inner(&config, &mut build_medium_with_backup_name) {
            eprintln!("Error occurred: {e}");
            eprintln!("Will restart after 30 seconds.");
            std::thread::sleep(std::time::Duration::from_secs(30));
        }
    }
}

fn run_inner(
    config: &Config,
    mut build_medium_with_backup_name: impl FnMut(&str) -> anyhow::Result<Box<dyn Medium>>,
) -> anyhow::Result<()> {
    loop {
        let mut next_backup = None;
        let mut sleep_time = Duration::zero();
        for backup in &config.backups {
            let medium = build_medium_with_backup_name(&backup.name)?;
            let latest_version = medium.load_version(0)?;
            match &latest_version {
                Some(v) => {
                    let version = Version::decode(v.as_slice())?;
                    let scheduled_backup_time = version.timestamp() + backup.interval.to_duration();
                    let now = Utc::now();
                    if scheduled_backup_time < now {
                        next_backup = Some(backup);
                        break;
                    }

                    let this_sleep_time = scheduled_backup_time - now;
                    if next_backup.is_none() || this_sleep_time < sleep_time {
                        next_backup = Some(backup);
                        sleep_time = this_sleep_time;
                    }
                }
                None => {
                    next_backup = Some(backup);
                    break;
                }
            }
        }

        std::thread::sleep(sleep_time.to_std()?);

        let backup = next_backup.expect(">0 backups to manage");
        println!("Running scheduled backup '{}'", backup.name);
        let medium = build_medium_with_backup_name(&backup.name)?;

        backup::run(backup::Config {
            source_dir: backup.source_dir.clone(),
            medium: &*medium,
        })?;
        println!("Backup '{}' completed", backup.name);
    }
}
