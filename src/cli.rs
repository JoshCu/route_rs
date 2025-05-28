use anyhow::{Context, Result};
use clap::{Parser, command};
use std::path::PathBuf;

/// Network routing simulation tool
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Route directory path
    route_dir: PathBuf,

    /// Internal timestep in seconds
    #[arg(short, long, default_value_t = 3600)]
    internal_timestep_seconds: usize,
}

pub fn get_args() -> Result<(PathBuf, PathBuf, PathBuf, usize)> {
    let args = Args::parse();
    
    let root_dir = args.route_dir;
    let csv_dir = root_dir.join("outputs").join("ngen");
    let config_dir = root_dir.join("config");
    
    // Find the .gpkg file in the config directory
    let gpkg_file = config_dir
        .read_dir()
        .context("Failed to read config directory")?
        .filter_map(Result::ok)
        .find(|entry| {
            entry.path().extension()
                .map_or(false, |ext| ext == "gpkg")
        })
        .ok_or_else(|| anyhow::anyhow!("No .gpkg file found in config directory"))?
        .path();

    Ok((
        config_dir,
        csv_dir,
        gpkg_file,
        args.internal_timestep_seconds,
    ))
}