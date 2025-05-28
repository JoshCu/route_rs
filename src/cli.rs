use clap::{Parser, command};
use std::path::{Path, PathBuf};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    route_dir: PathBuf,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 3600)]
    internal_timestep_seconds: usize,
}

pub fn get_args() -> (PathBuf, PathBuf, PathBuf, usize) {
    let args = Args::parse();

    // root folder, csv_input_dir, gpkg_path, internal_ts
    let root_dir = args.route_dir;
    let csv_dir = root_dir.join("outputs").join("ngen");

    // gpkg path is the first file with .gpkg extension in the root_dir
    let config_dir = root_dir.join("config");
    let gpkg_file = config_dir
        .read_dir()
        .unwrap()
        .find(|entry| {
            entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .unwrap_or_default()
                == "gpkg"
        })
        .unwrap()
        .unwrap()
        .path();

    (
        config_dir,
        csv_dir,
        gpkg_file,
        args.internal_timestep_seconds,
    )

    // let csv_dir = root_dir.
}
