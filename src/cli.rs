use crate::kernel::muskingum::MuskingumCungeKernel;
use anyhow::{Context, Result};
use clap::Parser;
use colored::Colorize;
use num_cpus;
use std::path::PathBuf;
/// Network routing simulation tool
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Route directory path
    route_dir: PathBuf,

    /// Path to the GeoPackage (.gpkg) hydrofabric file
    #[arg(long)]
    hf: Option<PathBuf>,

    /// Path to the input directory containing CSV files
    #[arg(short = 'i', long)]
    input_dir: Option<PathBuf>,

    /// Path to the output directory
    #[arg(short, long)]
    output_dir: Option<PathBuf>,

    /// Internal timestep in seconds
    #[arg(short = 't', long, default_value_t = 300)]
    internal_timestep_seconds: usize,
    #[arg(short, long, default_value_t = MuskingumCungeKernel::TRouteModernized)]
    kernel: MuskingumCungeKernel,
    #[arg(short, long, default_value_t = num_cpus::get())]
    num_threads: usize,
}
pub fn print_banner(config: &Config) {
    eprintln!("   {}", "🌊 Route RS".cyan().bold());
    eprintln!("  Kernel:   {}", format!("{}", config.kernel).green());
    eprintln!("  Timestep: {}s", config.internal_timestep_seconds);
    eprintln!("  Threads:  {}", config.num_threads);
    eprintln!(
        "  GeoPackage: {}",
        config.gpkg_file.display().to_string().dimmed()
    );
    eprintln!();
}
/// The Config struct holds all the configuration parameters needed for the routing simulation, including:
///  * Paths to the config directory, input CSV directory, GeoPackage file, and output directory
///  * Internal timestep in seconds
///  * Routing kernel to use
///  * Number of threads for parallel processing
#[derive(Debug, Clone)]
pub struct Config {
    pub config_dir: PathBuf,
    pub csv_dir: PathBuf,
    pub gpkg_file: PathBuf,
    pub internal_timestep_seconds: usize,
    pub output_dir: PathBuf,
    pub kernel: MuskingumCungeKernel,
    pub num_threads: usize,
}

pub fn get_args() -> Result<Config> {
    let args = Args::parse();

    let root_dir = args.route_dir;
    let csv_dir = args.input_dir.unwrap_or_else(|| root_dir.join("outputs").join("ngen"));
    let config_dir = root_dir.join("config");
    let output_dir = args.output_dir.unwrap_or_else(|| root_dir.join("outputs").join("troute"));

    // Check directories valid
    if !root_dir.exists() || !root_dir.is_dir() {
        return Err(anyhow::anyhow!(
            "Given root directory does not exist or is not a directory: {:?}",
            root_dir
        ))
        .with_context(|| format!("Failed to access root directory: {:?}", root_dir));
    }

    let dirs_to_check: Vec<&PathBuf> = if args.hf.is_some() {
        vec![&csv_dir, &output_dir]
    } else {
        vec![&csv_dir, &config_dir, &output_dir]
    };
    let mut missing_dirs = Vec::new();
    for dir in dirs_to_check {
        if !dir.exists() || !dir.is_dir() {
            missing_dirs.push(dir);
        }
    }
    if !missing_dirs.is_empty() {
        return Err(anyhow::anyhow!(
            "Missing required directories: {:?}",
            missing_dirs
        ))
        .with_context(|| format!("Failed to access required directories: {:?}", missing_dirs));
    }

    // Use provided gpkg file or find one in the config directory
    let gpkg_file = if let Some(hf) = args.hf {
        if !hf.exists() {
            return Err(anyhow::anyhow!(
                "Specified hydrofabric file does not exist: {:?}",
                hf
            ));
        }
        hf
    } else {
        config_dir
            .read_dir()
            .context("Failed to read config directory")?
            .filter_map(Result::ok)
            .find(|entry| entry.path().extension().map_or(false, |ext| ext == "gpkg"))
            .ok_or_else(|| anyhow::anyhow!("No .gpkg file found in config directory"))?
            .path()
    };
    let cfg = Config {
        config_dir,
        csv_dir,
        gpkg_file,
        internal_timestep_seconds: args.internal_timestep_seconds,
        output_dir,
        kernel: args.kernel,
        num_threads: args.num_threads,
    };
    print_banner(&cfg);
    Ok(cfg)
}

/// The CfgContext struct is a simplified version of the Config struct that 
/// contains only the essential parameters needed for the routing simulation, such as the internal timestep, kernel type, and number of threads.
/// 
/// Unlike the Config struct, however, it may include additional non-program-argument attributes
/// that are controlled elsewhere. To facilitate this, it should be passed by reference.
/// 
/// By removing the complex types, we can simplify a good portion of function signatures,
/// while making it significantly easier to add top-down toggles and other control variables
/// that might be needed at any point in the call stack, without needing to modify a large number
/// of both function signatures and call sites.
/// 
/// When fully implemented, 
#[allow(unused)]
#[derive(Debug, Clone, Copy)]
pub struct CfgContext {
    pub internal_timestep_seconds: usize,
    pub kernel: MuskingumCungeKernel,
    pub num_threads: usize,
}

impl CfgContext {
    pub fn from_config(config: &Config) -> Self {
        Self {
            internal_timestep_seconds: config.internal_timestep_seconds,
            kernel: config.kernel,
            num_threads: config.num_threads,
        }
    }
    /// If the provided arguments are important/different enough, provide a suffix/infix for naming
    /// the output files and distinguish them from other runs.
    #[allow(unused)]
    pub fn flags_identifier(self) -> Option<String> {
        let mut parts: Vec<String> = Vec::new();
        // With the current setup, only the kernel type is likely to produce meaningful differences in output, so we only include that for now.
        // match self.kernel { // Leaving commented for now to avoid a potentially breaking change.
        //     // PartialEq not implemented, so match statement works.
        //     MuskingumCungeKernel::TRouteModernized => {}
        //     _ => parts.push(format!("kernel-{}", self.kernel)),
        // }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("_"))
        }
    }
}

#[cfg(test)]
mod tests {
    // Same-file tests for CLI module
    use super::*;

    // Test Args parsing with default values
    #[test]
    fn test_args_parsing_defaults() {
        let args = Args::parse_from(["test", "test_route_dir"]);
        assert_eq!(args.route_dir, PathBuf::from("test_route_dir"));
        assert_eq!(args.internal_timestep_seconds, 300);
        match args.kernel {
            MuskingumCungeKernel::TRouteModernized => {}
            _ => panic!("Expected default kernel to be TRouteModernized"),
        }
    }
    // Test Args parsing with custom values
    #[test]
    fn test_args_parsing_custom() {
        let args = Args::parse_from([
            "test",
            "test_route_dir",
            "-t",
            "600",
            "-k",
            "t-route-legacy",
        ]);
        assert_eq!(args.route_dir, PathBuf::from("test_route_dir"));
        assert_eq!(args.internal_timestep_seconds, 600);
        match args.kernel {
            MuskingumCungeKernel::TRouteLegacy => {}
            _ => panic!("Expected kernel to be TRouteLegacy"),
        }
    }
    // Impossible to test get_args(), as it pulls from the program's actual command line arguments, which we can't easily manipulate.
    // #[test]
    // fn test_get_args_invalid_root() {
    //     let result = get_args();
    //     assert!(result.is_err());
    // }
}
