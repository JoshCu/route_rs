use anyhow::{Context, Result};
use chrono::{Duration, NaiveDateTime};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::sync::Arc;

mod cli;
mod config;
mod io;

mod network;
mod routing;
mod state;
pub mod kernel {
    pub mod muskingum;
}

use cli::get_args;
use config::{ChannelParams, ColumnConfig, OutputFormat};
use io::netcdf::init_netcdf_output;
use network::build_network_topology;
use routing::process_routing_parallel;

static OUTPUT_TYPE: &str = "NetCDF"; // or "CSV" or "Both"

fn main() -> Result<()> {
    // Configuration
    //let (_, csv_dir, db_path, internal_timestep_seconds, output_dir)
    let config: cli::Config = get_args()?;
    run_routing(config, false)
}

fn run_routing(config: cli::Config, quiet: bool) -> Result<()> {
    let config_args: cli::CfgContext = cli::CfgContext::from_config(&config);
    let dt: f32 = config.internal_timestep_seconds as f32;
    let db_path: std::path::PathBuf = config.gpkg_file;
    let csv_dir: std::path::PathBuf = config.csv_dir;
    let _config_dir: std::path::PathBuf = config.config_dir;
    // let output_format: OutputFormat = OutputFormat::NetCdf;
    let output_format: OutputFormat = match OUTPUT_TYPE {
        "CSV" => OutputFormat::Csv,
        "NetCDF" => OutputFormat::NetCdf,
        "Both" => OutputFormat::Both,
        _ => return Err(anyhow::anyhow!("Invalid output type: {}", OUTPUT_TYPE)),
    };

    // Initialize SQLite connection
    let conn = rusqlite::Connection::open(&db_path)
        .with_context(|| format!("Failed to open database: {:?}", db_path))?;

    let column_config = ColumnConfig::new();

    // Build network topology
    println!("Building network topology...");
    let topology = build_network_topology(&conn, &column_config, &csv_dir, &config_args)?;

    // Load channel parameters
    println!("Loading channel parameters...");
    let channel_params_map =
        network::load_channel_parameters(&conn, &topology, &column_config, &config_args)?;

    // Set up CSV output if needed
    let csv_writer = if matches!(output_format, OutputFormat::Csv | OutputFormat::Both) {
        Some(io::csv::create_csv_writer("network_routing_results.csv")?)
    } else {
        None
    };

    // Get simulation parameters
    let (max_external_steps, reference_time) =
        get_simulation_params(&csv_dir, &channel_params_map)?;

    let start_time = reference_time;
    let end_time = start_time + Duration::seconds((3600 * max_external_steps) as i64);

    let external_timestep_seconds = 3600;
    let downsampling = external_timestep_seconds / config.internal_timestep_seconds;
    let total_timesteps = max_external_steps * downsampling;

    println!("\nSimulation Configuration:");
    println!("  Period: {} to {}", start_time, end_time);
    println!(
        "  Internal timestep: {} seconds",
        config.internal_timestep_seconds
    );
    println!("  Network nodes: {}", topology.routing_order.len());
    println!("  Total timesteps: {}", total_timesteps);

    // Initialize NetCDF output
    // skip the 0th timestep
    let timesteps: Vec<f64> = (1..=max_external_steps)
        .map(|step| (step * external_timestep_seconds) as f64)
        .collect();

    let config_args_infix: Option<String> = config_args.flags_identifier();
    // let nc_filename = format!("troute_output_{}.nc", reference_time.format("%Y%m%d%H%M"));
    let nc_filename = if let Some(infix) = config_args_infix {
        format!("troute_output_{}_{}.nc", infix, reference_time.format("%Y%m%d%H%M"))
    } else {
        format!("troute_output_{}.nc", reference_time.format("%Y%m%d%H%M"))
    };
    let netcdf_writer = init_netcdf_output(
        config.output_dir,
        &nc_filename,
        topology.routing_order.len(),
        timesteps,
        &reference_time,
    )?;

    // Create progress bar
    let pb = ProgressBar::new(topology.routing_order.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} nodes ({eta})")?
            .progress_chars("#>-")
    );
    if quiet {
        pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }

    // Run parallel routing
    println!("\nStarting parallel wave-front routing...");
    process_routing_parallel(
        Arc::new(topology),
        Arc::new(channel_params_map),
        total_timesteps,
        dt,
        downsampling,
        netcdf_writer,
        Arc::new(pb),
        &config_args,
    )?;

    // Final flush for CSV
    if let Some(mut wtr) = csv_writer {
        wtr.flush().context("Failed to flush CSV writer")?;
        println!("CSV results saved to network_routing_results.csv");
    }

    println!(
        "\nNetwork routing complete. Output saved to {}",
        nc_filename
    );
    Ok(())
}

fn get_simulation_params(
    csv_dir: &std::path::PathBuf,
    features: &HashMap<u32, ChannelParams>,
) -> Result<(usize, NaiveDateTime)> {
    let first_id = features
        .keys()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No features found"))?;

    let file_name = csv_dir.join(format!("cat-{}.csv", first_id));
    let content = std::fs::read_to_string(&file_name)
        .with_context(|| format!("Failed to read file: {:?}", file_name))?;

    if content.lines().count() == 0 {
        return Err(anyhow::anyhow!("CSV file is empty: {:?}", file_name))
            .with_context(|| format!("Failed to read CSV file: {:?}", file_name));
    } else if content.lines().count() == 1 {
        return Err(anyhow::anyhow!(
            "CSV file only contains header: {:?}",
            file_name
        ))
        .with_context(|| format!("Failed to read CSV file: {:?}", file_name));
    }

    let max_external_steps = content.lines().count().saturating_sub(2);

    let line = content
        .lines()
        .nth(1)
        .with_context(|| format!("Failed to read second line of CSV file: {:?}", file_name))?;
    let time = line
        .split(',')
        .nth(1)
        .with_context(|| format!("Failed to parse time from CSV line: {:?}", line))?;

    let reference_time = NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S")
        .context("Failed to parse reference time")?;

    Ok((max_external_steps, reference_time))
}

#[cfg(test)]
mod tests {
    use crate::kernel::muskingum;
    use crate::kernel::muskingum::MuskingumCungeKernel;

    // Same-file tests for main file
    use super::*;

    // Can't easily test the full routing process without
    // creating test data, but we can use the included ./tests/one_cat/ dataset
    // to test for expected results

    fn setup_test_config() -> cli::Config {
        cli::Config {
            config_dir: std::path::PathBuf::from("./tests/one_cat/config"),
            csv_dir: std::path::PathBuf::from("./tests/one_cat/outputs/ngen"),
            gpkg_file: std::path::PathBuf::from("./tests/one_cat/config/cat-486888_subset.gpkg"),
            internal_timestep_seconds: 300,
            output_dir: std::path::PathBuf::from("./tests/one_cat/outputs/troute"),
            kernel: muskingum::MuskingumCungeKernel::TRouteModernized,
            num_threads: 1,
        }
    }

    // Test get_simulation_params with the included test dataset

    #[test]
    fn test_get_simulation_params() {
        // Test that get_simulation_params correctly reads the CSV file and extracts the max external steps and reference time
        let config: cli::Config = setup_test_config();
        let config_args: cli::CfgContext = cli::CfgContext {
            internal_timestep_seconds: config.internal_timestep_seconds,
            kernel: config.kernel,
            num_threads: config.num_threads,
        };
        let conn: rusqlite::Connection = rusqlite::Connection::open(&config.gpkg_file).unwrap();
        let column_config: ColumnConfig = ColumnConfig::new();
        let topology: network::NetworkTopology =
            build_network_topology(&conn, &column_config, &config.csv_dir, &config_args).unwrap();
        let channel_params_map: HashMap<u32, ChannelParams> =
            network::load_channel_parameters(&conn, &topology, &column_config, &config_args)
                .unwrap();

        let (max_external_steps, reference_time) =
            get_simulation_params(&config.csv_dir, &channel_params_map).unwrap();

        assert_eq!(max_external_steps, 24);
        assert_eq!(
            reference_time,
            NaiveDateTime::parse_from_str("2010-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
        );
    }

    #[test]
    fn test_invalid_csv_file() {
        // Test that get_simulation_params returns an error when the CSV file is missing or invalid
        let config: cli::Config = setup_test_config();
        let config_args: cli::CfgContext = cli::CfgContext {
            internal_timestep_seconds: config.internal_timestep_seconds,
            kernel: config.kernel,
            num_threads: config.num_threads,
        };
        let conn: rusqlite::Connection = rusqlite::Connection::open(&config.gpkg_file).unwrap();
        let column_config: ColumnConfig = ColumnConfig::new();
        let topology: network::NetworkTopology =
            build_network_topology(&conn, &column_config, &config.csv_dir, &config_args).unwrap();
        let channel_params_map: HashMap<u32, ChannelParams> =
            network::load_channel_parameters(&conn, &topology, &column_config, &config_args)
                .unwrap();

        // Test with non-existent file
        let invalid_csv_dir = std::path::PathBuf::from("./tests/one_cat/outputs/invalid_csv");
        let result: std::result::Result<(usize, NaiveDateTime), anyhow::Error> =
            get_simulation_params(&invalid_csv_dir, &channel_params_map);

        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Failed to read file:")
        );
    }

    #[test]
    fn test_empty_features() {
        // Test that get_simulation_params returns an error when the features map is empty
        let empty_features: HashMap<u32, ChannelParams> = HashMap::new();
        let result = get_simulation_params(
            &std::path::PathBuf::from("./tests/one_cat/outputs/ngen"),
            &empty_features,
        );
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("No features found")
        );
    }

    #[test]
    fn test_invalid_feature() {
        // Test that get_simulation_params returns an error when the first feature ID does not correspond to a valid CSV file
        let config: cli::Config = setup_test_config();
        let mut invalid_features: HashMap<u32, ChannelParams> = HashMap::new();
        invalid_features.insert(
            999999,
            ChannelParams {
                dx: 0.0,
                n: 0.0,
                ncc: 0.0,
                s0: 0.0,
                bw: 0.0,
                tw: 0.0,
                twcc: 0.0,
                cs: 0.0,
            },
        );
        let result = get_simulation_params(&config.csv_dir, &invalid_features);
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Failed to read file:")
        );
    }

    // Do basic test for run_routing with the included test dataset, just to check that it runs without error and produces output files
    // Ideally we want to fail if any part of the routing process fails

    #[test]
    fn test_run_routing() {
        let tmp_dir = std::env::temp_dir().join("rs_route_test_run_routing");
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let config = cli::Config {
            output_dir: tmp_dir.clone(),
            ..setup_test_config()
        };
        let result = run_routing(config, true);
        let _ = std::fs::remove_dir_all(&tmp_dir);
        assert!(result.is_ok());
    }

    fn make_test_input() -> muskingum::MuskingumCungeInput {
        muskingum::MuskingumCungeInput {
            dt: 300.0,
            qup: 5.0,
            quc: 6.0,
            qdp: 4.5,
            ql: 0.5,
            dx: 5000.0,
            bw: 10.0,
            tw: 100.0,
            tw_cc: 120.0,
            n: 0.06,
            n_cc: 0.12,
            cs: 1.0,
            s0: 0.001,
            velp: 0.0,
            depthp: 0.5,
        }
    }

    fn assert_results_close(
        a: &muskingum::MuskingumCungeResult,
        b: &muskingum::MuskingumCungeResult,
        tolerance: f32,
        label: &str,
    ) {
        let flow_diff = if a.qdc == 0.0 && b.qdc == 0.0 {
            0.0
        } else {
            ((a.qdc - b.qdc) / a.qdc).abs()
        };
        let vel_diff = if a.velc == 0.0 && b.velc == 0.0 {
            0.0
        } else {
            ((a.velc - b.velc) / a.velc).abs()
        };
        let depth_diff = if a.depthc == 0.0 && b.depthc == 0.0 {
            0.0
        } else {
            ((a.depthc - b.depthc) / a.depthc).abs()
        };

        assert!(
            flow_diff < tolerance,
            "{}: flow differs by {:.1}% (got {}, expected {})",
            label,
            flow_diff * 100.0,
            b.qdc,
            a.qdc,
        );
        assert!(
            vel_diff < tolerance,
            "{}: velocity differs by {:.1}% (got {}, expected {})",
            label,
            vel_diff * 100.0,
            b.velc,
            a.velc,
        );
        assert!(
            depth_diff < tolerance,
            "{}: depth differs by {:.1}% (got {}, expected {})",
            label,
            depth_diff * 100.0,
            b.depthc,
            a.depthc,
        );
    }

    /// The two Fortran kernels (modernized and legacy) should produce
    /// near-identical results for a single timestep.
    #[test]
    fn test_kernel_equivalence_fortran_kernels() {
        let input = make_test_input();
        let f_mod = MuskingumCungeKernel::TRouteModernized.exec(&input, false);
        let f_leg = MuskingumCungeKernel::TRouteLegacy.exec(&input, false);
        let tolerance = 0.0001; //0.01%

        assert_results_close(&f_mod, &f_leg, tolerance, "Fortran Modernized vs Legacy");
    }

    /// The Rust kernel should closely match the Fortran kernels.
    #[test]
    fn test_kernel_equivalence_rust_vs_fortran() {
        let input = make_test_input();
        let rs = MuskingumCungeKernel::RouteRs.exec(&input, false);
        let f_mod = MuskingumCungeKernel::TRouteModernized.exec(&input, false);
        let tolerance = 0.0001; //0.01%
        assert_results_close(&rs, &f_mod, tolerance, "RouteRs vs TRouteModernized");
    }

    /// The C kernel uses double precision internally and a different secant method
    /// structure, so it can diverge more than the other three. This test documents
    /// the current level of divergence without enforcing a tight tolerance.
    #[test]
    fn test_c_kernel_produces_output() {
        let input = make_test_input();
        let result = MuskingumCungeKernel::CMuskingumCunge.exec(&input, false);

        // Should produce non-zero, positive results
        assert!(
            result.qdc > 0.0,
            "C kernel flow should be positive, got {}",
            result.qdc
        );
        assert!(
            result.velc > 0.0,
            "C kernel velocity should be positive, got {}",
            result.velc
        );
        assert!(
            result.depthc > 0.0,
            "C kernel depth should be positive, got {}",
            result.depthc
        );
    }

    /// Run each kernel through the full test routing dataset, writing to
    /// separate temp directories to avoid file conflicts.
    #[test]
    fn test_kernel_equivalence_full_routing() {
        let kernels = [
            MuskingumCungeKernel::RouteRs,
            MuskingumCungeKernel::TRouteModernized,
            MuskingumCungeKernel::TRouteLegacy,
        ];
        let mut flow_sums: Vec<(MuskingumCungeKernel, f64)> = Vec::new();

        for kernel in kernels {
            let tmp_dir = std::env::temp_dir().join(format!("rs_route_test_{:?}", kernel));
            std::fs::create_dir_all(&tmp_dir).unwrap();

            let config: cli::Config = cli::Config {
                kernel,
                output_dir: tmp_dir.clone(),
                ..setup_test_config()
            };
            let config_args: cli::CfgContext = cli::CfgContext::from_config(&config);
            let result = run_routing(config, true);
            assert!(
                result.is_ok(),
                "Kernel {:?} failed: {:?}",
                kernel,
                result.err()
            );
            

            // Read back the output NetCDF to get total flow
            let config_args_infix: Option<String> = config_args.flags_identifier();

            // let nc_path = tmp_dir.join("troute_output_201001010000.nc");
            let nc_filename = if let Some(infix) = config_args_infix {
                format!("troute_output_{}_201001010000.nc", infix)
            } else {
                format!("troute_output_201001010000.nc")
            };
            let nc_path = tmp_dir.join(nc_filename);
            println!("{}", nc_path.display());
            let file = netcdf::open(&nc_path).unwrap();
            let flow_var = file.variable("flow").unwrap();
            let flow_data = flow_var.get::<f32, _>(..).unwrap();
            let total_flow: f64 = flow_data.iter().map(|&v| v as f64).sum();
            flow_sums.push((kernel, total_flow));

            // Cleanup
            let _ = std::fs::remove_dir_all(&tmp_dir);
        }

        let tolerance = 0.0001; //0.01% relative tolerance
        let (ref_name, ref_flow) = flow_sums[0];
        for &(name, flow) in &flow_sums[1..] {
            let diff = ((flow - ref_flow) / ref_flow).abs();
            assert!(
                diff < tolerance,
                "{:?} vs {:?}: total flow differs by {:.1}% ({} vs {})",
                name,
                ref_name,
                diff * 100.0,
                flow,
                ref_flow,
            );
        }
    }

    #[test]
    fn test_run_invalid_config() {
        // Test that run_routing returns an error when given an invalid configuration (e.g. non-existent database file)
        let invalid_config = cli::Config {
            config_dir: std::path::PathBuf::from("./tests/invalid_test/config"),
            csv_dir: std::path::PathBuf::from("./tests/invalid_test/outputs/ngen"),
            gpkg_file: std::path::PathBuf::from(
                "./tests/invalid_test/config/cat-486888_subset.gpkg",
            ),
            internal_timestep_seconds: 300,
            output_dir: std::path::PathBuf::from("./tests/invalid_test/outputs/troute"),
            kernel: muskingum::MuskingumCungeKernel::TRouteModernized,
            num_threads: 1,
        };
        let result = run_routing(invalid_config, true);
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Failed to open database:")
        );
    }
}
