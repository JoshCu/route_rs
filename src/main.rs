use chrono::{Duration, NaiveDateTime};
use std::error::Error;

mod config;
mod io;
mod mc_kernel;
mod network;
mod routing;
mod state;

use config::{ColumnConfig, OutputFormat};
use io::{
    csv::load_external_flows_parallel, netcdf::write_netcdf_output, results::SimulationResults,
};
use network::build_network_topology;
use routing::process_timestep_parallel;
use state::NetworkState;

fn main() -> Result<(), Box<dyn Error>> {
    // Configuration
    let db_path = "tests/gage-10154200/config/gage-10154200_subset.gpkg";
    let internal_timestep_seconds = 300.0;
    let dt = internal_timestep_seconds;

    // Output format configuration (can be made a command-line argument)
    let output_format = OutputFormat::NetCdf; // Change to Csv, NetCdf, or Both as needed

    // Directory containing CSV files (one per catchment)
    let csv_dir = "tests/gage-10154200/outputs/ngen/";

    // Initialize SQLite connection
    let conn = rusqlite::Connection::open(db_path)?;

    // Initialize column configuration
    let column_config = ColumnConfig::new();

    // Build network topology
    let topology = build_network_topology(&conn, &column_config)?;

    // Break up the network into segments for parallel processing
    let segments = topology.identify_segments();
    println!(
        "Identified {} network segments for parallel processing",
        segments.len()
    );

    // Initialize network state
    let mut network_state = NetworkState::new();

    // Load external flows in parallel (this is I/O bound and benefits from parallelization)
    println!("Loading external flows in parallel...");
    let qlat_var = Some("Q_OUT");
    network_state.external_flows = load_external_flows_parallel(&topology, csv_dir, qlat_var);

    // Load channel parameters and prepare features for NetCDF
    let (channel_params_map, feature_map, features) = network::load_channel_parameters(
        &conn,
        &topology,
        &column_config,
        &mut network_state,
        &output_format,
    )?;

    // Initialize simulation results for NetCDF
    let mut sim_results = SimulationResults::new();
    if matches!(output_format, OutputFormat::NetCdf | OutputFormat::Both) {
        sim_results.initialize_features(features);
    }

    // Set up CSV output if needed
    let mut csv_writer = if matches!(output_format, OutputFormat::Csv | OutputFormat::Both) {
        Some(io::csv::create_csv_writer("network_routing_results.csv")?)
    } else {
        None
    };

    // Determine simulation period
    let max_external_steps = network_state
        .external_flows
        .values()
        .flat_map(|flows| flows.keys())
        .max()
        .copied()
        .unwrap_or(0);

    let reference_time = NaiveDateTime::parse_from_str("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?;
    let start_time = reference_time;
    let end_time = start_time + Duration::seconds((3600 * max_external_steps) as i64);

    println!("Simulation period: {} to {}", start_time, end_time);
    println!(
        "Using internal timestep of {} seconds",
        internal_timestep_seconds
    );
    println!("Processing {} network nodes", topology.routing_order.len());

    // Main routing loop
    let mut current_time = start_time;
    let mut step_idx = 0;

    while step_idx <= max_external_steps * 12 {
        let current_external_idx = step_idx / 12;

        // Add timestep to results for NetCDF (only at output intervals)
        if step_idx % 12 == 0 && matches!(output_format, OutputFormat::NetCdf | OutputFormat::Both)
        {
            let time_seconds = (current_time - reference_time).num_seconds() as f64;
            sim_results.add_timestep(time_seconds);
        }

        // Process the timestep
        process_timestep_parallel(
            &topology,
            &segments,
            &mut network_state,
            &channel_params_map,
            &feature_map,
            current_external_idx,
            dt,
            step_idx,
            &mut csv_writer,
            &mut sim_results,
            &output_format,
        )?;

        // Advance to next timestep
        current_time = current_time + Duration::seconds(internal_timestep_seconds as i64);
        step_idx += 1;
    }

    // Final flush for CSV
    if let Some(mut wtr) = csv_writer {
        wtr.flush()?;
        println!("CSV results saved to network_routing_results.csv");
    }

    // Write NetCDF output if needed
    if matches!(output_format, OutputFormat::NetCdf | OutputFormat::Both) {
        let nc_filename = format!("troute_output_{}.nc", reference_time.format("%Y%m%d%H%M"));
        write_netcdf_output(&nc_filename, &sim_results, &reference_time)?;
        println!("NetCDF results saved to {}", nc_filename);
    }

    println!("Network routing complete.");
    Ok(())
}
