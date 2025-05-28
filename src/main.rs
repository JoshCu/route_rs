use chrono::{Duration, NaiveDateTime};
use std::error::Error;
mod cli;
mod config;
mod io;
mod mc_kernel;
mod network;
mod routing;
mod state;

use cli::get_args;
use config::{ColumnConfig, OutputFormat};
use io::{netcdf::init_netcdf_output, results::SimulationResults};
use network::build_network_topology;
use routing::process_routing_parallel;

fn main() -> Result<(), Box<dyn Error>> {
    // Configuration
    let (_, csv_dir, db_path, internal_timestep_seconds) = get_args();
    let dt = internal_timestep_seconds as f32;

    // Output format configuration (can be made a command-line argument)
    let output_format = OutputFormat::NetCdf; // Change to Csv, NetCdf, or Both as needed

    // Initialize SQLite connection
    let conn = rusqlite::Connection::open(db_path)?;

    // Initialize column configuration
    let column_config = ColumnConfig::new();

    // Build network topology
    let topology = build_network_topology(&conn, &column_config, &csv_dir)?;

    // Load channel parameters and prepare features for NetCDF
    let (channel_params_map, feature_map, features) =
        network::load_channel_parameters(&conn, &topology, &column_config, &output_format)?;

    // Set up CSV output if needed
    let mut csv_writer = if matches!(output_format, OutputFormat::Csv | OutputFormat::Both) {
        Some(io::csv::create_csv_writer("network_routing_results.csv")?)
    } else {
        None
    };

    // read the last line of one of the cat files to get the last time step
    let first_id = features.get(0).unwrap().clone();
    let file_name = csv_dir.join(format!("cat-{}.csv", first_id));
    let max_external_steps = std::fs::read_to_string(file_name.as_path())?
        .lines()
        .count()
        - 1;

    let reference_time = NaiveDateTime::parse_from_str("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?;
    let start_time = reference_time;
    let end_time = start_time + Duration::seconds((3600 * max_external_steps) as i64);

    // Calculate total timesteps (internal)
    let external_timestep_seconds = 3600;
    let total_timesteps =
        (max_external_steps + 1) * (external_timestep_seconds / internal_timestep_seconds);

    println!("Simulation period: {} to {}", start_time, end_time);
    println!(
        "Using internal timestep of {} seconds",
        internal_timestep_seconds
    );
    println!("Processing {} network nodes", topology.routing_order.len());
    println!("Total internal timesteps: {}", total_timesteps);

    // Add timesteps to results for NetCDF
    let timesteps = (0..=max_external_steps)
        .map(|step| {
            let time_seconds = (step * 3600) as f64;
            time_seconds
        })
        .collect::<Vec<f64>>();
    let nc_filename = format!("troute_output_{}.nc", reference_time.format("%Y%m%d%H%M"));
    let mut netcdf_writer = init_netcdf_output(
        &nc_filename,
        topology.routing_order.len(),
        timesteps,
        &reference_time,
    )
    .unwrap();

    // Run parallel routing
    println!("Starting parallel wave-front routing...");
    process_routing_parallel(
        &topology,
        &channel_params_map,
        total_timesteps,
        dt,
        netcdf_writer,
    )?;

    // Final flush for CSV
    if let Some(mut wtr) = csv_writer {
        wtr.flush()?;
        println!("CSV results saved to network_routing_results.csv");
    }

    println!("Network routing complete.");
    Ok(())
}
