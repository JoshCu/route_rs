use chrono::{Duration, NaiveDateTime};
use csv::{ReaderBuilder, StringRecord};
use rusqlite::{Connection, Result as SqliteResult};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;

mod mc_kernel;
// Configuration structure for column name mapping
#[derive(Debug, Clone)]
struct ColumnConfig {
    key: String,
    downstream: String,
    dx: String,
    n: String,
    ncc: String,
    s0: String,
    bw: String,
    waterbody: String,
    gages: String,
    tw: String,
    twcc: String,
    musk: String,
    musx: String,
    cs: String,
    alt: String,
}

impl ColumnConfig {
    fn new() -> Self {
        // Default configuration based on provided YAML example
        ColumnConfig {
            key: "id".to_string(),
            downstream: "toid".to_string(),
            dx: "Length_m".to_string(),
            n: "n".to_string(),
            ncc: "nCC".to_string(),
            s0: "So".to_string(),
            bw: "BtmWdth".to_string(),
            waterbody: "WaterbodyID".to_string(),
            gages: "gage".to_string(),
            tw: "TopWdth".to_string(),
            twcc: "TopWdthCC".to_string(),
            musk: "MusK".to_string(),
            musx: "MusX".to_string(),
            cs: "ChSlp".to_string(),
            alt: "alt".to_string(),
        }
    }
}

// Flow data from CSV
#[derive(Debug)]
struct FlowData {
    index: usize,
    timestamp: String,
    ql: f64,
}

impl FlowData {
    fn from_record(record: &StringRecord) -> Result<Self, Box<dyn Error>> {
        if record.len() < 3 {
            return Err("Record has fewer than 3 fields".into());
        }

        let index = record[0].trim().parse::<usize>()?;
        let timestamp = record[1].trim().to_string();
        let ql = record[2].trim().parse::<f64>()?;

        Ok(FlowData {
            index,
            timestamp,
            ql,
        })
    }
}

// Channel parameters from SQLite
#[derive(Debug)]
struct ChannelParams {
    dx: f64,
    n: f64,
    ncc: f64,
    s0: f64,
    bw: f64,
    tw: f64,
    twcc: f64,
    cs: f64,
}

// State to track previous time step values
#[derive(Debug)]
struct RoutingState {
    qup: f64,
    quc: f64,
    qdp: f64,
    depth_p: f64,
}

impl RoutingState {
    fn new() -> Self {
        RoutingState {
            qup: 0.0,
            quc: 0.0,
            qdp: 0.0,
            depth_p: 0.0,
        }
    }

    fn update(&mut self, quc: f64, qdc: f64, depthc: f64) {
        self.qup = self.quc;
        self.quc = quc;
        self.qdp = qdc;
        self.depth_p = depthc;
    }
}

// Function to fetch channel parameters from SQLite
fn get_channel_params(
    conn: &Connection,
    channel_id: &str,
    config: &ColumnConfig,
) -> SqliteResult<ChannelParams> {
    let query = format!(
        "SELECT {}, {}, {}, {}, {}, {}, {}, {} FROM 'flowpath-attributes' WHERE {} = ?",
        config.dx,
        config.n,
        config.ncc,
        config.s0,
        config.bw,
        config.tw,
        config.twcc,
        config.cs,
        config.key
    );

    let mut stmt = conn.prepare(&query)?;
    let params = rusqlite::params![channel_id];

    let channel_params = stmt.query_row(params, |row| {
        Ok(ChannelParams {
            dx: row.get(0)?,
            n: row.get(1)?,
            ncc: row.get(2)?,
            s0: row.get(3)?,
            bw: row.get(4)?,
            tw: row.get(5)?,
            twcc: row.get(6)?,
            cs: row.get(7)?,
        })
    })?;

    Ok(channel_params)
}

// Calculate time difference in seconds between two timestamps
fn calculate_dt(prev_ts: &str, curr_ts: &str) -> Result<f64, Box<dyn Error>> {
    let format = "%Y-%m-%d %H:%M:%S";
    let prev_dt = NaiveDateTime::parse_from_str(prev_ts, format)?;
    let curr_dt = NaiveDateTime::parse_from_str(curr_ts, format)?;

    let duration = curr_dt.signed_duration_since(prev_dt);
    Ok(duration.num_seconds() as f64)
}

// Linear interpolation between two flow values
fn interpolate_flow(
    time1: &NaiveDateTime,
    flow1: f64,
    time2: &NaiveDateTime,
    flow2: f64,
    target_time: &NaiveDateTime,
) -> f64 {
    if time1 == time2 {
        return flow1;
    }

    let total_seconds = (time2.signed_duration_since(*time1)).num_seconds() as f64;
    let elapsed_seconds = (target_time.signed_duration_since(*time1)).num_seconds() as f64;

    if total_seconds == 0.0 {
        return flow1;
    }

    let ratio = elapsed_seconds / total_seconds;
    flow1 + (flow2 - flow1) * ratio
}

fn main() -> Result<(), Box<dyn Error>> {
    // Hardcoded CSV file name and channel ID
    let csv_file = "tests/ngen/nex-486889_output.csv";
    let channel_id = "wb-486888";

    // Set internal timestep to 5 minutes (300 seconds)
    let internal_timestep_seconds = 300.0;

    // Initialize SQLite connection
    let db_path = "tests/config/cat-486888_subset.gpkg";
    let conn = Connection::open(db_path)?;

    // Initialize column configuration
    let column_config = ColumnConfig::new();

    // Fetch channel parameters for the specified channel
    let channel_params = match get_channel_params(&conn, channel_id, &column_config) {
        Ok(params) => params,
        Err(e) => {
            eprintln!("Failed to fetch channel parameters: {}", e);
            return Err(e.into());
        }
    };

    println!(
        "Channel parameters for {}: {:?}",
        channel_id, channel_params
    );

    // Open CSV file and set up reader for the format with comma-delimited fields
    let file = File::open(csv_file)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(file);

    // Read all records into memory for interpolation
    let mut all_records: Vec<FlowData> = Vec::new();
    for result in rdr.records() {
        let record = result?;
        let flow_data = FlowData::from_record(&record)?;
        all_records.push(flow_data);
    }

    if all_records.is_empty() {
        return Err("No records found in CSV file".into());
    }

    // Initialize routing state
    let mut state = RoutingState::new();

    // Create results to store output
    let mut results = Vec::new();

    // Parse the first and last timestamps to determine the simulation period
    let format = "%Y-%m-%d %H:%M:%S";
    let start_time = NaiveDateTime::parse_from_str(&all_records[0].timestamp, format)?;
    let end_time =
        NaiveDateTime::parse_from_str(&all_records[all_records.len() - 1].timestamp, format)?;

    println!("Simulation period: {} to {}", start_time, end_time);
    println!(
        "Using internal timestep of {} seconds",
        internal_timestep_seconds
    );

    // Create a vector of all internal timesteps
    let mut current_time = start_time;
    let mut timesteps = Vec::new();

    while current_time <= end_time {
        timesteps.push(current_time);
        current_time = current_time + Duration::seconds(internal_timestep_seconds as i64);
    }

    println!("Number of internal timesteps: {}", timesteps.len());

    // Previous values for interpolation
    let mut prev_timestamp: Option<NaiveDateTime> = None;

    // Process each internal timestep
    for (step_idx, step_time) in timesteps.iter().enumerate() {
        // Find the two data points to interpolate between
        let mut prev_record_idx = 0;
        let mut next_record_idx = 0;
        let mut found = false;

        for i in 0..all_records.len() - 1 {
            let curr_time = NaiveDateTime::parse_from_str(&all_records[i].timestamp, format)?;
            let next_time = NaiveDateTime::parse_from_str(&all_records[i + 1].timestamp, format)?;

            if *step_time >= curr_time && *step_time <= next_time {
                prev_record_idx = i;
                next_record_idx = i + 1;
                found = true;
                break;
            }
        }

        if !found {
            // If not found within intervals, use the closest record
            if *step_time < start_time {
                prev_record_idx = 0;
                next_record_idx = 0;
            } else {
                prev_record_idx = all_records.len() - 1;
                next_record_idx = all_records.len() - 1;
            }
        }

        // Get the two data points and interpolate the lateral inflow
        let prev_time =
            NaiveDateTime::parse_from_str(&all_records[prev_record_idx].timestamp, format)?;
        let next_time =
            NaiveDateTime::parse_from_str(&all_records[next_record_idx].timestamp, format)?;
        let prev_ql = all_records[prev_record_idx].ql;
        let next_ql = all_records[next_record_idx].ql;

        let interpolated_ql = interpolate_flow(&prev_time, prev_ql, &next_time, next_ql, step_time);
        // dbg!(interpolated_ql);

        // Calculate dt (time difference in seconds)
        let dt = if let Some(prev_time) = prev_timestamp {
            (step_time.signed_duration_since(prev_time)).num_seconds() as f64
        } else {
            internal_timestep_seconds // First timestep
        };

        // Run the Muskingcunge routing function
        let (qdc, velc, depthc) = mc_kernel::submuskingcunge(
            state.qup,
            state.quc,
            state.qdp,
            interpolated_ql,
            dt,
            channel_params.s0,
            channel_params.dx,
            channel_params.n,
            channel_params.cs,
            channel_params.bw,
            channel_params.tw,
            channel_params.twcc,
            channel_params.ncc,
            state.depth_p,
        );

        // Save results
        results.push((
            step_idx,
            step_time.format("%Y-%m-%d %H:%M:%S").to_string(),
            interpolated_ql,
            qdc,
            velc,
            depthc,
        ));

        // Update routing state for next iteration
        state.update(interpolated_ql, qdc, depthc);
        prev_timestamp = Some(*step_time);

        // Print results every hour (to reduce console output)
        if step_idx % 12 == 0 {
            // For 5-minute timesteps, print every hour (12 steps)
            // println!(
            //     "Step: {}, Time: {}, QL: {:.6}, QDC: {:.6}, Velocity: {:.6}, Depth: {:.6}",
            //     step_idx, step_time, interpolated_ql, qdc, velc, depthc
            // );
            println!(
                "Time: {}, flow: {:.6}, Velocity: {:.6}, Depth: {:.6}",
                step_time, qdc, velc, depthc
            );
        }
    }

    // Save results to output CSV
    write_results_to_csv(&results, "muskingcunge_results.csv")?;

    println!("Processing complete. Results saved to muskingcunge_results.csv");
    Ok(())
}

// Function to write results to CSV
fn write_results_to_csv(
    results: &[(usize, String, f64, f64, f64, f64)],
    output_file: &str,
) -> Result<(), Box<dyn Error>> {
    let path = Path::new(output_file);
    let mut wtr = csv::Writer::from_path(path)?;

    // Write header
    //wtr.write_record(&["step", "timestamp", "ql", "qdc", "velocity", "depth"])?;
    wtr.write_record(&["step", "flow", "velocity", "depth"])?;
    // Write data
    for (step, timestamp, ql, qdc, velocity, depth) in results {
        if step % 1 == 0 {
            wtr.write_record(&[
                step.to_string(),
                // timestamp.clone(),
                qdc.to_string(),
                velocity.to_string(),
                depth.to_string(),
            ])?;
        }
    }

    wtr.flush()?;
    Ok(())
}
