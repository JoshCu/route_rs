use chrono::{Duration, NaiveDateTime};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use rusqlite::{Connection, Result as SqliteResult};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Write};
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

fn main() -> Result<(), Box<dyn Error>> {
    // Hardcoded CSV file name and channel ID
    let csv_file = "tests/ngen/nex-486889_output.csv";
    let channel_id = "wb-486888";

    // Set internal timestep to 5 minutes (300 seconds)
    let internal_timestep_seconds = 300.0;
    let dt = internal_timestep_seconds;

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

    // Open CSV file with buffered reader for better performance
    let file = File::open(csv_file)?;
    let buffered_reader = BufReader::new(file);

    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(buffered_reader);

    // Set up output CSV writer immediately to stream results
    let output_path = Path::new("muskingcunge_results.csv");
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_path)?;

    // Write header
    wtr.write_record(&["step", "flow", "velocity", "depth"])?;

    // Process the first record to get start time
    let mut records = rdr.records();

    let first_record = match records.next() {
        Some(Ok(record)) => record,
        Some(Err(e)) => return Err(format!("Error reading first record: {}", e).into()),
        None => return Err("CSV file is empty".into()),
    };

    let first_data = FlowData::from_record(&first_record)?;
    let format = "%Y-%m-%d %H:%M:%S";
    let start_time = NaiveDateTime::parse_from_str(&first_data.timestamp, format)?;

    // Initialize a time-indexed cache of external flow inputs
    let mut external_flow_cache = HashMap::new();
    external_flow_cache.insert(first_data.index, first_data.ql);

    // Create a streaming buffer for the next records
    let mut current_external_flow = first_data.ql;

    // Initialize routing state
    let mut state = RoutingState::new();

    // Process remaining external flow records in the background
    let mut max_external_idx = first_data.index;

    for result in records {
        let record = result?;
        let flow_data = FlowData::from_record(&record)?;
        external_flow_cache.insert(flow_data.index, flow_data.ql);
        if flow_data.index > max_external_idx {
            max_external_idx = flow_data.index;
        }
    }

    // Get end time from the highest index record
    // Assuming the last record in the CSV has the highest index and represents the end time
    let end_time = start_time + Duration::seconds((3600 * max_external_idx) as i64);

    println!("Simulation period: {} to {}", start_time, end_time);
    println!(
        "Using internal timestep of {} seconds",
        internal_timestep_seconds
    );

    // Process each internal timestep directly
    let mut current_time = start_time;
    let mut step_idx = 0;

    while current_time <= end_time {
        // Calculate which external flow value to use
        let current_external_idx = step_idx / 12;

        // Get external flow value from cache
        if let Some(ql) = external_flow_cache.get(&current_external_idx) {
            current_external_flow = *ql;
        }

        // Run the Muskingcunge routing function
        let (qdc, velc, depthc) = mc_kernel::submuskingcunge(
            state.qup,
            state.quc,
            state.qdp,
            current_external_flow,
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

        // Stream results to output file directly if this is an output timestep
        if step_idx % 12 == 0 {
            wtr.write_record(&[
                step_idx.to_string(),
                qdc.to_string(),
                velc.to_string(),
                depthc.to_string(),
            ])?;
        }

        // Update routing state for next iteration
        state.update(current_external_flow, qdc, depthc);

        // Advance to next timestep
        current_time = current_time + Duration::seconds(internal_timestep_seconds as i64);
        step_idx += 1;
    }

    // Final flush to ensure all data is written
    wtr.flush()?;

    println!("Processing complete. Results saved to muskingcunge_results.csv");
    Ok(())
}
