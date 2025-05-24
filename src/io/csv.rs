use crate::network::NetworkTopology;
use csv::{ReaderBuilder, StringRecord, Writer, WriterBuilder};
use rayon::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

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

// Function to load external flows for a specific nexus/catchment
pub fn load_external_flows(
    csv_file: &str,
    nexus_id: &str,
) -> Result<HashMap<usize, f64>, Box<dyn Error>> {
    let mut external_flows = HashMap::new();

    // Check if file exists, if not return empty flows
    if !Path::new(csv_file).exists() {
        println!("No external flow file found for {}: {}", nexus_id, csv_file);
        return Ok(external_flows);
    }

    let file = File::open(csv_file)?;
    let buffered_reader = BufReader::new(file);

    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(buffered_reader);

    for result in rdr.records() {
        let record = result?;
        let flow_data = FlowData::from_record(&record)?;
        external_flows.insert(flow_data.index, flow_data.ql);
    }

    println!(
        "Loaded {} external flow records for {}",
        external_flows.len(),
        nexus_id
    );
    Ok(external_flows)
}

// Parallel loading of external flows
pub fn load_external_flows_parallel(
    topology: &NetworkTopology,
    csv_dir: &str,
) -> HashMap<String, HashMap<usize, f64>> {
    let nexus_ids: Vec<_> = topology.routing_order.clone();

    let flows: Vec<_> = nexus_ids
        .par_iter()
        .map(|nexus_id| {
            let csv_file = format!("{}{}_output.csv", csv_dir, nexus_id);
            let flows = match load_external_flows(&csv_file, nexus_id) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Failed to load external flows for {}: {}", nexus_id, e);
                    HashMap::new()
                }
            };
            (nexus_id.clone(), flows)
        })
        .collect();

    flows.into_iter().collect()
}

// Create CSV writer with headers
pub fn create_csv_writer(path: &str) -> Result<Writer<File>, Box<dyn Error>> {
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(path)?;

    // Write header
    wtr.write_record(&[
        "step",
        "nexus_id",
        "channel_id",
        "flow",
        "velocity",
        "depth",
    ])?;

    Ok(wtr)
}
