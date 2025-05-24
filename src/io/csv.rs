use crate::network::{NetworkTopology, NodeType, get_area_sqkm};
use csv::{ReaderBuilder, StringRecord, Writer, WriterBuilder};
use netcdf::rc::get;
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
    fn from_record(record: &StringRecord, qlat_index: usize) -> Result<Self, Box<dyn Error>> {
        if record.len() < 3 {
            return Err("Record has fewer than 3 fields".into());
        }

        let index = record[0].trim().parse::<usize>()?;
        let timestamp = record[1].trim().to_string();
        let ql = record[qlat_index].trim().parse::<f64>()?;

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
    id: &str,
    var_name: Option<&str>,
    area: f64,
) -> Result<HashMap<usize, f64>, Box<dyn Error>> {
    let mut external_flows = HashMap::new();

    // Check if file exists, if not return empty flows
    if !Path::new(csv_file).exists() {
        println!("No external flow file found for {}: {}", id, csv_file);
        return Ok(external_flows);
    }

    let file = File::open(csv_file)?;
    let buffered_reader = BufReader::new(file);

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(buffered_reader);

    let qlat_index = match var_name {
        Some(var_name) => {
            let headers = rdr.headers()?;
            headers.iter().position(|h| h == var_name).unwrap_or(2)
        }
        None => 2,
    };

    for result in rdr.records() {
        let record = result?;
        let flow_data = FlowData::from_record(&record, qlat_index)?;
        // https://github.com/CIROH-UA/ngen/blob/ed2a903730467fa631716c033b757c3dff5fa2bb/include/core/Layer.hpp#L142
        let adjusted_flow = (flow_data.ql * (area * 1000000.0)) / 3600.0;
        external_flows.insert(flow_data.index, adjusted_flow);
    }

    println!(
        "Loaded {} external flow records for {}",
        external_flows.len(),
        id
    );
    Ok(external_flows)
}

// Parallel loading of external flows
pub fn load_external_flows_parallel(
    topology: &NetworkTopology,
    csv_dir: &str,
    var_name: Option<&str>,
) -> HashMap<String, HashMap<usize, f64>> {
    let ids: Vec<_> = topology
        .routing_order
        .clone()
        .into_par_iter()
        .filter(|id| match topology.nodes.get(id) {
            Some(node) => node.node_type == NodeType::Flowpath,
            None => false,
        })
        .collect();

    let flows: Vec<_> = ids
        .par_iter()
        .map(|id| {
            let csv_file = format!("{}{}.csv", csv_dir, id.replace("wb", "cat"));
            let area = topology.nodes.get(id).unwrap().area_sqkm.unwrap();
            let flows = match load_external_flows(&csv_file, id, var_name, area) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Failed to load external flows for {}: {}", id, e);
                    HashMap::new()
                }
            };
            (id.clone(), flows)
        })
        .collect();

    flows.into_iter().collect()
}

// Create CSV writer with headers
pub fn create_csv_writer(path: &str) -> Result<Writer<File>, Box<dyn Error>> {
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(path)?;

    // Write header
    wtr.write_record(&["step", "feature_id", "flow", "velocity", "depth"])?;

    Ok(wtr)
}
