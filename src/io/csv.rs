use anyhow::{Context, Result};
use csv::{ReaderBuilder, Writer, WriterBuilder};
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

// Function to load external flows for a specific nexus/catchment
pub fn load_external_flows(
    csv_file: PathBuf,
    id: &u32,
    var_name: Option<&str>,
    area: f32,
) -> Result<VecDeque<f32>> {
    let mut external_flows = Vec::new();

    // Check if file exists, if not return empty flows
    if !csv_file.exists() {
        println!(
            "No external flow file found for {}: {}",
            id,
            csv_file.display()
        );
        return Ok(VecDeque::from(external_flows));
    }

    let file = File::open(&csv_file)
        .with_context(|| format!("Failed to open CSV file: {}", csv_file.display()))?;
    let buffered_reader = BufReader::new(file);

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(buffered_reader);

    let qlat_index = match var_name {
        Some(var_name) => {
            let headers = rdr.headers()
                .context("Failed to read CSV headers")?;
            headers.iter().position(|h| h == var_name).unwrap_or(2)
        }
        None => 2,
    };

    for (i, result) in rdr.records().enumerate() {
        let record = result
            .with_context(|| format!("Failed to read record {} in file {}", i, csv_file.display()))?;
        
        let ql_str = record.get(qlat_index)
            .ok_or_else(|| anyhow::anyhow!("Missing column {} in record {}", qlat_index, i))?;
        
        let ql = ql_str.trim().parse::<f32>()
            .with_context(|| format!("Failed to parse flow value '{}' in record {}", ql_str, i))?;

        // https://github.com/CIROH-UA/ngen/blob/ed2a903730467fa631716c033b757c3dff5fa2bb/include/core/Layer.hpp#L142
        let adjusted_flow = (ql * (area * 1_000_000.0)) / 3600.0;
        external_flows.push(adjusted_flow);
    }
    
    Ok(VecDeque::from(external_flows))
}

// Create CSV writer with headers
pub fn create_csv_writer(path: &str) -> Result<Writer<File>> {
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("Failed to create CSV writer at {}", path))?;

    // Write header
    wtr.write_record(&["step", "feature_id", "flow", "velocity", "depth"])
        .context("Failed to write CSV header")?;

    Ok(wtr)
}