use crate::io::results::SimulationResults;
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use netcdf::{self, FileMut};
use std::sync::{Arc, Mutex};

pub fn init_netcdf_output(
    filename: &str,
    num_flowpaths: usize,
    timesteps: Vec<f64>,
    reference_time: &NaiveDateTime,
) -> Result<Arc<Mutex<FileMut>>> {
    // Create NetCDF file
    let mut file = netcdf::create(filename)
        .with_context(|| format!("Failed to create NetCDF file: {}", filename))?;

    // Add dimensions
    file.add_dimension("feature_id", 0)
        .context("Failed to add feature_id dimension")?;
    file.add_dimension("time", timesteps.len())
        .context("Failed to add time dimension")?;

    // Add variables
    // Time variable
    let mut time_var = file.add_variable::<f64>("time", &["time"])
        .context("Failed to add time variable")?;
    time_var.put_attribute("_FillValue", -9999.0)?;
    time_var.put_attribute("long_name", "valid output time")?;
    time_var.put_attribute("standard_name", "time")?;
    time_var.put_attribute(
        "units",
        format!(
            "seconds since {}",
            reference_time.format("%Y-%m-%d %H:%M:%S")
        ),
    )?;
    time_var.put_attribute("missing_value", -9999.0)?;
    time_var.put_values(&timesteps, ..)
        .context("Failed to write time values")?;

    // Feature ID variable
    let mut feature_var = file.add_variable::<i64>("feature_id", &["feature_id"])
        .context("Failed to add feature_id variable")?;
    feature_var.put_attribute("long_name", "Segment ID")?;

    // Flow variable
    let mut flow_var = file.add_variable::<f32>("flow", &["feature_id", "time"])
        .context("Failed to add flow variable")?;
    flow_var.put_attribute("_FillValue", -9999.0f32)?;
    flow_var.put_attribute("long_name", "Flow")?;
    flow_var.put_attribute("units", "m3 s-1")?;
    flow_var.put_attribute("missing_value", -9999.0f32)?;

    // Velocity variable
    let mut velocity_var = file.add_variable::<f32>("velocity", &["feature_id", "time"])
        .context("Failed to add velocity variable")?;
    velocity_var.put_attribute("_FillValue", -9999.0f32)?;
    velocity_var.put_attribute("long_name", "Velocity")?;
    velocity_var.put_attribute("units", "m/s")?;
    velocity_var.put_attribute("missing_value", -9999.0f32)?;

    // Depth variable
    let mut depth_var = file.add_variable::<f32>("depth", &["feature_id", "time"])
        .context("Failed to add depth variable")?;
    depth_var.put_attribute("_FillValue", -9999.0f32)?;
    depth_var.put_attribute("long_name", "Depth")?;
    depth_var.put_attribute("units", "m")?;
    depth_var.put_attribute("missing_value", -9999.0f32)?;

    // Global attributes
    file.add_attribute("TITLE", "OUTPUT FROM ROUTE_RS")?;
    file.add_attribute(
        "file_reference_time",
        reference_time.format("%Y-%m-%d_%H:%M:%S").to_string(),
    )?;
    file.add_attribute("code_version", "")?;

    // Additional expected variables
    let _ = file.add_variable::<f32>("type", &["feature_id"])?;
    let _ = file.add_variable::<f32>("nudge", &["feature_id"])?;

    Ok(Arc::new(Mutex::new(file)))
}

// Function to write results to NetCDF
pub fn write_output(
    output_file: &Arc<Mutex<FileMut>>,
    results: &SimulationResults,
) -> Result<()> {
    // Get lock on file
    let mut file = output_file.lock()
        .map_err(|e| anyhow::anyhow!("Failed to acquire NetCDF file lock: {}", e))?;

    // Get feature variable
    let mut feature_var = file.variable_mut("feature_id")
        .ok_or_else(|| anyhow::anyhow!("feature_id variable not found"))?;
    let fidx = feature_var.len();
    feature_var.put_value(results.feature_id, fidx)
        .context("Failed to write feature_id")?;

    // Flow variable
    let mut flow_var = file.variable_mut("flow")
        .ok_or_else(|| anyhow::anyhow!("flow variable not found"))?;
    flow_var.put_values(&results.flow_data, (fidx, ..))
        .context("Failed to write flow data")?;

    // Velocity variable
    let mut velocity_var = file.variable_mut("velocity")
        .ok_or_else(|| anyhow::anyhow!("velocity variable not found"))?;
    velocity_var.put_values(&results.velocity_data, (fidx, ..))
        .context("Failed to write velocity data")?;

    // Depth variable
    let mut depth_var = file.variable_mut("depth")
        .ok_or_else(|| anyhow::anyhow!("depth variable not found"))?;
    depth_var.put_values(&results.depth_data, (fidx, ..))
        .context("Failed to write depth data")?;

    Ok(())
}