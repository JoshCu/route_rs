use crate::io::results::SimulationResults;
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use netcdf::{self, FileMut};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// Silence HDF5 diagnostic output (e.g. "unable to determine if file is accessible")
// that occurs when netcdf::create checks for an existing file.
unsafe extern "C" {
    fn H5Eset_auto2(
        estack_id: i64,
        func: Option<unsafe extern "C" fn() -> i32>,
        client_data: *mut std::ffi::c_void,
    ) -> i32;
}

pub fn init_netcdf_output(
    output_dir: PathBuf,
    filename: &str,
    _num_flowpaths: usize,
    timesteps: Vec<f64>,
    reference_time: &NaiveDateTime,
) -> Result<Arc<Mutex<FileMut>>> {
    // Suppress HDF5 diagnostic messages during file creation
    unsafe {
        H5Eset_auto2(0, None, std::ptr::null_mut());
    }

    // Create NetCDF file
    let mut file = netcdf::create(output_dir.join(filename))
        .with_context(|| format!("Failed to create NetCDF file: {}", filename))?;

    // Add dimensions
    file.add_dimension("feature_id", _num_flowpaths)
        .context("Failed to add feature_id dimension")?;
    file.add_dimension("time", timesteps.len())
        .context("Failed to add time dimension")?;

    // Add variables
    // Time variable
    let mut time_var = file
        .add_variable::<f64>("time", &["time"])
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
    time_var
        .put_values(&timesteps, ..)
        .context("Failed to write time values")?;

    // Feature ID variable
    let mut feature_var = file
        .add_variable::<i64>("feature_id", &["feature_id"])
        .context("Failed to add feature_id variable")?;
    feature_var.put_attribute("long_name", "Segment ID")?;

    // Flow variable
    let mut flow_var = file
        .add_variable::<f32>("flow", &["feature_id", "time"])
        .context("Failed to add flow variable")?;
    flow_var.put_attribute("_FillValue", -9999.0f32)?;
    flow_var.put_attribute("long_name", "Flow")?;
    flow_var.put_attribute("units", "m3 s-1")?;
    flow_var.put_attribute("missing_value", -9999.0f32)?;

    // Velocity variable
    let mut velocity_var = file
        .add_variable::<f32>("velocity", &["feature_id", "time"])
        .context("Failed to add velocity variable")?;
    velocity_var.put_attribute("_FillValue", -9999.0f32)?;
    velocity_var.put_attribute("long_name", "Velocity")?;
    velocity_var.put_attribute("units", "m/s")?;
    velocity_var.put_attribute("missing_value", -9999.0f32)?;

    // Depth variable
    let mut depth_var = file
        .add_variable::<f32>("depth", &["feature_id", "time"])
        .context("Failed to add depth variable")?;
    depth_var.put_attribute("_FillValue", -9999.0f32)?;
    depth_var.put_attribute("long_name", "Depth")?;
    depth_var.put_attribute("units", "m")?;
    depth_var.put_attribute("missing_value", -9999.0f32)?;

    // Global attributes
    file.add_attribute("TITLE", "OUTPUT FROM RS-ROUTE")?;
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

pub fn write_batch(
    output_file: &Arc<Mutex<FileMut>>,
    batch: &[Arc<SimulationResults>],
    batch_num: usize,
) -> Result<()> {
    let mut file = output_file
        .lock()
        .map_err(|e| anyhow::anyhow!("Failed to acquire NetCDF file lock: {}", e))?;

    let start_idx = batch_num * batch.len();

    // Prepare all data arrays (already downsampled by workers)
    let mut all_feature_ids = Vec::with_capacity(batch.len());
    let mut all_flows = Vec::new();
    let mut all_velocities = Vec::new();
    let mut all_depths = Vec::new();

    for results in batch {
        all_feature_ids.push(results.feature_id);
        all_flows.extend_from_slice(&results.flow_data);
        all_velocities.extend_from_slice(&results.velocity_data);
        all_depths.extend_from_slice(&results.depth_data);
    }

    // Write all feature IDs at once
    let mut feature_var = file
        .variable_mut("feature_id")
        .ok_or_else(|| anyhow::anyhow!("feature_id variable not found"))?;
    feature_var
        .put_values(&all_feature_ids, start_idx..)
        .context("Failed to write feature_ids")?;

    let end_idx = start_idx + all_feature_ids.len();

    // flow/velocity/depth are already flat in row-major (feature, time) order,
    // matching the variable layout, so each can be written in a single call.
    let mut flow_var = file
        .variable_mut("flow")
        .ok_or_else(|| anyhow::anyhow!("flow variable not found"))?;
    flow_var
        .put_values(&all_flows, (start_idx..end_idx, ..))
        .context("Failed to write flow data")?;

    let mut velocity_var = file
        .variable_mut("velocity")
        .ok_or_else(|| anyhow::anyhow!("velocity variable not found"))?;
    velocity_var
        .put_values(&all_velocities, (start_idx..end_idx, ..))
        .context("Failed to write velocity data")?;

    let mut depth_var = file
        .variable_mut("depth")
        .ok_or_else(|| anyhow::anyhow!("depth variable not found"))?;
    depth_var
        .put_values(&all_depths, (start_idx..end_idx, ..))
        .context("Failed to write depth data")?;

    Ok(())
}

// Function to write results to NetCDF
pub fn _write_output(
    output_file: &Arc<Mutex<FileMut>>,
    results: &Arc<SimulationResults>,
) -> Result<()> {
    // Get lock on file
    let mut file = output_file
        .lock()
        .map_err(|e| anyhow::anyhow!("Failed to acquire NetCDF file lock: {}", e))?;

    // figure out the downsampling that needs to be done
    let expected_timesteps = file
        .dimension("time")
        .ok_or_else(|| anyhow::anyhow!("time dimension not found"))?;
    let actual_timesteps = results.flow_data.len();
    let downsampling = actual_timesteps / expected_timesteps.len();
    let mut downsampled_flow_data = Vec::with_capacity(expected_timesteps.len());
    let mut downsampled_velocity_data = Vec::with_capacity(expected_timesteps.len());
    let mut downsampled_depth_data = Vec::with_capacity(expected_timesteps.len());
    for i in (downsampling - 1..actual_timesteps).step_by(downsampling) {
        downsampled_flow_data.push(results.flow_data[i]);
        downsampled_velocity_data.push(results.velocity_data[i]);
        downsampled_depth_data.push(results.depth_data[i]);
    }

    // Get feature variable
    let mut feature_var = file
        .variable_mut("feature_id")
        .ok_or_else(|| anyhow::anyhow!("feature_id variable not found"))?;
    let fidx = feature_var.len();
    feature_var
        .put_value(results.feature_id, fidx)
        .context("Failed to write feature_id")?;

    // Flow variable
    let mut flow_var = file
        .variable_mut("flow")
        .ok_or_else(|| anyhow::anyhow!("flow variable not found"))?;
    flow_var
        .put_values(&downsampled_flow_data, (fidx, ..))
        .context("Failed to write flow data")?;

    // Velocity variable
    let mut velocity_var = file
        .variable_mut("velocity")
        .ok_or_else(|| anyhow::anyhow!("velocity variable not found"))?;
    velocity_var
        .put_values(&downsampled_velocity_data, (fidx, ..))
        .context("Failed to write velocity data")?;

    // Depth variable
    let mut depth_var = file
        .variable_mut("depth")
        .ok_or_else(|| anyhow::anyhow!("depth variable not found"))?;
    depth_var
        .put_values(&downsampled_depth_data, (fidx, ..))
        .context("Failed to write depth data")?;

    Ok(())
}
