use crate::io::results::SimulationResults;
use chrono::NaiveDateTime;
use netcdf::{self};
use std::error::Error;
// Function to write results to NetCDF
pub fn write_netcdf_output(
    filename: &str,
    results: &SimulationResults,
    reference_time: &NaiveDateTime,
) -> Result<(), Box<dyn Error>> {
    // Create NetCDF file
    let mut file = netcdf::create(filename)?;

    // Add dimensions
    file.add_dimension("feature_id", results.feature_ids.len())?;
    file.add_dimension("time", results.times.len())?;
    // let type_strlen_dim = file.add_dimension("type_strlen", 2)?;

    // Add variables
    // Time variable
    let mut time_var = file.add_variable::<f64>("time", &["time"])?;
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
    time_var.put_values(&results.times, ..)?;

    // Feature ID variable
    let mut feature_var = file.add_variable::<i64>("feature_id", &["feature_id"])?;
    feature_var.put_attribute("long_name", "Segment ID")?;
    feature_var.put_values(&results.feature_ids, ..)?;

    // Type variable (as string)
    // let mut type_var = file.add_variable::<String>("type", &["feature_id"])?;
    // type_var.put_attribute("long_name", "Type")?;
    //     // Write type strings
    // for (i, ftype) in results.feature_types.iter().enumerate() {
    //     type_var.put_string(&ftype, i)?;
    // }

    // Flow variable
    let mut flow_var = file.add_variable::<f32>("flow", &["feature_id", "time"])?;
    flow_var.put_attribute("_FillValue", -9999.0f32)?;
    flow_var.put_attribute("long_name", "Flow")?;
    flow_var.put_attribute("units", "m3 s-1")?;
    flow_var.put_attribute("missing_value", -9999.0f32)?;
    for i in 0..results.feature_ids.len() {
        flow_var.put_values(&results.flow_data[i], (&i, ..))?;
    }

    // Velocity variable
    let mut velocity_var = file.add_variable::<f32>("velocity", &["feature_id", "time"])?;
    velocity_var.put_attribute("_FillValue", -9999.0f32)?;
    velocity_var.put_attribute("long_name", "Velocity")?;
    velocity_var.put_attribute("units", "m/s")?;
    velocity_var.put_attribute("missing_value", -9999.0f32)?;
    for i in 0..results.feature_ids.len() {
        velocity_var.put_values(&results.velocity_data[i], (&i, ..))?;
    }

    // Depth variable
    let mut depth_var = file.add_variable::<f32>("depth", &["feature_id", "time"])?;
    depth_var.put_attribute("_FillValue", -9999.0f32)?;
    depth_var.put_attribute("long_name", "Depth")?;
    depth_var.put_attribute("units", "m")?;
    depth_var.put_attribute("missing_value", -9999.0f32)?;
    for i in 0..results.feature_ids.len() {
        depth_var.put_values(&results.depth_data[i], (&i, ..))?;
    }

    // Global attributes
    file.add_attribute("TITLE", "OUTPUT FROM ROUTE_RS")?;
    file.add_attribute(
        "file_reference_time",
        reference_time.format("%Y-%m-%d_%H:%M:%S").to_string(),
    )?;
    file.add_attribute("code_version", "")?;

    Ok(())
}
