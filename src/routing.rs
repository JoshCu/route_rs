use crate::config::{ChannelParams, OutputFormat};
use crate::io::results::SimulationResults;
use crate::mc_kernel;
use crate::network::NetworkTopology;
use crate::state::NetworkState;
use csv::Writer;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;

// Process a single timestep
pub fn process_timestep(
    topology: &NetworkTopology,
    network_state: &mut NetworkState,
    channel_params_map: &HashMap<u32, ChannelParams>,
    feature_map: &HashMap<u32, usize>,
    current_external_idx: usize,
    dt: f32,
    step_idx: usize,
    csv_writer: &mut Option<Writer<File>>,
    sim_results: &mut SimulationResults,
    output_format: &OutputFormat,
) -> Result<(), Box<dyn Error>> {
    // Process each node in topological order (MUST be serial due to dependencies)
    for id in &topology.routing_order {
        if let Some(node) = topology.nodes.get(id) {
            if let Some(channel_params) = channel_params_map.get(id) {
                // Get external flow for this timestep
                let external_flow = network_state
                    .external_flows
                    .get(id)
                    .and_then(|flows| flows.get(&current_external_idx))
                    .unwrap_or(&0.0);

                // Get upstream flow (sum of all upstream nodes)
                let upstream_flow = network_state.get_upstream_flow(id, &node.upstream_ids);

                // Get current state values
                let state = network_state.states.get(id).unwrap();
                let (qup, qdp, depth_p) = (state.qup, state.qdp, state.depth_p);

                // Run Muskingcunge routing
                let (qdc, velc, depthc, _, _, _) = mc_kernel::submuskingcunge(
                    qup,
                    upstream_flow, // Use upstream flow as current upstream input
                    qdp,
                    *external_flow,
                    dt,
                    channel_params.s0,
                    channel_params.dx,
                    channel_params.n,
                    channel_params.cs,
                    channel_params.bw,
                    channel_params.tw,
                    channel_params.twcc,
                    channel_params.ncc,
                    depth_p,
                );

                // Update network state
                network_state.update_flow(id, qdc);

                // Update routing state
                let state = network_state.states.get_mut(id).unwrap();
                state.update(upstream_flow, qdc, depthc);

                // Write results at output timesteps (every hour)
                if step_idx % 12 == 0 {
                    // Write to CSV if needed
                    if matches!(output_format, OutputFormat::Csv | OutputFormat::Both) {
                        if let Some(wtr) = csv_writer {
                            wtr.write_record(&[
                                step_idx.to_string(),
                                id.to_string(),
                                qdc.to_string(),
                                velc.to_string(),
                                depthc.to_string(),
                            ])?;
                        }
                    }

                    // Store for NetCDF if needed
                    if matches!(output_format, OutputFormat::NetCdf | OutputFormat::Both) {
                        if let Some(&feature_idx) = feature_map.get(id) {
                            sim_results.add_result(
                                feature_idx,
                                qdc as f32,
                                velc as f32,
                                depthc as f32,
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
