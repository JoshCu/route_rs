use crate::config::{ChannelParams, OutputFormat};
use crate::io::results::SimulationResults;
use crate::mc_kernel;
use crate::network::{NetworkSegment, NetworkTopology};
use crate::state::NetworkState;
use csv::Writer;
use std::collections::{HashMap, HashSet};
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

use rayon::prelude::*;
use std::sync::{Arc, Mutex};

// Process a segment of the network
pub fn process_segment(
    segment: &NetworkSegment,
    topology: &NetworkTopology,
    network_state: &Arc<Mutex<NetworkState>>,
    channel_params_map: &HashMap<u32, ChannelParams>,
    feature_map: &HashMap<u32, usize>,
    current_external_idx: usize,
    dt: f32,
    step_idx: usize,
    output_format: &OutputFormat,
) -> Vec<(u32, f32, f32, f32)> {
    let mut results = Vec::new();

    for &id in &segment.nodes {
        if let Some(node) = topology.nodes.get(&id) {
            if let Some(channel_params) = channel_params_map.get(&id) {
                // Get state values
                let (external_flow, qup, qdp, depth_p, upstream_flow) = {
                    let state_lock = network_state.lock().unwrap();

                    let external_flow = state_lock
                        .external_flows
                        .get(&id)
                        .and_then(|flows| flows.get(&current_external_idx))
                        .copied()
                        .unwrap_or(0.0);

                    let upstream_flow = state_lock.get_upstream_flow(&id, &node.upstream_ids);
                    let state = state_lock.states.get(&id).unwrap();

                    (
                        external_flow,
                        state.qup,
                        state.qdp,
                        state.depth_p,
                        upstream_flow,
                    )
                };

                // Run Muskingcunge routing
                let (qdc, velc, depthc, _, _, _) = mc_kernel::submuskingcunge(
                    qup,
                    upstream_flow,
                    qdp,
                    external_flow,
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

                // Update state
                {
                    let mut state_lock = network_state.lock().unwrap();
                    state_lock.update_flow(&id, qdc);
                    let state = state_lock.states.get_mut(&id).unwrap();
                    state.update(upstream_flow, qdc, depthc);
                }

                // Collect results for output
                if step_idx % 12 == 0 {
                    results.push((id, qdc, velc, depthc));
                }
            }
        }
    }

    results
}

// Parallel timestep processing
pub fn process_timestep_parallel(
    topology: &NetworkTopology,
    segments: &[NetworkSegment],
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
    let network_state_arc = Arc::new(Mutex::new(std::mem::take(network_state)));
    let mut processed_segments = HashSet::new();
    let mut all_results = Vec::new();

    // Process segments in waves
    while processed_segments.len() < segments.len() {
        // Get segments that can be processed in this wave
        let ready_segments = topology.get_parallel_segments(segments, &processed_segments);

        if ready_segments.is_empty() {
            return Err("Circular dependency detected in network segments".into());
        }

        // Process segments in parallel
        let wave_results: Vec<_> = ready_segments
            .par_iter()
            .map(|&seg_id| {
                let segment = &segments[seg_id];
                process_segment(
                    segment,
                    topology,
                    &network_state_arc,
                    channel_params_map,
                    feature_map,
                    current_external_idx,
                    dt,
                    step_idx,
                    output_format,
                )
            })
            .collect();

        // Collect results
        for segment_results in wave_results {
            all_results.extend(segment_results);
        }

        // Mark segments as processed
        for seg_id in ready_segments {
            processed_segments.insert(seg_id);
        }
    }

    // Extract state back
    *network_state = Arc::try_unwrap(network_state_arc)
        .unwrap_or_else(|_| panic!("Failed to unwrap Arc"))
        .into_inner()
        .unwrap();

    // Write results
    if step_idx % 12 == 0 {
        for (id, qdc, velc, depthc) in all_results {
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
                if let Some(&feature_idx) = feature_map.get(&id) {
                    sim_results.add_result(feature_idx, qdc as f32, velc as f32, depthc as f32);
                }
            }
        }
    }

    Ok(())
}
