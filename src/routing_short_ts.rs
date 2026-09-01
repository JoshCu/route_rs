// "Assume short timestep" routing engine.
//
// Standard t-route/NWM approximation: assume the current-timestep upstream inflow
// (quc) equals the previous timestep's upstream inflow (qup), instead of the true
// simultaneous sum of upstream outflows. This removes the need to wait for an
// upstream reach's current-timestep result before computing a downstream reach, so
// the whole network can be routed one timestep at a time instead of in strict
// upstream-to-downstream dependency order (see routing.rs's wavefront scheduler).
//
// The approximation only affects the kernel's instantaneous coupling term; after
// every reach finishes a timestep, qup is updated to the *actual* just-computed
// upstream outflow for the next timestep, so error does not compound across the run.

use crate::cli::CfgContext;
use crate::config::ChannelParams;
use crate::io::csv::load_external_flows;
use crate::io::results::SimulationResults;
use crate::kernel::muskingum::{MuskingumCungeInput, MuskingumCungeKernel, MuskingumCungeResult};
use crate::network::NetworkTopology;
use crate::routing::{downsample_results, writer_thread, WriterMessage};
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use netcdf::FileMut;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

struct ShortTsNode {
    upstream_idxs: Vec<usize>,
    params: ChannelParams,
    s0: f32,
    ql_series: Vec<f32>,
    qup: f32,
    qdp: f32,
    depth_p: f32,
    last_qdc: f32,
    results: SimulationResults,
    // Mirrors the wavefront engine's all-zero shortcut for a headwater node with no
    // external flow file: never has real inflow, so its output is always zero.
    trivial_zero: bool,
}

fn build_nodes(
    topology: &NetworkTopology,
    channel_params_map: &HashMap<u32, ChannelParams>,
    max_timesteps: usize,
) -> Result<Vec<ShortTsNode>> {
    // Restrict to nodes with known channel parameters, matching the wavefront
    // engine's silent-skip behavior in routing.rs's worker_thread.
    let mut ids: Vec<u32> = topology
        .nodes
        .keys()
        .copied()
        .filter(|id| channel_params_map.contains_key(id))
        .collect();
    ids.sort_unstable();

    let index_of: HashMap<u32, usize> = ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();

    let mut nodes = Vec::with_capacity(ids.len());
    for &id in &ids {
        let node = topology
            .nodes
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("Node {} not found", id))?;
        let params = channel_params_map
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("Node {} has no channel parameters", id))?
            .clone();

        let area = node
            .area_sqkm
            .ok_or_else(|| anyhow::anyhow!("Node {} has no area defined", id))?;

        let mut external_flows =
            load_external_flows(node.qlat_file.clone(), &id, Some("Q_OUT"), area)?;

        let s0 = if params.s0 == 0.0 { 0.00001 } else { params.s0 };

        let upstream_idxs: Vec<usize> = node
            .upstream_ids
            .iter()
            .filter_map(|uid| index_of.get(uid).copied())
            .collect();

        let trivial_zero = upstream_idxs.is_empty() && external_flows.is_empty();

        let ql_series = if trivial_zero {
            Vec::new()
        } else if external_flows.is_empty() {
            vec![0.0; max_timesteps]
        } else if external_flows.len() == 1 {
            return Err(anyhow::anyhow!(
                "External flow file for node {} only contains one value, which is not sufficient for routing. Please check the file: {:?}",
                id,
                node.qlat_file
            )).with_context(|| format!("Failed to load external flows for node {}: {:?}", id, node.qlat_file));
        } else {
            // -1 because the input files have one additional timestep
            let upsampling = max_timesteps / (external_flows.len() - 1);
            let mut series = Vec::with_capacity(max_timesteps);
            let mut current = 0.0;
            for t in 0..max_timesteps {
                if t % upsampling == 0 {
                    current = external_flows.pop_front().ok_or_else(|| {
                        anyhow::anyhow!(
                            "Failed to fetch qlateral from file for: {} at timestep {}",
                            id,
                            t
                        )
                    })?;
                }
                series.push(current);
            }
            series
        };

        let mut results = SimulationResults::new(id as i64);
        if trivial_zero {
            results.flow_data = vec![0.0; max_timesteps];
            results.velocity_data = vec![0.0; max_timesteps];
            results.depth_data = vec![0.0; max_timesteps];
        } else {
            results.flow_data.reserve(max_timesteps);
            results.velocity_data.reserve(max_timesteps);
            results.depth_data.reserve(max_timesteps);
        }

        nodes.push(ShortTsNode {
            upstream_idxs,
            s0,
            params,
            ql_series,
            qup: 0.0,
            qdp: 0.0,
            depth_p: 0.0,
            last_qdc: 0.0,
            results,
            trivial_zero,
        });
    }

    Ok(nodes)
}

// Compute one node's routing step for `timestep`, assuming quc == qup (the current
// timestep's upstream inflow equals the previous timestep's).
fn compute_node_timestep(
    node: &mut ShortTsNode,
    kernel: MuskingumCungeKernel,
    dt: f32,
    timestep: usize,
) {
    if node.trivial_zero {
        return;
    }
    let result: MuskingumCungeResult = kernel.exec(
        &MuskingumCungeInput {
            dt,
            qup: node.qup,
            quc: node.qup,
            qdp: node.qdp,
            ql: node.ql_series[timestep],
            dx: node.params.dx,
            bw: node.params.bw,
            tw: node.params.tw,
            tw_cc: node.params.twcc,
            n: node.params.n,
            n_cc: node.params.ncc,
            cs: node.params.cs,
            s0: node.s0,
            velp: 0.0, // unused
            depthp: node.depth_p,
        },
        false,
    );

    node.results.flow_data.push(result.qdc);
    node.results.velocity_data.push(result.velc);
    node.results.depth_data.push(result.depthc);

    node.last_qdc = result.qdc;
    node.qdp = result.qdc;
    node.depth_p = result.depthc;
}

// Once every node has finished its (approximate) step for this timestep, the true
// upstream aggregate is known, so set qup to the exact value for the next timestep.
fn aggregate_qup(nodes: &mut [ShortTsNode]) {
    for i in 0..nodes.len() {
        let mut sum = 0.0;
        for &up in &nodes[i].upstream_idxs {
            sum += nodes[up].last_qdc;
        }
        nodes[i].qup = sum;
    }
}

pub fn process_routing_short_timestep(
    topology: Arc<NetworkTopology>,
    channel_params_map: Arc<HashMap<u32, ChannelParams>>,
    max_timesteps: usize,
    dt: f32,
    downsampling: usize,
    output_file: Arc<Mutex<FileMut>>,
    progress_bar: Arc<ProgressBar>,
    config_args: &CfgContext,
) -> Result<()> {
    let kernel: MuskingumCungeKernel = config_args.kernel;
    let mut nodes = build_nodes(&topology, &channel_params_map, max_timesteps)?;
    let total_nodes = nodes.len();

    println!(
        "Using {} worker threads for parallel short-timestep processing across {} nodes",
        config_args.num_threads, total_nodes
    );

    let num_threads = config_args.num_threads.max(1);
    let chunk_size = total_nodes.div_ceil(num_threads).max(1);

    for timestep in 0..max_timesteps {
        // Phase A: every node's step is independent given last timestep's state, so
        // this is embarrassingly parallel — no cross-node dependency within a timestep.
        thread::scope(|s| {
            for chunk in nodes.chunks_mut(chunk_size) {
                s.spawn(move || {
                    for node in chunk.iter_mut() {
                        compute_node_timestep(node, kernel, dt, timestep);
                    }
                });
            }
        });

        // Phase B: cheap serial aggregation now that thread::scope has joined
        // every worker, so plain indexing across nodes is safe.
        aggregate_qup(&mut nodes);

        progress_bar.inc(1);
    }

    let (writer_tx, writer_rx) = mpsc::channel();
    let output_file_clone = Arc::clone(&output_file);
    let writer_handle = thread::spawn(move || {
        if let Err(e) = writer_thread(writer_rx, output_file_clone, total_nodes.clamp(1, 100)) {
            eprintln!("Writer thread error: {}", e);
        }
    });

    for node in nodes {
        let downsampled = downsample_results(node.results, downsampling);
        if let Err(e) = writer_tx.send(WriterMessage::WriteResults(Arc::new(downsampled))) {
            eprintln!("Failed to send results to writer: {}", e);
        }
    }
    drop(writer_tx);

    writer_handle
        .join()
        .map_err(|e| anyhow::anyhow!("Writer thread panicked: {:?}", e))?;

    progress_bar.finish_with_message("Complete");
    println!(
        "Successfully processed all {} nodes (assume short timestep)",
        total_nodes
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_params() -> ChannelParams {
        ChannelParams {
            dx: 1000.0,
            n: 0.06,
            ncc: 0.12,
            s0: 0.001,
            bw: 10.0,
            tw: 50.0,
            twcc: 100.0,
            cs: 1.0,
        }
    }

    fn make_node(upstream_idxs: Vec<usize>, ql: f32, max_timesteps: usize) -> ShortTsNode {
        let params = make_params();
        ShortTsNode {
            upstream_idxs,
            s0: params.s0,
            params,
            ql_series: vec![ql; max_timesteps],
            qup: 0.0,
            qdp: 0.0,
            depth_p: 0.0,
            last_qdc: 0.0,
            results: SimulationResults::new(0),
            trivial_zero: false,
        }
    }

    // The core correctness property the whole feature rests on: even though each
    // node's kernel call approximates quc as qup (last timestep's value), the qup
    // handed to the next timestep must be the *actual* upstream qdc just computed,
    // not the approximation, so error doesn't compound across the run.
    #[test]
    fn aggregate_qup_uses_actual_upstream_output_not_the_approximation() {
        let max_timesteps = 3;
        let kernel = MuskingumCungeKernel::TRouteModernized;
        let dt = 300.0;

        let mut nodes = vec![
            make_node(vec![], 10.0, max_timesteps), // headwater with lateral inflow
            make_node(vec![0], 0.0, max_timesteps), // outlet, fed only by node 0
        ];

        for timestep in 0..max_timesteps {
            for node in nodes.iter_mut() {
                compute_node_timestep(node, kernel, dt, timestep);
            }
            let headwater_qdc = nodes[0].last_qdc;
            assert!(
                headwater_qdc > 0.0,
                "expected headwater to produce positive flow from lateral inflow"
            );

            aggregate_qup(&mut nodes);

            assert_eq!(
                nodes[1].qup, headwater_qdc,
                "outlet's qup for the next timestep must equal the headwater's actual qdc, not the approximation"
            );
        }
    }

    #[test]
    fn trivial_zero_node_never_produces_flow() {
        let max_timesteps = 2;
        let kernel = MuskingumCungeKernel::TRouteModernized;
        let dt = 300.0;

        let mut node = make_node(vec![], 0.0, max_timesteps);
        node.trivial_zero = true;

        for timestep in 0..max_timesteps {
            compute_node_timestep(&mut node, kernel, dt, timestep);
        }

        assert_eq!(node.last_qdc, 0.0);
        assert!(node.results.flow_data.is_empty());
    }
}
