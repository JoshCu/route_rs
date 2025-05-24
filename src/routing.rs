use crate::config::{ChannelParams, OutputFormat};
use crate::io::csv::load_external_flows;
use crate::io::results::SimulationResults;
use crate::mc_kernel;
use crate::network::{NetworkNode, NetworkTopology};
use crate::state::NodeStatus;
use csv::Writer;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

// Process all timesteps for a single node
fn process_node_all_timesteps(
    node_id: &u32,
    topology: &NetworkTopology,
    channel_params: &ChannelParams,
    max_timesteps: usize,
    dt: f32,
) -> Vec<(usize, f32, f32, f32)> {
    let node = topology.nodes.get(&node_id).unwrap();
    let mut results = Vec::with_capacity(max_timesteps);
    let mut external_flows = load_external_flows(
        node.qlat_file.clone(),
        &node.id,
        None,
        node.area_sqkm.unwrap(),
    )
    .unwrap_or_default();

    let aggregated_flows: Vec<f32> = {
        let sums: HashMap<usize, f32> = node
            .upstream_ids
            .iter()
            .filter_map(|&up_id| topology.nodes.get(&up_id))
            .flat_map(|upstream_node| upstream_node.flow_storage.lock().unwrap().clone())
            .fold(HashMap::new(), |mut acc, (key, v1, _, _)| {
                *acc.entry(key).or_insert(0.0) += v1;
                acc
            });

        // Sort by key, then extract just the values
        let mut sorted: Vec<_> = sums.into_iter().collect();
        sorted.sort_by_key(|&(k, _)| k);
        sorted.into_iter().map(|(_, v)| v).collect()
    };

    let mut aggregated_flows = VecDeque::from(aggregated_flows);

    for timestep in 0..max_timesteps {
        // Get external flow
        let external_flow = external_flows.pop_front().unwrap_or(0.0);

        // Get upstream flow
        let upstream_flow = aggregated_flows.pop_front().unwrap_or(0.0);

        // Get previous state
        let (qup, qdp, depth_p) = if timestep == 0 {
            (0.0, 0.0, 0.0)
        } else {
            let (_, prev_qdc, _, prev_depthc) = results[timestep - 1];
            (upstream_flow, prev_qdc, prev_depthc)
        };

        // Run routing
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
        results.push((timestep, qdc, velc, depthc));
    }
    results
}

// Worker function
fn worker_thread(
    work_queue: Arc<Vec<u32>>,
    work_index: Arc<AtomicUsize>,
    topology: Arc<NetworkTopology>,
    channel_params_map: Arc<HashMap<u32, ChannelParams>>,
    max_timesteps: usize,
    dt: f32,
) {
    loop {
        // Get next work item
        let idx = work_index.fetch_add(1, Ordering::Relaxed);
        if idx >= work_queue.len() {
            break;
        }

        let node_id = &work_queue[idx];
        let topo = &topology;
        // Check dependencies
        let node = match topo.nodes.get(node_id) {
            Some(node) => node,
            None => continue,
        };

        // check if the status of all the upstream ids is
        let upstream_status = node.upstream_ids.iter().all(|node_id| {
            topo.nodes
                .get(node_id)
                .unwrap()
                .status
                .read()
                .unwrap()
                .eq(&NodeStatus::Ready)
        });

        if !upstream_status {
            // Put back in queue
            work_index.fetch_sub(1, Ordering::Relaxed);
            thread::yield_now();
            continue;
        }

        // Process node
        if let Some(params) = channel_params_map.get(&node_id) {
            let results = process_node_all_timesteps(node_id, &topology, params, max_timesteps, dt);

            // Store results
            {
                let mut buffer = node.flow_storage.lock().unwrap();
                if buffer.is_empty() {
                    buffer.extend(results);
                }
                println!("done with node {}", node_id);
                let mut status = node.status.write().unwrap();
                *status = NodeStatus::Ready;
            }
        }
    }
}

// Compute depths from leaves
fn compute_depths(topology: &NetworkTopology) -> HashMap<u32, usize> {
    let mut depths = HashMap::new();

    // Process in reverse routing order
    for &node_id in topology.routing_order.iter().rev() {
        let node = &topology.nodes[&node_id];

        if node.upstream_ids.is_empty() {
            depths.insert(node_id, 0);
        } else {
            let max_upstream = node
                .upstream_ids
                .iter()
                .map(|&up_id| depths.get(&up_id).copied().unwrap_or(0))
                .max()
                .unwrap_or(0);
            depths.insert(node_id, max_upstream + 1);
        }
    }

    depths
}

// Main parallel routing function
pub fn process_routing_parallel(
    topology: &NetworkTopology,
    channel_params_map: &HashMap<u32, ChannelParams>,
    feature_map: &HashMap<u32, usize>,
    max_timesteps: usize,
    dt: f32,
    csv_writer: &mut Option<Writer<File>>,
    sim_results: &mut SimulationResults,
    output_format: &OutputFormat,
    features: &Vec<i64>,
) -> Result<(), Box<dyn Error>> {
    // Compute node depths and create work queue
    let depths = compute_depths(topology);
    let max_depth = *depths.values().max().unwrap_or(&0);

    // Create work items sorted by depth (deepest first)
    let mut work_items = Vec::new();
    for depth_level in (0..=max_depth).rev() {
        for (&node_id, &depth) in &depths {
            if depth == depth_level {
                work_items.push(node_id);
            }
        }
    }

    println!(
        "Processing {} nodes with max depth {}",
        work_items.len(),
        max_depth
    );

    // Shared state
    let work_index = Arc::new(AtomicUsize::new(0));
    let work_queue = Arc::new(work_items);
    let topology_arc = Arc::new(topology.clone());
    let channel_params_arc = Arc::new(channel_params_map.clone());

    // Spawn worker threads
    let num_threads = num_cpus::get();
    let mut handles = vec![];

    for _ in 0..num_threads {
        let queue = Arc::clone(&work_queue);
        let topo = Arc::clone(&topology_arc);
        let params = Arc::clone(&channel_params_arc);
        let index = Arc::clone(&work_index);
        let handle = thread::spawn(move || {
            worker_thread(queue, index, topo, params, max_timesteps, dt);
        });
        handles.push(handle);
    }

    // Wait for completion
    for handle in handles {
        handle.join().unwrap();
    }

    println!("there are {} nodes", topology.nodes.len());

    for (key, node) in topology.nodes.iter() {
        let feature_idx = features.iter().position(|f| *f == *key as i64).unwrap();
        let output = node.flow_storage.lock().unwrap().clone();
        sim_results.add_result(feature_idx, output);
    }

    Ok(())
}
