use crate::config::{ChannelParams, OutputFormat};
use crate::io::csv::load_external_flows;
use crate::io::results::SimulationResults;
use crate::mc_kernel;
use crate::network::{NetworkNode, NetworkTopology};
use crate::state::NodeStatus;
use csv::Writer;
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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

// Worker function with message queue pattern
fn worker_thread(
    work_queue: Arc<Mutex<VecDeque<u32>>>,
    completed_count: Arc<AtomicUsize>,
    total_nodes: usize,
    topology: Arc<NetworkTopology>,
    channel_params_map: Arc<HashMap<u32, ChannelParams>>,
    max_timesteps: usize,
    dt: f32,
) {
    let mut retry_count = HashMap::new();

    loop {
        // Try to get work from the queue
        let node_id = {
            let mut queue = work_queue.lock().unwrap();

            // Check if we're done
            if queue.is_empty() && completed_count.load(Ordering::Relaxed) >= total_nodes {
                break;
            }

            queue.pop_front()
        };

        // If no work available, wait a bit
        let node_id = match node_id {
            Some(id) => id,
            None => {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };

        // Get node
        let node = match topology.nodes.get(&node_id) {
            Some(node) => node,
            None => {
                eprintln!("Node {} not found in topology", node_id);
                completed_count.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        // Check if all upstream nodes are ready
        let upstream_ready = node.upstream_ids.iter().all(|up_id| {
            topology
                .nodes
                .get(up_id)
                .and_then(|up_node| up_node.status.read().ok())
                .map(|status| *status == NodeStatus::Ready)
                .unwrap_or(false)
        });

        if !upstream_ready {
            // Track retry attempts to detect potential circular dependencies
            let retries = retry_count.entry(node_id).or_insert(0);
            *retries += 1;

            // if *retries > 100 {
            //     eprintln!(
            //         "Node {} appears to have unresolvable dependencies after 100 retries",
            //         node_id
            //     );
            //     completed_count.fetch_add(1, Ordering::Relaxed);
            //     continue;
            // }

            // Put back at the end of the queue
            {
                let mut queue = work_queue.lock().unwrap();
                queue.push_back(node_id);
            }

            // Yield to let other threads work
            thread::yield_now();
            continue;
        }

        // Process node
        if let Some(params) = channel_params_map.get(&node_id) {
            let results =
                process_node_all_timesteps(&node_id, &topology, params, max_timesteps, dt);

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

        // Mark as completed
        completed_count.fetch_add(1, Ordering::Relaxed);
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

    // Create work queue sorted by depth (deepest first)
    let mut work_items = VecDeque::new();
    for depth_level in (0..=max_depth).rev() {
        for (&node_id, &depth) in &depths {
            if depth == depth_level {
                work_items.push_back(node_id);
            }
        }
    }

    let total_nodes = work_items.len();
    println!(
        "Processing {} nodes with max depth {}",
        total_nodes, max_depth
    );

    // Shared state
    let work_queue = Arc::new(Mutex::new(work_items));
    let completed_count = Arc::new(AtomicUsize::new(0));
    let topology_arc = Arc::new(topology.clone());
    let channel_params_arc = Arc::new(channel_params_map.clone());

    // Spawn worker threads
    let num_threads = num_cpus::get();
    let mut handles = vec![];

    for i in 0..num_threads {
        let queue = Arc::clone(&work_queue);
        let completed = Arc::clone(&completed_count);
        let topo = Arc::clone(&topology_arc);
        let params = Arc::clone(&channel_params_arc);

        let handle = thread::spawn(move || {
            println!("Worker {} started", i);
            worker_thread(
                queue,
                completed,
                total_nodes,
                topo,
                params,
                max_timesteps,
                dt,
            );
            println!("Worker {} finished", i);
        });
        handles.push(handle);
    }

    // Wait for completion
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all nodes were processed
    let final_count = completed_count.load(Ordering::Relaxed);
    if final_count != total_nodes {
        eprintln!(
            "Warning: Only {}/{} nodes were processed",
            final_count, total_nodes
        );
    } else {
        println!("Successfully processed all {} nodes", total_nodes);
    }

    println!("there are {} nodes", topology.nodes.len());

    for (key, node) in topology.nodes.iter() {
        let feature_idx = features.iter().position(|f| *f == *key as i64).unwrap();
        let output = node.flow_storage.lock().unwrap().clone();
        sim_results.add_result(feature_idx, output);
    }

    Ok(())
}
