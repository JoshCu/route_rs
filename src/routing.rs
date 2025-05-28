use crate::config::{ChannelParams, OutputFormat};
use crate::io::csv::load_external_flows;
use crate::io::netcdf::write_output;
use crate::io::results::SimulationResults;
use crate::mc_kernel;
use crate::network::NetworkTopology;
use crate::state::NodeStatus;
use csv::Writer;
use netcdf::FileMut;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
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
) -> SimulationResults {
    let node = topology.nodes.get(&node_id).unwrap();
    let mut results = SimulationResults::new(node.id as i64);
    let mut external_flows = load_external_flows(
        node.qlat_file.clone(),
        &node.id,
        None,
        node.area_sqkm.unwrap(),
    )
    .unwrap_or_default();

    let s0 = if channel_params.s0 == 0.0 {
        0.00001
    } else {
        channel_params.s0
    };

    let mut inflow = node.inflow_storage.lock().unwrap();

    let mut qup = 0.0;
    let mut qdp = 0.0;
    let mut depth_p = 0.0;

    for timestep in 0..max_timesteps {
        // Get external flow
        let external_flow = external_flows.pop_front().unwrap_or(0.0);

        // Get upstream flow
        let upstream_flow = inflow.pop_front().unwrap_or(0.0);

        // Run routing
        let (qdc, velc, depthc, _, _, _) = mc_kernel::submuskingcunge(
            qup,
            upstream_flow,
            qdp,
            external_flow,
            dt,
            s0,
            channel_params.dx,
            channel_params.n,
            channel_params.cs,
            channel_params.bw,
            channel_params.tw,
            channel_params.twcc,
            channel_params.ncc,
            depth_p,
        );
        results.flow_data.push(qdc);
        results.velocity_data.push(velc);
        results.depth_data.push(depthc);

        // Update previous state
        qup = upstream_flow;
        qdp = qdc;
        depth_p = depthc;
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
    output_file: Arc<Mutex<FileMut>>,
) {
    loop {
        // Try to get work from the queue
        let node_id = {
            if completed_count.load(Ordering::Relaxed) >= total_nodes {
                break;
            }
            let mut queue = work_queue.lock().unwrap();
            queue.pop_front()
        };

        // If no work available, wait a bit
        let node_id = match node_id {
            Some(id) => id,
            None => {
                thread::sleep(Duration::from_millis(50));
                thread::yield_now();
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
            // Put back at the end of the queue
            {
                let mut queue = work_queue.lock().unwrap();
                queue.push_back(node_id);
            }
            // Yield to let other threads work
            thread::sleep(Duration::from_millis(50));
            thread::yield_now();
            continue;
        }

        // Process node
        if let Some(params) = channel_params_map.get(&node_id) {
            let results =
                process_node_all_timesteps(&node_id, &topology, params, max_timesteps, dt);

            // Store results
            {
                // write results to netcdf
                let _ = write_output(&output_file, &results);
                let down_streamnode = topology.nodes[&node_id].downstream_id;
                if let Some(down_streamnode) = down_streamnode {
                    let mut buffer = topology.nodes[&down_streamnode]
                        .inflow_storage
                        .lock()
                        .unwrap();
                    if buffer.is_empty() {
                        buffer.resize(results.flow_data.len(), 0.0);
                    }
                    for q in 0..buffer.len() {
                        buffer[q] += results.flow_data[q];
                    }
                }
                println!("done with node {}", node_id);
                let mut status = node.status.write().unwrap();
                *status = NodeStatus::Ready;
                // clear this nodes inflow storage
                let mut old_inflow = topology.nodes[&node_id].inflow_storage.lock().unwrap();
                old_inflow.clear();
            }
            // Mark as completed
            completed_count.fetch_add(1, Ordering::Relaxed);
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
    max_timesteps: usize,
    dt: f32,
    output_file: Arc<Mutex<FileMut>>,
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
        let output = Arc::clone(&output_file);

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
                output,
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

    Ok(())
}
