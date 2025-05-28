use crate::config::ChannelParams;
use crate::io::csv::load_external_flows;
use crate::io::netcdf::write_output;
use crate::io::results::SimulationResults;
use crate::mc_kernel;
use crate::network::NetworkTopology;
use crate::state::NodeStatus;
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use netcdf::FileMut;
use std::collections::{HashMap, VecDeque};
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
) -> Result<SimulationResults> {
    let node = topology.nodes.get(node_id)
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;
    
    let mut results = SimulationResults::new(node.id as i64);
    
    let area = node.area_sqkm
        .ok_or_else(|| anyhow::anyhow!("Node {} has no area defined", node_id))?;
    
    let mut external_flows = load_external_flows(
        node.qlat_file.clone(),
        &node.id,
        None,
        area,
    )?;

    let s0 = if channel_params.s0 == 0.0 {
        0.00001
    } else {
        channel_params.s0
    };

    let mut inflow = node.inflow_storage.lock()
        .map_err(|e| anyhow::anyhow!("Failed to lock inflow storage: {}", e))?;

    let mut qup = 0.0;
    let mut qdp = 0.0;
    let mut depth_p = 0.0;

    for _timestep in 0..max_timesteps {
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
    
    Ok(results)
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
    progress_bar: Arc<ProgressBar>,
) -> Result<()> {
    loop {
        // Check if all work is done
        if completed_count.load(Ordering::Relaxed) >= total_nodes {
            break;
        }

        // Try to get work from the queue
        let node_id = {
            let mut queue = work_queue.lock()
                .map_err(|e| anyhow::anyhow!("Failed to lock work queue: {}", e))?;
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
                progress_bar.inc(1);
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
                let mut queue = work_queue.lock()
                    .map_err(|e| anyhow::anyhow!("Failed to lock work queue: {}", e))?;
                queue.push_back(node_id);
            }
            // Yield to let other threads work
            thread::sleep(Duration::from_millis(50));
            thread::yield_now();
            continue;
        }

        // Process node
        if let Some(params) = channel_params_map.get(&node_id) {
            match process_node_all_timesteps(&node_id, &topology, params, max_timesteps, dt) {
                Ok(results) => {
                    // Write results to netcdf
                    write_output(&output_file, &results)?;
                    
                    // Pass flow to downstream node if exists
                    if let Some(downstream_id) = topology.nodes[&node_id].downstream_id {
                        if let Some(downstream_node) = topology.nodes.get(&downstream_id) {
                            let mut buffer = downstream_node.inflow_storage.lock()
                                .map_err(|e| anyhow::anyhow!("Failed to lock downstream buffer: {}", e))?;
                            if buffer.is_empty() {
                                buffer.resize(results.flow_data.len(), 0.0);
                            }
                            for (i, &flow) in results.flow_data.iter().enumerate() {
                                if i < buffer.len() {
                                    buffer[i] += flow;
                                }
                            }
                        }
                    }
                    
                    // Update status
                    let mut status = node.status.write()
                        .map_err(|e| anyhow::anyhow!("Failed to acquire status write lock: {}", e))?;
                    *status = NodeStatus::Ready;
                    
                    // Clear this node's inflow storage
                    let mut old_inflow = topology.nodes[&node_id].inflow_storage.lock()
                        .map_err(|e| anyhow::anyhow!("Failed to lock inflow storage: {}", e))?;
                    old_inflow.clear();
                }
                Err(e) => {
                    eprintln!("Error processing node {}: {}", node_id, e);
                }
            }
            
            // Mark as completed
            completed_count.fetch_add(1, Ordering::Relaxed);
            progress_bar.inc(1);
        }
    }
    Ok(())
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
    progress_bar: Arc<ProgressBar>,
) -> Result<()> {
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
    println!("Using {} threads for parallel processing", num_threads);
    
    let mut handles = vec![];

    for i in 0..num_threads {
        let queue = Arc::clone(&work_queue);
        let completed = Arc::clone(&completed_count);
        let topo = Arc::clone(&topology_arc);
        let params = Arc::clone(&channel_params_arc);
        let output = Arc::clone(&output_file);
        let pb = Arc::clone(&progress_bar);

        let handle = thread::spawn(move || {
            if let Err(e) = worker_thread(queue, completed, total_nodes, topo, params, max_timesteps, dt, output, pb) {
                eprintln!("Worker {} encountered error: {}", i, e);
            }
        });
        handles.push(handle);
    }

    // Wait for completion
    for (i, handle) in handles.into_iter().enumerate() {
        handle.join()
            .map_err(|e| anyhow::anyhow!("Worker thread {} panicked: {:?}", i, e))?;
    }

    progress_bar.finish_with_message("Complete");

    // Verify all nodes were processed
    let final_count = completed_count.load(Ordering::Relaxed);
    if final_count != total_nodes {
        anyhow::bail!(
            "Only {}/{} nodes were processed",
            final_count, total_nodes
        );
    }

    println!("Successfully processed all {} nodes", total_nodes);
    Ok(())
}