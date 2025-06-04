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
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

// Message types
enum WriterMessage {
    WriteResults(Arc<SimulationResults>),
    Shutdown,
}

enum WorkerMessage {
    ProcessNode(u32),
    Shutdown,
}

enum SchedulerMessage {
    NodeCompleted(u32),
    Shutdown,
}

// Process all timesteps for a single node (unchanged)
fn process_node_all_timesteps(
    node_id: &u32,
    topology: &NetworkTopology,
    channel_params: &ChannelParams,
    max_timesteps: usize,
    dt: f32,
) -> Result<SimulationResults> {
    let node = topology
        .nodes
        .get(node_id)
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;

    let mut results = SimulationResults::new(node.id as i64);

    let area = node
        .area_sqkm
        .ok_or_else(|| anyhow::anyhow!("Node {} has no area defined", node_id))?;

    let mut external_flows = load_external_flows(node.qlat_file.clone(), &node.id, None, area)?;

    let s0 = if channel_params.s0 == 0.0 {
        0.00001
    } else {
        channel_params.s0
    };

    let mut inflow = node
        .inflow_storage
        .lock()
        .map_err(|e| anyhow::anyhow!("Failed to lock inflow storage: {}", e))?;

    let mut qup = 0.0;
    let mut qdp = 0.0;
    let mut depth_p = 0.0;

    let upsampling = max_timesteps / external_flows.len();

    let mut external_flow = 0.0;
    let mut upstream_flow = 0.0;

    for _timestep in 0..max_timesteps {
        if _timestep % upsampling == 0 {
            external_flow = external_flows.pop_front().unwrap();
        }
        upstream_flow = inflow.pop_front().unwrap_or(0.0);

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

        qup = upstream_flow;
        qdp = qdc;
        depth_p = depthc;
    }

    Ok(results)
}

// Writer thread function (unchanged)
fn writer_thread(
    receiver: Receiver<WriterMessage>,
    output_file: Arc<Mutex<FileMut>>,
) -> Result<()> {
    loop {
        match receiver.recv() {
            Ok(WriterMessage::WriteResults(results)) => {
                if let Err(e) = write_output(&output_file, &results) {
                    eprintln!(
                        "Error writing results for node {}: {}",
                        results.feature_id, e
                    );
                }
            }
            Ok(WriterMessage::Shutdown) => break,
            Err(e) => {
                eprintln!("Writer thread channel error: {}", e);
                break;
            }
        }
    }
    Ok(())
}

// Scheduler thread that tracks dependencies and sends ready work
fn scheduler_thread(
    topology: Arc<NetworkTopology>,
    scheduler_rx: Receiver<SchedulerMessage>,
    worker_tx: Vec<Sender<WorkerMessage>>,
    total_nodes: usize,
    completed_count: Arc<AtomicUsize>,
) -> Result<()> {
    // Track which nodes are ready to process
    let mut ready_nodes = VecDeque::new();
    let mut processed_nodes = HashSet::new();
    let mut pending_downstream_count: HashMap<u32, usize> = HashMap::new();

    // Initialize with leaf nodes (no upstream dependencies)
    for (&node_id, node) in &topology.nodes {
        if node.upstream_ids.is_empty() {
            ready_nodes.push_back(node_id);
        } else {
            // Count how many upstream nodes need to complete
            pending_downstream_count.insert(node_id, node.upstream_ids.len());
        }
    }

    let num_workers = worker_tx.len();
    let mut next_worker = 0;

    loop {
        // Send ready work to workers
        while let Some(node_id) = ready_nodes.pop_front() {
            // Round-robin distribution to workers
            if let Err(e) = worker_tx[next_worker].send(WorkerMessage::ProcessNode(node_id)) {
                eprintln!("Failed to send work to worker {}: {}", next_worker, e);
            }
            next_worker = (next_worker + 1) % num_workers;
        }

        // Wait for completion messages
        match scheduler_rx.recv() {
            Ok(SchedulerMessage::NodeCompleted(node_id)) => {
                processed_nodes.insert(node_id);

                // Check if this enables any downstream nodes
                if let Some(node) = topology.nodes.get(&node_id) {
                    if let Some(downstream_id) = node.downstream_id {
                        if let Some(count) = pending_downstream_count.get_mut(&downstream_id) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                // All upstream nodes are complete, this node is ready
                                ready_nodes.push_back(downstream_id);
                                pending_downstream_count.remove(&downstream_id);
                            }
                        }
                    }
                }

                // Check if we're done
                if processed_nodes.len() >= total_nodes {
                    break;
                }
            }
            Ok(SchedulerMessage::Shutdown) => break,
            Err(e) => {
                eprintln!("Scheduler channel error: {}", e);
                break;
            }
        }
    }

    // Send shutdown to all workers
    for tx in &worker_tx {
        let _ = tx.send(WorkerMessage::Shutdown);
    }

    Ok(())
}

// Worker thread - now just receives work and processes it
fn worker_thread(
    work_rx: Receiver<WorkerMessage>,
    scheduler_tx: Sender<SchedulerMessage>,
    topology: Arc<NetworkTopology>,
    channel_params_map: Arc<HashMap<u32, ChannelParams>>,
    max_timesteps: usize,
    dt: f32,
    writer_tx: Sender<WriterMessage>,
    progress_bar: Arc<ProgressBar>,
) -> Result<()> {
    loop {
        match work_rx.recv() {
            Ok(WorkerMessage::ProcessNode(node_id)) => {
                // Process the node
                if let Some(params) = channel_params_map.get(&node_id) {
                    match process_node_all_timesteps(&node_id, &topology, params, max_timesteps, dt)
                    {
                        Ok(results) => {
                            let results_arc = Arc::new(results);

                            // Send results to writer
                            if let Err(e) = writer_tx
                                .send(WriterMessage::WriteResults(Arc::clone(&results_arc)))
                            {
                                eprintln!("Failed to send results to writer: {}", e);
                            }

                            // Pass flow to downstream node
                            if let Some(node) = topology.nodes.get(&node_id) {
                                if let Some(downstream_id) = node.downstream_id {
                                    if let Some(downstream_node) =
                                        topology.nodes.get(&downstream_id)
                                    {
                                        let mut buffer =
                                            downstream_node.inflow_storage.lock().map_err(|e| {
                                                anyhow::anyhow!(
                                                    "Failed to lock downstream buffer: {}",
                                                    e
                                                )
                                            })?;
                                        if buffer.is_empty() {
                                            buffer.resize(results_arc.flow_data.len(), 0.0);
                                        }
                                        for (i, &flow) in results_arc.flow_data.iter().enumerate() {
                                            if i < buffer.len() {
                                                buffer[i] += flow;
                                            }
                                        }
                                    }
                                }

                                // Update status
                                let mut status = node.status.write().map_err(|e| {
                                    anyhow::anyhow!("Failed to acquire status write lock: {}", e)
                                })?;
                                *status = NodeStatus::Ready;

                                // Clear inflow storage
                                let mut old_inflow = node.inflow_storage.lock().map_err(|e| {
                                    anyhow::anyhow!("Failed to lock inflow storage: {}", e)
                                })?;
                                old_inflow.clear();
                            }
                        }
                        Err(e) => {
                            eprintln!("Error processing node {}: {}", node_id, e);
                        }
                    }

                    progress_bar.inc(1);
                }

                // Notify scheduler that node is complete
                if let Err(e) = scheduler_tx.send(SchedulerMessage::NodeCompleted(node_id)) {
                    eprintln!("Failed to notify scheduler of completion: {}", e);
                }
            }
            Ok(WorkerMessage::Shutdown) => break,
            Err(e) => {
                eprintln!("Worker channel error: {}", e);
                break;
            }
        }
    }
    Ok(())
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
    let total_nodes = topology.nodes.len();
    let completed_count = Arc::new(AtomicUsize::new(0));
    let topology_arc = Arc::new(topology.clone());
    let channel_params_arc = Arc::new(channel_params_map.clone());

    // Create channels
    let (writer_tx, writer_rx) = mpsc::channel();
    let (scheduler_tx, scheduler_rx) = mpsc::channel();

    // Create worker channels
    let num_threads = num_cpus::get();
    println!(
        "Using {} worker threads for parallel processing",
        num_threads
    );

    let mut worker_txs = Vec::new();
    let mut worker_handles = Vec::new();

    // Spawn worker threads
    for i in 0..num_threads {
        let (work_tx, work_rx) = mpsc::channel();
        worker_txs.push(work_tx);

        let topo = Arc::clone(&topology_arc);
        let params = Arc::clone(&channel_params_arc);
        let writer = writer_tx.clone();
        let scheduler = scheduler_tx.clone();
        let pb = Arc::clone(&progress_bar);

        let handle = thread::spawn(move || {
            if let Err(e) = worker_thread(
                work_rx,
                scheduler,
                topo,
                params,
                max_timesteps,
                dt,
                writer,
                pb,
            ) {
                eprintln!("Worker {} error: {}", i, e);
            }
        });
        worker_handles.push(handle);
    }

    // Spawn writer thread
    let output_file_clone = Arc::clone(&output_file);
    let writer_handle = thread::spawn(move || {
        if let Err(e) = writer_thread(writer_rx, output_file_clone) {
            eprintln!("Writer thread error: {}", e);
        }
    });

    // Spawn scheduler thread
    let topo = Arc::clone(&topology_arc);
    let completed = Arc::clone(&completed_count);
    let scheduler_handle = thread::spawn(move || {
        if let Err(e) = scheduler_thread(topo, scheduler_rx, worker_txs, total_nodes, completed) {
            eprintln!("Scheduler thread error: {}", e);
        }
    });

    // Drop original senders
    drop(writer_tx);
    drop(scheduler_tx);

    // Wait for all threads to complete
    scheduler_handle
        .join()
        .map_err(|e| anyhow::anyhow!("Scheduler thread panicked: {:?}", e))?;

    for (i, handle) in worker_handles.into_iter().enumerate() {
        handle
            .join()
            .map_err(|e| anyhow::anyhow!("Worker thread {} panicked: {:?}", i, e))?;
    }

    writer_handle
        .join()
        .map_err(|e| anyhow::anyhow!("Writer thread panicked: {:?}", e))?;

    progress_bar.finish_with_message("Complete");
    println!("Successfully processed all {} nodes", total_nodes);

    Ok(())
}
