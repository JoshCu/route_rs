use crate::config::ChannelParams;
use crate::io::csv::load_external_flows;
use crate::io::netcdf::write_batch;
use crate::io::results::SimulationResults;
use crate::kernel::muskingum::{MuskingumCungeInput, MuskingumCungeKernel, MuskingumCungeResult};
use crate::network::NetworkTopology;
use crate::state::NodeStatus;
use crate::cli::CfgContext;
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use netcdf::FileMut;
use rusqlite::config;
use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicUsize;
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
    // kernel: MuskingumCungeKernel,
    node_id: &u32,
    topology: &NetworkTopology,
    channel_params: &ChannelParams,
    max_timesteps: usize,
    dt: f32,
    config_args: &CfgContext,
) -> Result<SimulationResults> {
    let kernel: MuskingumCungeKernel = config_args.kernel;
    let node = topology
        .nodes
        .get(node_id)
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;

    let mut results = SimulationResults::new(node.id as i64);

    let area = node
        .area_sqkm
        .ok_or_else(|| anyhow::anyhow!("Node {} has no area defined", node_id))?;

    let mut external_flows =
        load_external_flows(node.qlat_file.clone(), &node.id, Some(&"Q_OUT"), area)?;

    let s0 = if channel_params.s0 == 0.0 {
        0.00001
    } else {
        channel_params.s0
    };
    let mut inflow = node
        .inflow_storage
        .lock()
        .map_err(|e| anyhow::anyhow!("Failed to lock inflow storage: {}", e))?;

    if inflow.len() == 0 && external_flows.len() == 0 {
        // if these are both empty then just return all zeros to the results
        results.flow_data = vec![0.0; max_timesteps];
        results.velocity_data = vec![0.0; max_timesteps];
        results.depth_data = vec![0.0; max_timesteps];
        return Ok(results);
    }

    // if headwater then upstream inflow is 0.0
    if inflow.len() == 0 {
        inflow.resize(max_timesteps, 0.0);
    }

    if external_flows.len() == 0 {
        external_flows.resize(max_timesteps, 0.0);
    } else if external_flows.len() == 1 {
        // Only a single external flow value breaks the upsampling logic,
        // so we throw an error if the file only contains one value (which is likely a mistake)
        return Err(anyhow::anyhow!(
            "External flow file for node {} only contains one value, which is not sufficient for routing. Please check the file: {:?}",
            node_id,
            node.qlat_file
        )).with_context(|| format!("Failed to load external flows for node {}: {:?}", node_id, node.qlat_file));
    }

    let mut qup = 0.0;
    let mut qdp = 0.0;
    let mut depth_p = 0.0;
    // -1 because the input files have one additional timestep
    let upsampling = max_timesteps / (external_flows.len() - 1);

    let mut external_flow = 0.0;
    // let mut upstream_flow = 0.0;

    for _timestep in 0..max_timesteps {
        if _timestep % upsampling == 0 {
            external_flow = external_flows.pop_front().ok_or_else(|| {
                anyhow::anyhow!(
                    "Failed to fetch qlateral from file for: {} at timestep {}",
                    node_id,
                    _timestep
                )
            })?;
        }
        let upstream_flow = inflow.pop_front().unwrap();

        let result: MuskingumCungeResult = kernel.exec(
            &MuskingumCungeInput {
                dt,
                qup,
                quc: upstream_flow,
                qdp,
                ql: external_flow,
                dx: channel_params.dx,
                bw: channel_params.bw,
                tw: channel_params.tw,
                tw_cc: channel_params.twcc,
                n: channel_params.n,
                n_cc: channel_params.ncc,
                cs: channel_params.cs,
                s0,
                velp: 0.0, // unused
                depthp: depth_p,
            },
            false,
        );
        let (qdc, velc, depthc) = (result.qdc, result.velc, result.depthc);
        // let (qdc, velc, depthc, _, _, _) = mc_kernel::submuskingcunge(
        //     qup,
        //     upstream_flow,
        //     qdp,
        //     external_flow,
        //     dt,
        //     s0,
        //     channel_params.dx,
        //     channel_params.n,
        //     channel_params.cs,
        //     channel_params.bw,
        //     channel_params.tw,
        //     channel_params.twcc,
        //     channel_params.ncc,
        //     depth_p,
        //     false,
        // );

        results.flow_data.push(qdc);
        results.velocity_data.push(velc);
        results.depth_data.push(depthc);

        qup = upstream_flow;
        qdp = qdc;
        depth_p = depthc;
    }

    Ok(results)
}

fn writer_thread(
    receiver: Receiver<WriterMessage>,
    output_file: Arc<Mutex<FileMut>>,
    batch_size: usize, // e.g., 100 nodes
) -> Result<()> {
    let mut batch = Vec::new();

    loop {
        match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(WriterMessage::WriteResults(results)) => {
                batch.push(results);

                // Write when batch is full
                if batch.len() >= batch_size {
                    write_batch(&output_file, &batch)?;
                    batch.clear();
                }
            }
            Ok(WriterMessage::Shutdown) => {
                // Write remaining batch
                if !batch.is_empty() {
                    write_batch(&output_file, &batch)?;
                }
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Write partial batch on timeout to avoid holding data too long
                if !batch.is_empty() {
                    write_batch(&output_file, &batch)?;
                    batch.clear();
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // All senders dropped — normal shutdown
                if !batch.is_empty() {
                    write_batch(&output_file, &batch)?;
                }
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
    _completed_count: Arc<AtomicUsize>,
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
                                // Prioritize downstream nodes to free inflow buffers sooner
                                ready_nodes.push_front(downstream_id);
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

// Downsample full-resolution results to output frequency
fn downsample_results(results: SimulationResults, downsampling: usize) -> SimulationResults {
    if downsampling <= 1 {
        return results;
    }
    let actual_timesteps = results.flow_data.len();
    let mut flow_data = Vec::with_capacity(actual_timesteps / downsampling);
    let mut velocity_data = Vec::with_capacity(actual_timesteps / downsampling);
    let mut depth_data = Vec::with_capacity(actual_timesteps / downsampling);
    for i in (downsampling - 1..actual_timesteps).step_by(downsampling) {
        flow_data.push(results.flow_data[i]);
        velocity_data.push(results.velocity_data[i]);
        depth_data.push(results.depth_data[i]);
    }
    SimulationResults {
        feature_id: results.feature_id,
        flow_data,
        velocity_data,
        depth_data,
    }
}

// Worker thread - now just receives work and processes it
fn worker_thread(
    // kernel: MuskingumCungeKernel,
    work_rx: Receiver<WorkerMessage>,
    scheduler_tx: Sender<SchedulerMessage>,
    topology: Arc<NetworkTopology>,
    channel_params_map: Arc<HashMap<u32, ChannelParams>>,
    max_timesteps: usize,
    dt: f32,
    downsampling: usize,
    writer_tx: Sender<WriterMessage>,
    progress_bar: Arc<ProgressBar>,
    config_args: CfgContext,
) -> Result<()> {
    loop {
        match work_rx.recv() {
            Ok(WorkerMessage::ProcessNode(node_id)) => {
                // Process the node
                if let Some(params) = channel_params_map.get(&node_id) {
                    match process_node_all_timesteps(
                        // kernel,
                        &node_id,
                        &topology,
                        params,
                        max_timesteps,
                        dt,
                        &config_args,
                    ) {
                        Ok(results) => {
                            // Pass full-resolution flow to downstream node
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
                                let mut status = node.status.write().map_err(|e| {
                                    anyhow::anyhow!("Failed to acquire status write lock: {}", e)
                                })?;
                                *status = NodeStatus::Ready;

                                // Free inflow storage memory
                                let mut old_inflow = node.inflow_storage.lock().map_err(|e| {
                                    anyhow::anyhow!("Failed to lock inflow storage: {}", e)
                                })?;
                                *old_inflow = VecDeque::new();
                            }

                            // Downsample then send to writer
                            let downsampled = downsample_results(results, downsampling);
                            if let Err(e) =
                                writer_tx.send(WriterMessage::WriteResults(Arc::new(downsampled)))
                            {
                                eprintln!("Failed to send results to writer: {}", e);
                            }
                        }
                        Err(e) => {
                            let mut error_message =
                                format!("Error processing node {}: {}", node_id, e);
                            // if error context, elaborate on it
                            if let Some(context) = e.chain().skip(1).next() {
                                error_message.push_str(&format!("\nContext: {}", context));
                            }
                            eprintln!("{}", error_message);
                            writer_tx.send(WriterMessage::Shutdown).ok();
                            scheduler_tx.send(SchedulerMessage::Shutdown).ok();
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
// pub fn process_routing_parallel(
//     kernel: MuskingumCungeKernel,
//     topology: Arc<NetworkTopology>,
//     channel_params_map: Arc<HashMap<u32, ChannelParams>>,
//     max_timesteps: usize,
//     dt: f32,
//     downsampling: usize,
//     output_file: Arc<Mutex<FileMut>>,
//     progress_bar: Arc<ProgressBar>,
//     num_threads: usize,
// ) -> Result<()> {
pub fn process_routing_parallel(
    topology: Arc<NetworkTopology>,
    channel_params_map: Arc<HashMap<u32, ChannelParams>>,
    max_timesteps: usize,
    dt: f32,
    downsampling: usize,
    output_file: Arc<Mutex<FileMut>>,
    progress_bar: Arc<ProgressBar>,
    config_args: &CfgContext,
) -> Result<()> {
    // let kernel: MuskingumCungeKernel = config_args.kernel;
    // let num_threads: usize = config_args.num_threads;
    let total_nodes: usize = topology.nodes.len();
    let completed_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let topology_arc: Arc<NetworkTopology> = topology;
    let channel_params_arc: Arc<HashMap<u32, ChannelParams>> = channel_params_map;

    // Create channels
    let (writer_tx, writer_rx) = mpsc::channel();
    let (scheduler_tx, scheduler_rx) = mpsc::channel();

    // Create worker channels
    println!(
        "Using {} worker threads for parallel processing",
        config_args.num_threads
    );

    let mut worker_txs = Vec::new();
    let mut worker_handles = Vec::new();

    // Spawn worker threads
    for i in 0..config_args.num_threads {
        let (work_tx, work_rx) = mpsc::channel();
        worker_txs.push(work_tx);

        let topo: Arc<NetworkTopology> = Arc::clone(&topology_arc);
        let params: Arc<HashMap<u32, ChannelParams>> = Arc::clone(&channel_params_arc);
        let writer: Sender<WriterMessage> = writer_tx.clone();
        let scheduler: Sender<SchedulerMessage> = scheduler_tx.clone();
        let pb: Arc<ProgressBar> = Arc::clone(&progress_bar);
        let worker_config_args: CfgContext = config_args.clone();

        let handle = thread::spawn(move || {
            if let Err(e) = worker_thread(
                // /*kernel,*/ config_args.kernel,
                work_rx,
                scheduler,
                topo,
                params,
                max_timesteps,
                dt,
                downsampling,
                writer,
                pb,
                worker_config_args,
            ) {
                eprintln!("Worker {} error: {}", i, e);
            }
        });
        worker_handles.push(handle);
    }

    // Spawn writer thread
    let output_file_clone = Arc::clone(&output_file);
    let writer_handle = thread::spawn(move || {
        if let Err(e) = writer_thread(writer_rx, output_file_clone, min(100, total_nodes)) {
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
