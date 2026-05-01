use crate::cli;
use crate::config::{ChannelParams, ColumnConfig};
use crate::state::NodeStatus;
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

// Network node representing a catchment/nexus
#[derive(Debug, Clone)]
pub struct NetworkNode {
    pub id: u32,
    pub downstream_id: Option<u32>,
    pub upstream_ids: Vec<u32>,
    pub area_sqkm: Option<f32>,
    pub status: Arc<RwLock<NodeStatus>>,
    pub qlat_file: PathBuf,
    pub inflow_storage: Arc<Mutex<VecDeque<f32>>>,
}

impl NetworkNode {
    pub fn new(
        id: u32,
        downstream_id: Option<u32>,
        area_sqkm: Option<f32>,
        qlat_file: PathBuf,
    ) -> Self {
        NetworkNode {
            id,
            downstream_id,
            upstream_ids: Vec::new(),
            area_sqkm,
            status: Arc::new(RwLock::new(NodeStatus::NotReady)),
            qlat_file,
            inflow_storage: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

// Network topology
#[derive(Debug, Clone)]
pub struct NetworkTopology {
    pub nodes: HashMap<u32, NetworkNode>,
    pub routing_order: Vec<u32>,
}

impl NetworkTopology {
    pub fn new() -> Self {
        NetworkTopology {
            nodes: HashMap::new(),
            routing_order: Vec::new(),
        }
    }

    pub fn add_node(
        &mut self,
        id: u32,
        downstream_id: Option<u32>,
        area_sqkm: Option<f32>,
        qlat_file: PathBuf,
    ) {
        let node = NetworkNode::new(id, downstream_id, area_sqkm, qlat_file);
        self.nodes.insert(id, node);
    }

    pub fn build_upstream_connections(&mut self) {
        let mut upstream_map: HashMap<u32, Vec<u32>> = HashMap::new();

        for (id, node) in &self.nodes {
            if let Some(downstream) = &node.downstream_id {
                upstream_map
                    .entry(*downstream)
                    .or_insert_with(Vec::new)
                    .push(*id);
            }
        }

        for (id, upstreams) in upstream_map {
            if let Some(node) = self.nodes.get_mut(&id) {
                node.upstream_ids = upstreams;
            }
        }
    }

    pub fn collect_node_ids(&mut self) {
        self.routing_order = self.nodes.keys().copied().collect();
    }
}

// Function to build network topology from database
pub fn build_network_topology(
    conn: &Connection,
    config: &ColumnConfig,
    csv_dir: &PathBuf,
    _config_args: &cli::CfgContext,
) -> Result<NetworkTopology> {
    let mut topology = NetworkTopology::new();

    let network_query = format!(
        "SELECT {}, {}, areasqkm FROM 'flowpaths' WHERE {} IS NOT NULL GROUP BY {}",
        config.key, config.downstream, config.downstream, config.key
    );
    let mut stmt = conn
        .prepare(&network_query)
        .context("Failed to prepare network query")?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f32>(2)?,
        ))
    })?;

    for row in rows {
        let (id, downstream_id, area_sqkm) = row.context("Failed to read row")?;

        let n_id = id
            .split('-')
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid ID format: {}", id))?;

        let n_downstream_id = downstream_id
            .split('-')
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid toID format: {}", downstream_id))?;

        let qlat_file_path = csv_dir.join(format!("cat-{}.csv", n_id));
        topology.add_node(n_id, Some(n_downstream_id), Some(area_sqkm), qlat_file_path);
    }

    // Build upstream connections
    topology.build_upstream_connections();

    // Collect node IDs (scheduling order is handled by the scheduler thread)
    topology.collect_node_ids();

    println!("Network topology built with {} nodes", topology.nodes.len());
    println!(
        "Found {} outlet nodes",
        topology
            .nodes
            .values()
            .filter(|n| n.downstream_id.is_none())
            .count()
    );

    Ok(topology)
}

// Fetch all channel parameters and filter in memory
pub fn load_channel_parameters(
    conn: &Connection,
    topology: &NetworkTopology,
    config: &ColumnConfig,
    _config_args: &cli::CfgContext,
) -> Result<HashMap<u32, ChannelParams>> {
    if topology.routing_order.is_empty() {
        return Ok(HashMap::new());
    }

    println!(
        "Loading channel parameters for {} nodes...",
        topology.routing_order.len()
    );

    // Create a HashSet for O(1) lookups
    let needed_ids: HashSet<u32> = topology.routing_order.iter().cloned().collect();

    // Query all rows without WHERE clause
    let query = format!(
        "SELECT {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8} \
         FROM 'flowpath-attributes'",
        config.key,
        config.dx,
        config.n,
        config.ncc,
        config.s0,
        config.bw,
        config.tw,
        config.twcc,
        config.cs
    );

    let mut stmt = conn
        .prepare(&query)
        .context("Failed to prepare channel params query")?;

    // Execute query and filter results in memory
    let channel_params_map: HashMap<u32, ChannelParams> = stmt
        .query_map([], |row| {
            let wb_id: String = row.get(0)?;
            let id = wb_id
                .strip_prefix("wb-")
                .and_then(|s| s.parse::<u32>().ok())
                .ok_or(rusqlite::Error::InvalidQuery)?;

            Ok((
                id,
                ChannelParams {
                    dx: row.get(1)?,
                    n: row.get(2)?,
                    ncc: row.get(3)?,
                    s0: row.get(4)?,
                    bw: row.get(5)?,
                    tw: row.get(6)?,
                    twcc: row.get(7)?,
                    cs: row.get(8)?,
                },
            ))
        })?
        .filter_map(|result| {
            result.ok().and_then(|(id, params)| {
                // Only keep parameters for nodes we need
                if needed_ids.contains(&id) {
                    Some((id, params))
                } else {
                    None
                }
            })
        })
        .collect();

    // Report results
    let loaded = channel_params_map.len();
    let total = topology.routing_order.len();
    println!(
        "Successfully loaded parameters for {}/{} nodes",
        loaded, total
    );

    if loaded < total {
        let missing: Vec<_> = topology
            .routing_order
            .iter()
            .filter(|id| !channel_params_map.contains_key(id))
            .collect();
        println!(
            "Warning: Missing parameters for {} nodes: {:?}",
            missing.len(),
            missing
        );
    }

    Ok(channel_params_map)
}
