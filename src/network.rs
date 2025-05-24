use crate::config::{ChannelParams, ColumnConfig, OutputFormat};
use crate::state::NetworkState;
use rusqlite::{Connection, Result as SqliteResult};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::io::{Write, stdout};

// Network node representing a catchment/nexus
#[derive(Debug, Clone)]
pub struct NetworkNode {
    pub id: String,
    pub downstream_id: Option<String>,
    pub upstream_ids: Vec<String>,
    pub node_type: NodeType,
    pub area_sqkm: Option<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    Flowpath,
    Nexus,
}

// Network topology
#[derive(Debug)]
pub struct NetworkTopology {
    pub nodes: HashMap<String, NetworkNode>,
    pub routing_order: Vec<String>,
}

impl NetworkTopology {
    pub fn new() -> Self {
        NetworkTopology {
            nodes: HashMap::new(),
            routing_order: Vec::new(),
        }
    }

    pub fn add_node(&mut self, id: String, downstream_id: Option<String>, area_sqkm: Option<f32>) {
        let node = NetworkNode {
            id: id.clone(),
            downstream_id,
            upstream_ids: Vec::new(),
            node_type: if id.starts_with("wb") {
                NodeType::Flowpath
            } else {
                NodeType::Nexus
            },
            area_sqkm,
        };
        self.nodes.insert(id, node);
    }

    pub fn build_upstream_connections(&mut self) {
        let mut upstream_map: HashMap<String, Vec<String>> = HashMap::new();

        for (id, node) in &self.nodes {
            if let Some(downstream) = &node.downstream_id {
                upstream_map
                    .entry(downstream.clone())
                    .or_insert_with(Vec::new)
                    .push(id.clone());
            }
        }

        for (id, upstreams) in upstream_map {
            if let Some(node) = self.nodes.get_mut(&id) {
                node.upstream_ids = upstreams;
            }
        }
    }

    pub fn topological_sort(&mut self) -> Result<(), Box<dyn Error>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        // Calculate in-degrees
        for id in self.nodes.keys() {
            in_degree.insert(id.clone(), 0);
        }

        for (_, node) in &self.nodes {
            if let Some(downstream) = &node.downstream_id {
                if let Some(degree) = in_degree.get_mut(downstream) {
                    *degree += 1;
                }
            }
        }

        // Find nodes with no incoming edges (headwaters)
        for (id, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(id.clone());
            }
        }

        if queue.is_empty() {
            return Err("No headwater nodes found - possible cycle in network".into());
        }

        self.routing_order.clear();

        while let Some(current) = queue.pop_front() {
            self.routing_order.push(current.clone());

            if let Some(node) = self.nodes.get(&current) {
                if let Some(downstream) = &node.downstream_id {
                    if let Some(degree) = in_degree.get_mut(downstream) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(downstream.clone());
                        }
                    }
                }
            }
        }

        if self.routing_order.len() != self.nodes.len() {
            return Err(format!(
                "Cycle detected in network topology: processed {} nodes out of {}",
                self.routing_order.len(),
                self.nodes.len()
            )
            .into());
        }

        Ok(())
    }
}

pub fn get_area_sqkm(node_id: &str, conn: &Connection) -> Result<Option<f32>, Box<dyn Error>> {
    let area_query = "SELECT areasqkm FROM 'divides' WHERE id = ?";
    let mut stmt = conn.prepare(&area_query)?;
    let row = stmt.query_row([node_id], |row| Ok(row.get::<_, Option<f32>>(0)?))?;
    Ok(row)
}

// Function to build network topology from database
pub fn build_network_topology(
    conn: &Connection,
    config: &ColumnConfig,
) -> Result<NetworkTopology, Box<dyn Error>> {
    let mut topology = NetworkTopology::new();

    // Query nexus network table to get id-toid relationships
    let network_query = format!(
        "SELECT {}, {} FROM 'network' WHERE {} IS NOT NULL GROUP BY {}",
        config.key, config.downstream, config.downstream, config.key
    );

    let mut stmt = conn.prepare(&network_query)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,         // wb or nexus id
            row.get::<_, Option<String>>(1)?, // downstream wb or nexus id
        ))
    })?;

    // Collect nexus relationships and all nexus IDs
    let mut relationships = Vec::new();
    let mut all_ids = HashSet::new();

    for row in rows {
        let (id, downstream_id) = row?;
        all_ids.insert(id.clone());
        if let Some(ref ds) = downstream_id {
            all_ids.insert(ds.clone());
        }
        relationships.push((id, downstream_id));
    }

    // Build topology
    for (id, downstream_id) in relationships {
        // Check if downstream nexus exists in the network
        let validated_downstream = if let Some(ref ds) = downstream_id {
            if all_ids.contains(ds) {
                downstream_id
            } else {
                println!("id {} flows to {} which is outside the domain", id, ds);
                None
            }
        } else {
            None
        };
        let area = get_area_sqkm(&id, &conn).unwrap_or(None);
        topology.add_node(id, validated_downstream, area);
    }

    // Build upstream connections
    topology.build_upstream_connections();

    // Perform topological sort to get routing order
    topology.topological_sort()?;

    println!("Network topology built with {} nodes", topology.nodes.len());
    println!(
        "Found {} outlet nodes",
        topology
            .nodes
            .values()
            .filter(|n| n.downstream_id.is_none())
            .count()
    );
    println!("Routing order: {:?}", topology.routing_order);

    Ok(topology)
}

// Function to fetch channel parameters from SQLite
pub fn get_channel_params(
    conn: &Connection,
    channel_id: &str,
    config: &ColumnConfig,
) -> SqliteResult<ChannelParams> {
    let query = format!(
        "SELECT {}, {}, {}, {}, {}, {}, {}, {} FROM 'flowpath-attributes' WHERE {} = ?",
        config.dx,
        config.n,
        config.ncc,
        config.s0,
        config.bw,
        config.tw,
        config.twcc,
        config.cs,
        config.key
    );

    let mut stmt = conn.prepare(&query)?;
    let params = rusqlite::params![channel_id];

    let channel_params = stmt.query_row(params, |row| {
        Ok(ChannelParams {
            dx: row.get(0)?,
            n: row.get(1)?,
            ncc: row.get(2)?,
            s0: row.get(3)?,
            bw: row.get(4)?,
            tw: row.get(5)?,
            twcc: row.get(6)?,
            cs: row.get(7)?,
        })
    })?;

    Ok(channel_params)
}

// Load channel parameters and prepare features for NetCDF
pub fn load_channel_parameters(
    conn: &Connection,
    topology: &NetworkTopology,
    config: &ColumnConfig,
    network_state: &mut NetworkState,
    output_format: &OutputFormat,
) -> Result<
    (
        HashMap<String, ChannelParams>,
        HashMap<String, usize>,
        Vec<(i64, String)>,
    ),
    Box<dyn Error>,
> {
    let mut channel_params_map = HashMap::new();
    let mut feature_map = HashMap::new();
    let mut features = Vec::new();
    let mut lock = stdout().lock();

    for id in &topology.routing_order {
        network_state.initialize_node(id);
        if let Some(node) = topology.nodes.get(id) {
            if node.node_type == NodeType::Nexus {
                continue;
            }
            match get_channel_params(conn, &node.id, config) {
                Ok(params) => {
                    writeln!(lock, "Loaded channel parameters for {}: {:?}", id, params)?;
                    channel_params_map.insert(id.clone(), params);

                    // Add to feature list for NetCDF
                    if matches!(output_format, OutputFormat::NetCdf | OutputFormat::Both) {
                        let feature_id = id.split('-').last().unwrap().parse::<i64>().unwrap();
                        feature_map.insert(id.clone(), features.len());
                        features.push((feature_id, "ch".to_string()));
                    }
                }
                Err(e) => {
                    writeln!(lock, "Failed to load channel parameters for {}: {}", id, e)?;
                    continue;
                }
            }
        }
    }

    Ok((channel_params_map, feature_map, features))
}
