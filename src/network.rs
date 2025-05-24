use crate::config::{ChannelParams, ColumnConfig, OutputFormat};
use crate::state::NetworkState;
use rusqlite::{Connection, Result as SqliteResult};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;

// Network node representing a catchment/nexus
#[derive(Debug, Clone)]
pub struct NetworkNode {
    pub nexus_id: String,
    pub channel_id: Option<String>,
    pub downstream_nexus: Option<String>,
    pub upstream_nexuses: Vec<String>,
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

    pub fn add_node(
        &mut self,
        nexus_id: String,
        channel_id: Option<String>,
        downstream_nexus: Option<String>,
    ) {
        let node = NetworkNode {
            nexus_id: nexus_id.clone(),
            channel_id,
            downstream_nexus,
            upstream_nexuses: Vec::new(),
        };
        self.nodes.insert(nexus_id, node);
    }

    pub fn build_upstream_connections(&mut self) {
        let mut upstream_map: HashMap<String, Vec<String>> = HashMap::new();

        for (nexus_id, node) in &self.nodes {
            if let Some(downstream) = &node.downstream_nexus {
                upstream_map
                    .entry(downstream.clone())
                    .or_insert_with(Vec::new)
                    .push(nexus_id.clone());
            }
        }

        for (nexus_id, upstreams) in upstream_map {
            if let Some(node) = self.nodes.get_mut(&nexus_id) {
                node.upstream_nexuses = upstreams;
            }
        }
    }

    pub fn topological_sort(&mut self) -> Result<(), Box<dyn Error>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        // Calculate in-degrees
        for nexus_id in self.nodes.keys() {
            in_degree.insert(nexus_id.clone(), 0);
        }

        for (_, node) in &self.nodes {
            if let Some(downstream) = &node.downstream_nexus {
                if let Some(degree) = in_degree.get_mut(downstream) {
                    *degree += 1;
                }
            }
        }

        // Find nodes with no incoming edges (headwaters)
        for (nexus_id, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(nexus_id.clone());
            }
        }

        if queue.is_empty() {
            return Err("No headwater nodes found - possible cycle in network".into());
        }

        self.routing_order.clear();

        while let Some(current) = queue.pop_front() {
            self.routing_order.push(current.clone());

            if let Some(node) = self.nodes.get(&current) {
                if let Some(downstream) = &node.downstream_nexus {
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
            row.get::<_, Option<String>>(1)?, // downstream nexus id
        ))
    })?;

    // Collect nexus relationships and all nexus IDs
    let mut nexus_relationships = Vec::new();
    let mut all_nexus_ids = HashSet::new();

    for row in rows {
        let (nexus_id, downstream_nexus) = row?;
        all_nexus_ids.insert(nexus_id.clone());
        if let Some(ref ds) = downstream_nexus {
            all_nexus_ids.insert(ds.clone());
        }
        nexus_relationships.push((nexus_id, downstream_nexus));
    }

    // Query flowpaths table to find channel IDs for each nexus
    let flowpath_query = format!(
        "SELECT {} FROM 'flowpath-attributes' WHERE {} = ?",
        config.key, config.downstream
    );

    let mut flowpath_stmt = conn.prepare(&flowpath_query)?;

    // Build topology
    for (nexus_id, downstream_nexus) in nexus_relationships {
        // Find corresponding channel ID by looking for flowpath where toid = nexus_id
        let channel_id = match flowpath_stmt.query_row(rusqlite::params![&nexus_id], |row| {
            Ok(row.get::<_, String>(0)?)
        }) {
            Ok(id) => Some(id),
            Err(_) => None, // No channel found for this nexus (might be a junction)
        };

        // Check if downstream nexus exists in the network
        let validated_downstream = if let Some(ref ds) = downstream_nexus {
            if all_nexus_ids.contains(ds) {
                downstream_nexus
            } else {
                println!(
                    "Nexus {} flows to {} which is outside the domain",
                    nexus_id, ds
                );
                None
            }
        } else {
            None
        };

        topology.add_node(nexus_id, channel_id, validated_downstream);
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
            .filter(|n| n.downstream_nexus.is_none())
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

    for nexus_id in &topology.routing_order {
        network_state.initialize_node(nexus_id);

        if let Some(node) = topology.nodes.get(nexus_id) {
            if let Some(channel_id) = &node.channel_id {
                // Load channel parameters
                match get_channel_params(conn, channel_id, config) {
                    Ok(params) => {
                        println!("Loaded channel parameters for {}: {:?}", channel_id, params);
                        channel_params_map.insert(nexus_id.clone(), params);

                        // Add to feature list for NetCDF
                        if matches!(output_format, OutputFormat::NetCdf | OutputFormat::Both) {
                            let feature_id = channel_id.parse::<i64>().unwrap_or(-1);
                            feature_map.insert(nexus_id.clone(), features.len());
                            features.push((feature_id, "ch".to_string()));
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to load channel parameters for {}: {}",
                            channel_id, e
                        );
                        continue;
                    }
                }
            }
        }
    }

    Ok((channel_params_map, feature_map, features))
}
