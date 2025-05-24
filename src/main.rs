use chrono::Duration;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use rusqlite::{Connection, Result as SqliteResult};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

mod mc_kernel;

// Configuration structure for column name mapping
#[derive(Debug, Clone)]
struct ColumnConfig {
    key: String,
    downstream: String,
    dx: String,
    n: String,
    ncc: String,
    s0: String,
    bw: String,
    waterbody: String,
    gages: String,
    tw: String,
    twcc: String,
    musk: String,
    musx: String,
    cs: String,
    alt: String,
}

impl ColumnConfig {
    fn new() -> Self {
        ColumnConfig {
            key: "id".to_string(),
            downstream: "toid".to_string(),
            dx: "Length_m".to_string(),
            n: "n".to_string(),
            ncc: "nCC".to_string(),
            s0: "So".to_string(),
            bw: "BtmWdth".to_string(),
            waterbody: "WaterbodyID".to_string(),
            gages: "gage".to_string(),
            tw: "TopWdth".to_string(),
            twcc: "TopWdthCC".to_string(),
            musk: "MusK".to_string(),
            musx: "MusX".to_string(),
            cs: "ChSlp".to_string(),
            alt: "alt".to_string(),
        }
    }
}

// Network node representing a catchment/nexus
#[derive(Debug, Clone)]
struct NetworkNode {
    nexus_id: String,
    channel_id: Option<String>,
    downstream_nexus: Option<String>,
    upstream_nexuses: Vec<String>,
}

// Network topology
#[derive(Debug)]
struct NetworkTopology {
    nodes: HashMap<String, NetworkNode>,
    routing_order: Vec<String>,
}

impl NetworkTopology {
    fn new() -> Self {
        NetworkTopology {
            nodes: HashMap::new(),
            routing_order: Vec::new(),
        }
    }

    fn add_node(
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

    fn build_upstream_connections(&mut self) {
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

    fn topological_sort(&mut self) -> Result<(), Box<dyn Error>> {
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

// Flow data from CSV
#[derive(Debug)]
struct FlowData {
    index: usize,
    timestamp: String,
    ql: f64,
}

impl FlowData {
    fn from_record(record: &StringRecord) -> Result<Self, Box<dyn Error>> {
        if record.len() < 3 {
            return Err("Record has fewer than 3 fields".into());
        }

        let index = record[0].trim().parse::<usize>()?;
        let timestamp = record[1].trim().to_string();
        let ql = record[2].trim().parse::<f64>()?;

        Ok(FlowData {
            index,
            timestamp,
            ql,
        })
    }
}

// Channel parameters from SQLite
#[derive(Debug, Clone)]
struct ChannelParams {
    dx: f64,
    n: f64,
    ncc: f64,
    s0: f64,
    bw: f64,
    tw: f64,
    twcc: f64,
    cs: f64,
}

// State to track previous time step values for each channel
#[derive(Debug)]
struct RoutingState {
    qup: f64,
    quc: f64,
    qdp: f64,
    depth_p: f64,
}

impl RoutingState {
    fn new() -> Self {
        RoutingState {
            qup: 0.0,
            quc: 0.0,
            qdp: 0.0,
            depth_p: 0.0,
        }
    }

    fn update(&mut self, quc: f64, qdc: f64, depthc: f64) {
        self.qup = self.quc;
        self.quc = quc;
        self.qdp = qdc;
        self.depth_p = depthc;
    }
}

// Network state manager
#[derive(Debug)]
struct NetworkState {
    states: HashMap<String, RoutingState>,
    current_flows: HashMap<String, f64>,
    external_flows: HashMap<String, HashMap<usize, f64>>,
}

impl NetworkState {
    fn new() -> Self {
        NetworkState {
            states: HashMap::new(),
            current_flows: HashMap::new(),
            external_flows: HashMap::new(),
        }
    }

    fn initialize_node(&mut self, nexus_id: &str) {
        self.states
            .insert(nexus_id.to_string(), RoutingState::new());
        self.current_flows.insert(nexus_id.to_string(), 0.0);
    }

    fn get_upstream_flow(&self, _nexus_id: &str, upstream_nexuses: &[String]) -> f64 {
        upstream_nexuses
            .iter()
            .map(|upstream| self.current_flows.get(upstream).unwrap_or(&0.0))
            .sum()
    }

    fn update_flow(&mut self, nexus_id: &str, flow: f64) {
        self.current_flows.insert(nexus_id.to_string(), flow);
    }
}

// Function to build network topology from database
fn build_network_topology(
    conn: &Connection,
    config: &ColumnConfig,
) -> Result<NetworkTopology, Box<dyn Error>> {
    let mut topology = NetworkTopology::new();

    // Query nexus network table to get id-toid relationships
    let network_query = format!(
        "SELECT {}, {} FROM 'network' WHERE {} IS NOT NULL",
        config.key, config.downstream, config.downstream
    );

    let mut stmt = conn.prepare(&network_query)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,         // nexus id
            row.get::<_, Option<String>>(1)?, // downstream nexus id
        ))
    })?;

    // Collect nexus relationships and all nexus IDs
    let mut nexus_relationships = Vec::new();
    let mut all_nexus_ids = std::collections::HashSet::new();

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
        // If not, it's flowing out of the domain, so set downstream to None
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
fn get_channel_params(
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

// Function to load external flows for a specific nexus/catchment
fn load_external_flows(
    csv_file: &str,
    nexus_id: &str,
) -> Result<HashMap<usize, f64>, Box<dyn Error>> {
    let mut external_flows = HashMap::new();

    // Check if file exists, if not return empty flows
    if !Path::new(csv_file).exists() {
        println!("No external flow file found for {}: {}", nexus_id, csv_file);
        return Ok(external_flows);
    }

    let file = File::open(csv_file)?;
    let buffered_reader = BufReader::new(file);

    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(buffered_reader);

    for result in rdr.records() {
        let record = result?;
        let flow_data = FlowData::from_record(&record)?;
        external_flows.insert(flow_data.index, flow_data.ql);
    }

    println!(
        "Loaded {} external flow records for {}",
        external_flows.len(),
        nexus_id
    );
    Ok(external_flows)
}

fn main() -> Result<(), Box<dyn Error>> {
    // Configuration
    let db_path = "tests/gage-10154200/config/gage-10154200_subset.gpkg";
    let internal_timestep_seconds = 300.0;
    let dt = internal_timestep_seconds;

    // Directory containing CSV files (one per catchment)
    let csv_dir = "tests/gage-10154200/outputs/ngen/";

    // Initialize SQLite connection
    let conn = Connection::open(db_path)?;

    // Initialize column configuration
    let column_config = ColumnConfig::new();

    // Build network topology
    let topology = build_network_topology(&conn, &column_config)?;

    // Initialize network state
    let mut network_state = NetworkState::new();

    // Load channel parameters and external flows for each node
    let mut channel_params_map: HashMap<String, ChannelParams> = HashMap::new();

    for nexus_id in &topology.routing_order {
        network_state.initialize_node(nexus_id);

        if let Some(node) = topology.nodes.get(nexus_id) {
            if let Some(channel_id) = &node.channel_id {
                // Load channel parameters
                match get_channel_params(&conn, channel_id, &column_config) {
                    Ok(params) => {
                        println!("Loaded channel parameters for {}: {:?}", channel_id, params);
                        channel_params_map.insert(nexus_id.clone(), params);
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to load channel parameters for {}: {}",
                            channel_id, e
                        );
                        continue;
                    }
                }

                // Load external flows
                let csv_file = format!("{}{}_output.csv", csv_dir, nexus_id);
                match load_external_flows(&csv_file, nexus_id) {
                    Ok(flows) => {
                        network_state.external_flows.insert(nexus_id.clone(), flows);
                    }
                    Err(e) => {
                        eprintln!("Failed to load external flows for {}: {}", nexus_id, e);
                        network_state
                            .external_flows
                            .insert(nexus_id.clone(), HashMap::new());
                    }
                }
            }
        }
    }

    // Set up output CSV writer
    let output_path = Path::new("network_routing_results.csv");
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_path)?;

    // Write header
    wtr.write_record(&[
        "step",
        "nexus_id",
        "channel_id",
        "flow",
        "velocity",
        "depth",
    ])?;

    // Determine simulation period (assuming hourly external data)
    // Fix the borrow checker issue by completing the calculation before the loop
    let max_external_steps = network_state
        .external_flows
        .values()
        .flat_map(|flows| flows.keys())
        .max()
        .copied()
        .unwrap_or(0);

    let start_time = chrono::Utc::now().naive_utc();
    let end_time = start_time + Duration::seconds((3600 * max_external_steps) as i64);

    println!("Simulation period: {} to {}", start_time, end_time);
    println!(
        "Using internal timestep of {} seconds",
        internal_timestep_seconds
    );
    println!("Processing {} network nodes", topology.routing_order.len());

    // Main routing loop
    let mut current_time = start_time;
    let mut step_idx = 0;

    while step_idx <= max_external_steps * 12 {
        // 12 internal steps per hour
        let current_external_idx = step_idx / 12;

        // Process each node in topological order
        for nexus_id in &topology.routing_order {
            if let Some(node) = topology.nodes.get(nexus_id) {
                if let Some(channel_id) = &node.channel_id {
                    if let Some(channel_params) = channel_params_map.get(nexus_id) {
                        // Get external flow for this timestep
                        let external_flow = network_state
                            .external_flows
                            .get(nexus_id)
                            .and_then(|flows| flows.get(&current_external_idx))
                            .unwrap_or(&0.0);

                        // Get upstream flow (sum of all upstream nodes)
                        let upstream_flow =
                            network_state.get_upstream_flow(nexus_id, &node.upstream_nexuses);

                        // Get current state values (without holding mutable reference)
                        let (qup, qdp, depth_p) = {
                            let state = network_state.states.get(nexus_id).unwrap();
                            (state.qup, state.qdp, state.depth_p)
                        };

                        // Run Muskingcunge routing
                        let (qdc, velc, depthc) = mc_kernel::submuskingcunge(
                            qup,
                            upstream_flow, // Use upstream flow as current upstream input
                            qdp,
                            *external_flow,
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

                        // Update network state
                        network_state.update_flow(nexus_id, qdc);

                        // Update routing state
                        let state = network_state.states.get_mut(nexus_id).unwrap();
                        state.update(upstream_flow, qdc, depthc);

                        // Write results at output timesteps (every hour)
                        if step_idx % 12 == 0 {
                            wtr.write_record(&[
                                step_idx.to_string(),
                                nexus_id.clone(),
                                channel_id.clone(),
                                qdc.to_string(),
                                velc.to_string(),
                                depthc.to_string(),
                            ])?;
                        }
                    }
                } else {
                    // Junction node - just pass through upstream flows
                    let upstream_flow =
                        network_state.get_upstream_flow(nexus_id, &node.upstream_nexuses);
                    network_state.update_flow(nexus_id, upstream_flow);

                    // Write junction results
                    if step_idx % 12 == 0 {
                        wtr.write_record(&[
                            step_idx.to_string(),
                            nexus_id.clone(),
                            "junction".to_string(),
                            upstream_flow.to_string(),
                            "0.0".to_string(),
                            "0.0".to_string(),
                        ])?;
                    }
                }
            }
        }

        // Advance to next timestep
        current_time = current_time + Duration::seconds(internal_timestep_seconds as i64);
        step_idx += 1;
    }

    // Final flush
    wtr.flush()?;

    println!("Network routing complete. Results saved to network_routing_results.csv");
    Ok(())
}
