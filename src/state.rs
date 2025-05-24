use std::collections::HashMap;

// State to track previous time step values for each channel
#[derive(Debug)]
pub struct RoutingState {
    pub qup: f64,
    pub quc: f64,
    pub qdp: f64,
    pub depth_p: f64,
}

impl RoutingState {
    pub fn new() -> Self {
        RoutingState {
            qup: 0.0,
            quc: 0.0,
            qdp: 0.0,
            depth_p: 0.0,
        }
    }

    pub fn update(&mut self, quc: f64, qdc: f64, depthc: f64) {
        self.qup = self.quc;
        self.quc = quc;
        self.qdp = qdc;
        self.depth_p = depthc;
    }
}

// Network state manager
#[derive(Debug)]
pub struct NetworkState {
    pub states: HashMap<String, RoutingState>,
    pub current_flows: HashMap<String, f64>,
    pub external_flows: HashMap<String, HashMap<usize, f64>>,
}

impl NetworkState {
    pub fn new() -> Self {
        NetworkState {
            states: HashMap::new(),
            current_flows: HashMap::new(),
            external_flows: HashMap::new(),
        }
    }

    pub fn initialize_node(&mut self, nexus_id: &str) {
        self.states
            .insert(nexus_id.to_string(), RoutingState::new());
        self.current_flows.insert(nexus_id.to_string(), 0.0);
    }

    pub fn get_upstream_flow(&self, _nexus_id: &str, upstream_nexuses: &[String]) -> f64 {
        upstream_nexuses
            .iter()
            .map(|upstream| self.current_flows.get(upstream).unwrap_or(&0.0))
            .sum()
    }

    pub fn update_flow(&mut self, nexus_id: &str, flow: f64) {
        self.current_flows.insert(nexus_id.to_string(), flow);
    }
}
