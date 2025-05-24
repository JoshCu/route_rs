// Structure to store results for NetCDF output
#[derive(Debug)]
pub struct SimulationResults {
    pub feature_ids: Vec<i64>,
    pub feature_types: Vec<String>,
    pub times: Vec<f64>,
    pub flow_data: Vec<Vec<f32>>,
    pub velocity_data: Vec<Vec<f32>>,
    pub depth_data: Vec<Vec<f32>>,
}

impl SimulationResults {
    pub fn new() -> Self {
        SimulationResults {
            feature_ids: Vec::new(),
            feature_types: Vec::new(),
            times: Vec::new(),
            flow_data: Vec::new(),
            velocity_data: Vec::new(),
            depth_data: Vec::new(),
        }
    }

    pub fn initialize_features(&mut self, features: Vec<(i64, String)>) {
        for (id, ftype) in features {
            self.feature_ids.push(id);
            self.feature_types.push(ftype);
            self.flow_data.push(Vec::new());
            self.velocity_data.push(Vec::new());
            self.depth_data.push(Vec::new());
        }
    }

    pub fn add_timestep(&mut self, time: f64) {
        self.times.push(time);
    }

    pub fn add_result(&mut self, feature_idx: usize, flow: f32, velocity: f32, depth: f32) {
        self.flow_data[feature_idx].push(flow);
        self.velocity_data[feature_idx].push(velocity);
        self.depth_data[feature_idx].push(depth);
    }
}
