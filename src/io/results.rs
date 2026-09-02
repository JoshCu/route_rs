// Structure to store results for NetCDF output
#[derive(Debug)]
pub struct SimulationResults {
    pub feature_id: u32,
    pub flow_data: Vec<f32>,
    pub velocity_data: Vec<f32>,
    pub depth_data: Vec<f32>,
}

impl SimulationResults {
    pub fn new(feature_id: u32) -> Self {
        SimulationResults {
            feature_id,
            flow_data: Vec::new(),
            velocity_data: Vec::new(),
            depth_data: Vec::new(),
        }
    }
}
