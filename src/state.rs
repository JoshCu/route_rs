#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NodeStatus {
    NotReady,
    Ready,
    // DoneRunning,
    // DoneWriting,
    // Completed,
}

// pub fn get_upstream_flow(&self, _id: &u32, upstream_nexuses: &[u32]) -> f32 {
//     upstream_nexuses
//         .iter()
//         .map(|upstream| self.current_flows.get(upstream).unwrap_or(&0.0))
//         .sum()
// }
