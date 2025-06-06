// Configuration structure for column name mapping
#[derive(Debug, Clone)]
pub struct ColumnConfig {
    pub key: String,
    pub downstream: String,
    pub dx: String,
    pub n: String,
    pub ncc: String,
    pub s0: String,
    pub bw: String,
    pub tw: String,
    pub twcc: String,
    pub cs: String,
}

impl Default for ColumnConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnConfig {
    pub fn new() -> Self {
        ColumnConfig {
            key: "id".to_string(),
            downstream: "toid".to_string(),
            dx: "Length_m".to_string(),
            n: "n".to_string(),
            ncc: "nCC".to_string(),
            s0: "So".to_string(),
            bw: "BtmWdth".to_string(),
            tw: "TopWdth".to_string(),
            twcc: "TopWdthCC".to_string(),
            cs: "ChSlp".to_string(),
        }
    }
}

// Output format configuration
#[derive(Debug, Clone)]
pub enum OutputFormat {
    Csv,
    NetCdf,
    Both,
}

// Channel parameters from SQLite
#[derive(Debug, Clone)]
pub struct ChannelParams {
    pub dx: f32,
    pub n: f32,
    pub ncc: f32,
    pub s0: f32,
    pub bw: f32,
    pub tw: f32,
    pub twcc: f32,
    pub cs: f32,
}