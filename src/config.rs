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
    pub waterbody: String,
    pub gages: String,
    pub tw: String,
    pub twcc: String,
    pub musk: String,
    pub musx: String,
    pub cs: String,
    pub alt: String,
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
    pub dx: f64,
    pub n: f64,
    pub ncc: f64,
    pub s0: f64,
    pub bw: f64,
    pub tw: f64,
    pub twcc: f64,
    pub cs: f64,
}
