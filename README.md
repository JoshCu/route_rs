# Experimental MC routing in rust
immitating t-route to help teach myself rust
the code in here isn't very correct or good

# Network Routing

A Rust implementation of network flow routing using the Muskingum-Cunge method.

## Project Structure

```
src/
├── main.rs         # Main entry point
├── config.rs       # Configuration structures
├── network.rs      # Network topology and database operations
├── state.rs        # Network state management
├── routing.rs      # Core routing logic
├── mc_kernel.rs    # Muskingum-Cunge kernel (existing)
└── io/             # I/O operations
    ├── mod.rs      # Module declarations
    ├── csv.rs      # CSV reading/writing
    ├── netcdf.rs   # NetCDF output
    └── results.rs  # Simulation results storage
```

## Module Descriptions

### `main.rs`
- Program entry point
- Command-line argument parsing (future)
- High-level orchestration of the simulation

### `config.rs`
- `ColumnConfig`: Database column name mapping
- `OutputFormat`: Output format selection (CSV/NetCDF/Both)
- `ChannelParams`: Channel hydraulic parameters

### `network.rs`
- `NetworkNode`: Individual network node representation
- `NetworkTopology`: Complete network structure with topological ordering
- Database operations for loading network structure and channel parameters

### `state.rs`
- `RoutingState`: Per-channel routing state (previous timestep values)
- `NetworkState`: Complete network state including flows and external inputs

### `routing.rs`
- Core routing logic
- `process_timestep`: Processes one simulation timestep for all nodes

### `io/` module
- `csv.rs`: CSV file operations (reading external flows, writing results)
- `netcdf.rs`: NetCDF file writing
- `results.rs`: In-memory storage for simulation results

## Building and Running

```bash
# Build in release mode for optimal performance
cargo build --release

# Run the simulation
cargo run --release
```

## Configuration

Currently hardcoded in `main.rs`:
- Database path: `tests/gage-10154200/config/gage-10154200_subset.gpkg`
- CSV directory: `tests/gage-10154200/outputs/ngen/`
- Output format: `OutputFormat::Both`
- Internal timestep: 300 seconds

## Output Files

- CSV: `network_routing_results.csv`
- NetCDF: `troute_output_YYYYMMDDHHMMSS.nc`

## Performance Optimizations

- Parallel loading of external flow CSV files
- Serial routing computation (due to dependencies)
- Efficient topological sorting for correct processing order

## Future Improvements

- Command-line argument parsing
- Configuration file support
- Additional output formats
- Performance profiling and optimization
- Unit tests for each module
