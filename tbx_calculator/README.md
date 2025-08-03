# TBX Calculator - Top-Bottom X Hours Energy Arbitrage

High-performance Rust implementation for calculating energy arbitrage opportunities using the Top-Bottom X hours (TBX) methodology.

## Overview

TBX analysis identifies the most expensive hours (for discharging) and cheapest hours (for charging) within each 24-hour period to maximize battery energy storage system (BESS) revenue through energy arbitrage.

## Features

- **Multiple TBX Variants**: TB1 (1-hour), TB2 (2-hour), and TB4 (4-hour) batteries
- **Multi-Market Support**: 
  - Day-Ahead (DA) only
  - Real-Time (RT) only  
  - Blended DA+RT optimization
- **High Performance**: 
  - Parallel processing with Rayon
  - Choice of Polars or Arrow for data loading
  - Optimized for large datasets
- **Settlement Point Mapping**: Automatic mapping of generators to their pricing nodes
- **Flexible Output**: JSON, CSV, or summary format

## Usage

### Basic TB2 Calculation
```bash
cargo run --release -- \
  --variant TB2 \
  --power-mw 100 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --mapping-file /path/to/Resource_Node_to_Unit.csv \
  --da-path-pattern "/data/DA_{date}.parquet" \
  --rt-path-pattern "/data/RT_{date}.parquet" \
  --resource "EXAMPLE_BESS1"
```

### All BESS with Blended Optimization
```bash
cargo run --release -- \
  --variant TB2 \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --mapping-file /path/to/mappings.csv \
  --da-path-pattern "/data/ercot/dam/{date}_dam.parquet" \
  --rt-path-pattern "/data/ercot/sced/{date}_sced.parquet" \
  --resource ALL \
  --blended \
  --output summary
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--variant` | TBX variant (TB1, TB2, TB4) | Required |
| `--power-mw` | Battery power in MW | 100.0 |
| `--efficiency` | Round-trip efficiency (0-1) | 0.85 |
| `--start-date` | Start date (YYYY-MM-DD) | Required |
| `--end-date` | End date (YYYY-MM-DD) | Required |
| `--mapping-file` | Path to settlement point mapping | Required |
| `--da-path-pattern` | DA price file pattern | Required |
| `--rt-path-pattern` | RT price file pattern | Required |
| `--resource` | Resource name or "ALL" | ALL |
| `--output` | Output format (json/csv/summary) | json |
| `--use-arrow` | Use Arrow instead of Polars | false |
| `--blended` | Calculate blended DA+RT | false |

## Algorithm Details

### Basic TBX (Single Market)
1. For each 24-hour period:
   - Sort all price intervals by price
   - Select bottom X hours for charging (lowest prices)
   - Select top X hours for discharging (highest prices)
   - Calculate revenue: `(avg_high - avg_low) * power * duration * efficiency`

### Blended DA+RT Optimization
1. Identify RT price spikes that exceed DA prices
2. Allocate battery capacity to capture short RT spikes
3. Use remaining capacity for DA arbitrage
4. Respect battery energy and power constraints

### Example TB2 Calculation
```
Battery: 100 MW / 200 MWh
Day: 2024-01-15

Charge Hours:
- 02:00-03:00: $18/MWh
- 03:00-04:00: $19/MWh
Average: $18.50/MWh

Discharge Hours:
- 19:00-20:00: $95/MWh
- 20:00-21:00: $88/MWh
Average: $91.50/MWh

Spread: $73/MWh
Revenue: $73 * 100 MW * 2 hours * 0.85 = $12,410/day
```

## Performance Comparison

### Polars vs Arrow
- **Polars**: Better for complex queries and transformations
- **Arrow**: Faster for simple columnar reads
- Benchmark your specific use case with `--use-arrow` flag

### Optimization Tips
1. Pre-sort your Parquet files by settlement point
2. Use date-partitioned files
3. Enable Parquet compression (Snappy or LZ4)
4. Run with `--release` build

## Data Requirements

### Price Data Schema
DA Prices (Parquet):
- `DeliveryDate`: timestamp
- `DeliveryHour`: i32
- `SettlementPoint`: string
- `SettlementPointPrice`: f64

RT Prices (Parquet):
- `SCEDTimestamp`: timestamp  
- `SettlementPointName`: string
- `LMP`: f64

### Settlement Mapping (CSV):
- `RESOURCE_NODE`: string
- `UNIT_NAME`: string
- `UNIT_SUBSTATION`: string

## Building

```bash
# Development build
cargo build

# Optimized release build
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Integration with Power Market Pipeline

This TBX calculator is designed to work with the extracted ERCOT data from the main pipeline:

```bash
# First extract data using main processor
cd ../
cargo run --release -- --extract-all-ercot /path/to/ERCOT_data

# Then run TBX analysis
cd tbx_calculator
cargo run --release -- \
  --variant TB2 \
  --da-path-pattern "/path/to/processed/dam/{date}.parquet" \
  --rt-path-pattern "/path/to/processed/sced/{date}.parquet" \
  ...
```

## Output Examples

### JSON Output
```json
{
  "resource_name": "EXAMPLE_BESS1",
  "settlement_point": "EXAMPLE_RN",
  "date": "2024-01-15",
  "revenue_da": 12410.50,
  "revenue_rt": 11850.25,
  "revenue_blended": 13275.00,
  "avg_spread_da": 73.00,
  "utilization_factor": 1.0,
  "best_strategy": "Blended"
}
```

### Summary Output
```
TBX Analysis Summary
===================
Period: 2024-01-01 to 2024-01-31
Configuration: 100 MW / 200 MWh battery
Efficiency: 85.0%

Total Revenue by Resource:
  EXAMPLE_BESS1: $385,055.00 total ($12,421.45/day)
  EXAMPLE_BESS2: $372,840.00 total ($12,027.10/day)
  ...
```

## Future Enhancements

- [ ] Multi-cycle per day optimization
- [ ] Degradation and cycling costs
- [ ] Ancillary service co-optimization
- [ ] Forward curve integration
- [ ] Stochastic price scenarios
- [ ] Web API endpoint