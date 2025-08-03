# Running TBX Calculator with Reorganized Data

## Data Location
All processed ERCOT data is now organized at:
```
~/data/ERCOT_data/processed/
```

## Directory Structure
```
processed/
├── dam/annual/      # Day-Ahead Market prices by year
├── rtm/annual/      # Real-Time Market LMPs by year  
├── spp/annual/      # Settlement Point Prices (combined DAM/RT)
└── bess/            # BESS analysis results
```

## Important Notes About the Data

### Settlement Point Prices (SPP) Files
The SPP files (`spp_all_YYYY.parquet`) contain BOTH day-ahead and real-time prices in a single file. These are the primary files you'll use for TBX calculations from 2011 onwards.

### Data Availability by Year
- **2010-2014**: Separate DAM bus-level LMPs and RTM LMPs
- **2011-2025**: Combined Settlement Point Prices (SPP) with both DAM and RT

## Running TBX Calculator

### Example 1: TB2 Analysis for Single BESS (2024)
```bash
cd /Users/enrico/proj/power_market_pipeline/rt_rust_processor/tbx_calculator

cargo run --release -- \
  --variant TB2 \
  --power-mw 100 \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --mapping-file /Users/enrico/proj/solar_sim/ercot_network/settlement_point_electrical_bus_mapping/Resource_Node_to_Unit_03212025_130141.csv \
  --da-path-pattern "/Users/enrico/data/ERCOT_data/processed/spp/annual/{year}/spp_all_{year}.parquet" \
  --rt-path-pattern "/Users/enrico/data/ERCOT_data/processed/rtm/annual/{year}/rtm_lmp_{year}.parquet" \
  --resource "ARAGORN_UNIT1" \
  --output summary
```

### Example 2: All BESS with Blended Optimization
```bash
cargo run --release -- \
  --variant TB2 \
  --resource ALL \
  --start-date 2023-01-01 \
  --end-date 2023-12-31 \
  --mapping-file /Users/enrico/proj/solar_sim/ercot_network/settlement_point_electrical_bus_mapping/Resource_Node_to_Unit_03212025_130141.csv \
  --da-path-pattern "/Users/enrico/data/ERCOT_data/processed/spp/annual/{year}/spp_all_{year}.parquet" \
  --rt-path-pattern "/Users/enrico/data/ERCOT_data/processed/rtm/annual/{year}/rtm_lmp_{year}.parquet" \
  --blended \
  --output csv > tbx_results_2023.csv
```

### Example 3: Quick Test with One Month
```bash
cargo run --release -- \
  --variant TB1 \
  --power-mw 50 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --mapping-file /Users/enrico/proj/solar_sim/ercot_network/settlement_point_electrical_bus_mapping/Resource_Node_to_Unit_03212025_130141.csv \
  --da-path-pattern "/Users/enrico/data/ERCOT_data/processed/spp/annual/{year}/spp_all_{year}.parquet" \
  --rt-path-pattern "/Users/enrico/data/ERCOT_data/processed/rtm/annual/{year}/rtm_lmp_{year}.parquet" \
  --resource "EXAMPLE_BESS1" \
  --output json
```

## Path Pattern Notes

The TBX calculator uses `{year}` placeholder in path patterns:
- Pattern: `/path/to/spp/annual/{year}/spp_all_{year}.parquet`
- For 2024: `/path/to/spp/annual/2024/spp_all_2024.parquet`

Currently, the data is organized by year. If you need daily files for more granular analysis, you would need to:
1. Split the annual files into daily files
2. Update the path pattern to use `{date}` instead of `{year}`

## Troubleshooting

### If Column Names Don't Match
The TBX calculator expects specific column names. If you get errors, you may need to update `data_loader.rs`:

For SPP files:
- Expected: `DeliveryDate`, `DeliveryHour`, `SettlementPoint`, `SettlementPointPrice`
- May need to check actual column names in your files

For RTM files:
- Expected: `SCEDTimestamp`, `SettlementPointName`, `LMP`
- May need adjustment based on actual schema

### Performance Tips
1. Use `--use-arrow` flag to test Arrow performance vs Polars
2. Start with a small date range to test
3. Use `--output summary` for quick overview
4. Use `--blended` only when you need DA+RT optimization

## Next Steps

1. **Verify Schema**: Run the check_schema.py script to verify column names
2. **Test Small**: Start with a single day or week
3. **Scale Up**: Once working, run full year analysis
4. **Compare**: Run with and without `--blended` to see revenue differences