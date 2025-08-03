# ERCOT Data Reorganization Plan

## Current Structure
```
rt_rust_processor/
├── annual_output/
│   ├── DAM_Hourly_LMPs_BusLevel/
│   │   └── DAM_Hourly_LMPs_BusLevel_YYYY.parquet (2010-2014)
│   ├── LMPs_by_Resource_Nodes__Load_Zones_and_Trading_Hubs/
│   │   └── LMPs_*.parquet (2010-2025)
│   └── Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones/
│       └── Settlement_*.parquet (2011-2025)
└── bess_analysis/
    └── various BESS analysis outputs
```

## New Structure at ~/data/ERCOT_data/processed/
```
~/data/ERCOT_data/processed/
├── dam/                          # Day-Ahead Market
│   ├── annual/                   # Annual files
│   │   ├── 2010/
│   │   │   └── dam_lmp_bus_2010.parquet
│   │   ├── 2011/
│   │   │   └── dam_spp_2011.parquet
│   │   └── ...
│   └── daily/                    # Daily files (future use)
│       └── 2024/
│           └── 01/
│               └── dam_spp_20240101.parquet
│
├── rtm/                          # Real-Time Market
│   ├── annual/
│   │   ├── 2010/
│   │   │   └── rtm_lmp_2010.parquet
│   │   └── ...
│   └── daily/                    # Daily files (future use)
│
├── spp/                          # Settlement Point Prices
│   ├── annual/
│   │   ├── 2011/
│   │   │   └── spp_all_2011.parquet
│   │   └── ...
│   └── daily/                    # Daily files (future use)
│
├── bess/                         # BESS Analysis Results
│   ├── revenues/
│   │   ├── bess_annual_revenues.parquet
│   │   ├── bess_daily_revenues.parquet
│   │   └── bess_revenues_corrected.parquet
│   └── resources/
│       └── bess_resources_master_list.parquet
│
└── metadata/                     # Metadata and mappings
    ├── settlement_points/
    └── data_dictionary/
```

## File Naming Conventions

### Annual Files
- DAM: `dam_[type]_[year].parquet`
  - `dam_lmp_bus_2010.parquet` (bus-level LMPs)
  - `dam_spp_2011.parquet` (settlement point prices)

- RTM: `rtm_lmp_[year].parquet`
  - `rtm_lmp_2010.parquet` (all LMPs)

- SPP: `spp_all_[year].parquet`
  - `spp_all_2011.parquet` (all settlement point prices)

### Daily Files (for future use with TBX)
- DAM: `dam_spp_YYYYMMDD.parquet`
- RTM: `rtm_lmp_YYYYMMDD.parquet`

## Mapping Table

| Current Location | New Location | New Filename |
|-----------------|--------------|--------------|
| DAM_Hourly_LMPs_BusLevel_2010.parquet | ~/data/ERCOT_data/processed/dam/annual/2010/ | dam_lmp_bus_2010.parquet |
| LMPs_by_Resource_Nodes_*_2010.parquet | ~/data/ERCOT_data/processed/rtm/annual/2010/ | rtm_lmp_2010.parquet |
| Settlement_Point_Prices_*_2011.parquet | ~/data/ERCOT_data/processed/spp/annual/2011/ | spp_all_2011.parquet |

## Benefits of New Structure

1. **Clear Market Separation**: DAM, RTM, and SPP in separate directories
2. **Time-based Organization**: Annual vs daily files clearly separated
3. **TBX-Ready**: Daily file structure ready for TBX calculator
4. **Consistent Naming**: Short, descriptive filenames
5. **Scalable**: Easy to add new data types or time granularities
6. **Version Control Friendly**: Organized by year/month for easy tracking