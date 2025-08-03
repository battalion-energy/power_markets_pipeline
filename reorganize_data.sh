#!/bin/bash
# Script to reorganize ERCOT processed data into a clean structure

# Base directories
SOURCE_DIR="/Users/enrico/proj/power_market_pipeline/rt_rust_processor"
TARGET_DIR="/Users/enrico/data/ERCOT_data/processed"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}ERCOT Data Reorganization Script${NC}"
echo "=================================="
echo "Source: $SOURCE_DIR"
echo "Target: $TARGET_DIR"
echo ""

# Create directory structure
echo -e "${YELLOW}Creating directory structure...${NC}"
mkdir -p "$TARGET_DIR"/{dam/annual,rtm/annual,spp/annual,bess/{revenues,resources},metadata/{settlement_points,data_dictionary}}

# Create year subdirectories
for year in {2010..2025}; do
    mkdir -p "$TARGET_DIR/dam/annual/$year"
    mkdir -p "$TARGET_DIR/rtm/annual/$year"
    mkdir -p "$TARGET_DIR/spp/annual/$year"
done

echo -e "${GREEN}✓ Directory structure created${NC}"

# Function to copy and rename files
copy_and_rename() {
    local src="$1"
    local dst="$2"
    if [ -f "$src" ]; then
        cp -v "$src" "$dst"
        echo -e "${GREEN}✓ Copied: $(basename "$dst")${NC}"
    else
        echo -e "${YELLOW}⚠ Source not found: $src${NC}"
    fi
}

# Move DAM files (Day-Ahead Market)
echo -e "\n${BLUE}Processing DAM files...${NC}"
for year in {2010..2014}; do
    src="$SOURCE_DIR/annual_output/DAM_Hourly_LMPs_BusLevel/DAM_Hourly_LMPs_BusLevel_${year}.parquet"
    dst="$TARGET_DIR/dam/annual/$year/dam_lmp_bus_${year}.parquet"
    copy_and_rename "$src" "$dst"
done

# For years 2015+ we'll use Settlement Point Prices for DAM
for year in {2015..2025}; do
    # Check if SPP file exists for DAM data
    src="$SOURCE_DIR/annual_output/Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones/Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_${year}.parquet"
    dst="$TARGET_DIR/dam/annual/$year/dam_spp_${year}.parquet"
    if [ -f "$src" ]; then
        # This file contains both DAM and RT data, but we'll copy it to DAM for now
        # In production, you'd want to filter it
        copy_and_rename "$src" "$dst"
    fi
done

# Move RTM files (Real-Time Market)
echo -e "\n${BLUE}Processing RTM files...${NC}"
for year in {2010..2025}; do
    src="$SOURCE_DIR/annual_output/LMPs_by_Resource_Nodes__Load_Zones_and_Trading_Hubs/LMPs_by_Resource_Nodes__Load_Zones_and_Trading_Hubs_${year}.parquet"
    dst="$TARGET_DIR/rtm/annual/$year/rtm_lmp_${year}.parquet"
    copy_and_rename "$src" "$dst"
done

# Move SPP files (Settlement Point Prices - combined DAM/RT)
echo -e "\n${BLUE}Processing SPP files...${NC}"
for year in {2011..2025}; do
    src="$SOURCE_DIR/annual_output/Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones/Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_${year}.parquet"
    dst="$TARGET_DIR/spp/annual/$year/spp_all_${year}.parquet"
    copy_and_rename "$src" "$dst"
done

# Move BESS analysis files
echo -e "\n${BLUE}Processing BESS analysis files...${NC}"
copy_and_rename "$SOURCE_DIR/bess_analysis/bess_daily_revenues.parquet" "$TARGET_DIR/bess/revenues/bess_daily_revenues.parquet"
copy_and_rename "$SOURCE_DIR/bess_analysis/bess_resources_master_list.parquet" "$TARGET_DIR/bess/resources/bess_resources_master_list.parquet"
copy_and_rename "$SOURCE_DIR/bess_complete_analysis/bess_annual_revenues_complete.parquet" "$TARGET_DIR/bess/revenues/bess_annual_revenues.parquet"
copy_and_rename "$SOURCE_DIR/bess_complete_analysis/bess_revenues_corrected.parquet" "$TARGET_DIR/bess/revenues/bess_revenues_corrected.parquet"

# Create a summary file
echo -e "\n${BLUE}Creating summary...${NC}"
SUMMARY_FILE="$TARGET_DIR/REORGANIZATION_SUMMARY.txt"
{
    echo "ERCOT Data Reorganization Summary"
    echo "================================="
    echo "Date: $(date)"
    echo ""
    echo "Directory Structure:"
    echo ""
    tree -d "$TARGET_DIR" 2>/dev/null || find "$TARGET_DIR" -type d | sort
    echo ""
    echo "File Count by Type:"
    echo "DAM files: $(find "$TARGET_DIR/dam" -name "*.parquet" 2>/dev/null | wc -l)"
    echo "RTM files: $(find "$TARGET_DIR/rtm" -name "*.parquet" 2>/dev/null | wc -l)"
    echo "SPP files: $(find "$TARGET_DIR/spp" -name "*.parquet" 2>/dev/null | wc -l)"
    echo "BESS files: $(find "$TARGET_DIR/bess" -name "*.parquet" 2>/dev/null | wc -l)"
    echo ""
    echo "Total size: $(du -sh "$TARGET_DIR" 2>/dev/null | cut -f1)"
} > "$SUMMARY_FILE"

echo -e "${GREEN}✓ Summary created: $SUMMARY_FILE${NC}"

# Create README for the processed data
README_FILE="$TARGET_DIR/README.md"
cat > "$README_FILE" << 'EOF'
# ERCOT Processed Data

This directory contains processed ERCOT market data organized by market type and time period.

## Directory Structure

```
processed/
├── dam/        # Day-Ahead Market data
├── rtm/        # Real-Time Market data  
├── spp/        # Settlement Point Prices (combined DAM/RT)
├── bess/       # Battery Energy Storage System analysis
└── metadata/   # Reference data and mappings
```

## File Naming Conventions

- **Annual files**: `[market]_[type]_[year].parquet`
  - Example: `dam_spp_2024.parquet`
- **Daily files**: `[market]_[type]_YYYYMMDD.parquet`
  - Example: `rtm_lmp_20240115.parquet`

## Data Types

### DAM (Day-Ahead Market)
- `dam_lmp_bus_YYYY.parquet`: Bus-level LMPs (2010-2014)
- `dam_spp_YYYY.parquet`: Settlement point prices (2015+)

### RTM (Real-Time Market)
- `rtm_lmp_YYYY.parquet`: Real-time LMPs at all settlement points

### SPP (Settlement Point Prices)
- `spp_all_YYYY.parquet`: Combined DAM and RT settlement point prices

### BESS
- Revenue analysis and resource lists for battery storage systems

## Usage with TBX Calculator

```bash
tbx_calculator \
  --da-path-pattern "~/data/ERCOT_data/processed/dam/annual/{year}/dam_spp_{year}.parquet" \
  --rt-path-pattern "~/data/ERCOT_data/processed/rtm/annual/{year}/rtm_lmp_{year}.parquet"
```

## Data Updates

Last updated: $(date)
Source: ERCOT historical data processed by rt_rust_processor
EOF

echo -e "${GREEN}✓ README created: $README_FILE${NC}"

echo -e "\n${GREEN}Reorganization complete!${NC}"
echo -e "View the summary at: ${BLUE}$SUMMARY_FILE${NC}"
echo -e "Total files moved: $(find "$TARGET_DIR" -name "*.parquet" 2>/dev/null | wc -l)"