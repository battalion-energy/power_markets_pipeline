#!/bin/bash

echo "🔋 ERCOT BESS Revenue Analysis from 60-Day Disclosure Data"
echo "==========================================================="

# Check if we need to extract CSV files
if [ ! -d "disclosure_data/csv" ]; then
    echo "📂 Extracting disclosure ZIP files..."
    mkdir -p disclosure_data/csv
    
    # Extract COP snapshot files
    cd disclosure_data
    for zip in *.zip; do
        if [ -f "$zip" ]; then
            echo "  Extracting $zip..."
            unzip -q -o "$zip" -d csv/
        fi
    done
    cd ..
fi

# Check for other disclosure directories
for dir in "60-Day_SCED_Disclosure_Reports" "60-Day_DAM_Disclosure_Reports" "60-Day_SASM_Disclosure_Reports"; do
    if [ -d "/Users/enrico/data/ERCOT_data/$dir" ] && [ ! -d "disclosure_data/${dir}_extracted" ]; then
        echo "📂 Extracting $dir files..."
        mkdir -p "disclosure_data/${dir}_extracted"
        
        cd "/Users/enrico/data/ERCOT_data/$dir"
        for zip in *.zip; do
            if [ -f "$zip" ]; then
                unzip -q -o "$zip" -d "/Users/enrico/proj/power_market_pipeline/rt_rust_processor/disclosure_data/${dir}_extracted/"
            fi
        done
        cd -
    fi
done

# Now run the Rust program with the original BESS revenue calculator
# We'll need to modify the code to use the original calculator
echo "Running BESS revenue analysis..."

# For now, let's use Python to quickly analyze the data
python3 << 'EOF'
import pandas as pd
import numpy as np
from pathlib import Path
import glob

print("\n📊 Analyzing BESS revenues from 60-day disclosure data...")

# Load BESS master list
bess_df = pd.read_csv('bess_analysis/bess_resources_master_list.csv')
print(f"Loaded {len(bess_df)} BESS resources")

# Find DAM Gen Resource Data files
dam_files = glob.glob('disclosure_data/csv/*DAM_Gen_Resource_Data*.csv')
print(f"\nFound {len(dam_files)} DAM Gen Resource Data files")

if dam_files:
    # Process first file as example
    df = pd.read_csv(dam_files[0])
    print(f"\nColumns in DAM file: {list(df.columns)}")
    
    # Filter for BESS resources
    if 'Resource Type' in df.columns:
        bess_data = df[df['Resource Type'] == 'PWRSTR']
        print(f"Found {len(bess_data)} BESS records in first file")
        
        if len(bess_data) > 0:
            print("\nSample BESS data:")
            print(bess_data[['Resource Name', 'Delivery Date', 'Hour Ending', 
                           'Energy Awarded', 'Energy Settlement Point Price']].head())

# Find SCED Gen Resource Data files  
sced_files = glob.glob('disclosure_data/60-Day_SCED_Disclosure_Reports_extracted/*SCED_Gen_Resource_Data*.csv')
print(f"\n\nFound {len(sced_files)} SCED Gen Resource Data files")

print("\n✅ Analysis setup complete. Full implementation in Rust processor.")
EOF