#!/usr/bin/env python3
"""
Comprehensive inspection of all 60-day disclosure directories
"""

import pandas as pd
import glob
import os
from pathlib import Path

# All 5 disclosure directories
disclosure_dirs = {
    "COP_Adjustment": "/Users/enrico/data/ERCOT_data/60-Day_COP_Adjustment_Period_Snapshot/csv",
    "DAM_Disclosure": "/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv",
    "SCED_Disclosure": "/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv",
    "SASM_Disclosure": "/Users/enrico/data/ERCOT_data/60-Day_SASM_Disclosure_Reports/csv",
    "COP_All_Updates": "/Users/enrico/data/ERCOT_data/60-Day_COP_All_Updates/csv"
}

print("=" * 100)
print("📁 COMPREHENSIVE 60-DAY DISCLOSURE INSPECTION")
print("=" * 100)

for dir_name, dir_path in disclosure_dirs.items():
    print(f"\n{'='*80}")
    print(f"📂 {dir_name}: {dir_path}")
    print(f"{'='*80}")
    
    if not os.path.exists(dir_path):
        print(f"❌ Directory not found!")
        continue
    
    # Get all CSV files
    csv_files = glob.glob(os.path.join(dir_path, "*.csv"))
    print(f"Total CSV files: {len(csv_files)}")
    
    # Group by file pattern
    file_patterns = {}
    for f in csv_files:
        base = os.path.basename(f)
        # Extract pattern by removing date
        parts = base.split('-')
        if len(parts) >= 3:
            pattern = '-'.join(parts[:-3]) + '-*.csv'
        else:
            pattern = base.split('.')[0] + '*.csv'
        
        if pattern not in file_patterns:
            file_patterns[pattern] = []
        file_patterns[pattern].append(f)
    
    print(f"\nFile types found ({len(file_patterns)}):")
    for pattern, files in sorted(file_patterns.items()):
        print(f"  {pattern}: {len(files)} files")

# Now let's inspect specific files that are critical for BESS revenue
print("\n" + "="*100)
print("🔍 DETAILED INSPECTION OF KEY FILES FOR BESS REVENUE")
print("="*100)

# 1. DAM Gen Resource Data - check for AS awards and prices
print("\n1️⃣ DAM Gen Resource Data (Ancillary Service Awards)")
print("-" * 80)
dam_files = glob.glob(os.path.join(disclosure_dirs["DAM_Disclosure"], "*DAM_Gen_Resource_Data*.csv"))
if dam_files:
    # Check a recent file
    recent_file = sorted(dam_files)[-1]
    print(f"Inspecting: {os.path.basename(recent_file)}")
    
    df = pd.read_csv(recent_file, nrows=1000)
    print(f"\nColumns ({len(df.columns)}):")
    for i, col in enumerate(df.columns):
        print(f"  {i+1:3d}. {col}")
    
    # Check BESS data
    if 'Resource Type' in df.columns:
        bess_df = df[df['Resource Type'] == 'PWRSTR']
        print(f"\nBESS records in sample: {len(bess_df)}")
        
        if len(bess_df) > 0:
            # Check AS awards
            as_columns = [col for col in df.columns if 'Awarded' in col or 'MCPC' in col]
            print(f"\nAncillary Service columns found:")
            for col in as_columns:
                non_zero = (pd.to_numeric(bess_df[col], errors='coerce') > 0).sum()
                print(f"  {col}: {non_zero} non-zero values")

# 2. SCED Gen Resource Data - check for actual dispatch
print("\n2️⃣ SCED Gen Resource Data (Real-Time Dispatch)")
print("-" * 80)
sced_files = glob.glob(os.path.join(disclosure_dirs["SCED_Disclosure"], "*SCED_Gen_Resource_Data*.csv"))
if sced_files:
    recent_file = sorted(sced_files)[-1]
    print(f"Inspecting: {os.path.basename(recent_file)}")
    
    # Read smaller sample due to large size
    df = pd.read_csv(recent_file, nrows=10000)
    print(f"\nColumns ({len(df.columns)}):")
    for i, col in enumerate(df.columns):
        print(f"  {i+1:3d}. {col}")
    
    # Check BESS data
    if 'Resource Type' in df.columns:
        bess_df = df[df['Resource Type'] == 'PWRSTR']
        print(f"\nBESS records in sample: {len(bess_df)}")
        
        if len(bess_df) > 0 and 'Base Point' in bess_df.columns:
            base_points = pd.to_numeric(bess_df['Base Point'], errors='coerce')
            print(f"\nBase Point statistics (MW):")
            print(f"  Positive (discharge): {(base_points > 0).sum()} records")
            print(f"  Negative (charge): {(base_points < 0).sum()} records")
            print(f"  Zero: {(base_points == 0).sum()} records")
            print(f"  Max discharge: {base_points.max():.2f} MW")
            print(f"  Max charge: {base_points.min():.2f} MW")

# 3. SASM Data - check for AS clearing prices
print("\n3️⃣ SASM Disclosure (Supplemental AS Market)")
print("-" * 80)
sasm_files = glob.glob(os.path.join(disclosure_dirs["SASM_Disclosure"], "*.csv"))
if sasm_files:
    # Look for MCPC files
    mcpc_files = [f for f in sasm_files if 'MCPC' in f or 'Price' in f]
    print(f"Found {len(mcpc_files)} price-related files")
    
    if mcpc_files:
        sample_file = mcpc_files[0]
        print(f"\nInspecting: {os.path.basename(sample_file)}")
        df = pd.read_csv(sample_file, nrows=100)
        print(f"Columns: {list(df.columns)}")

# 4. Check a specific BESS across different files
print("\n4️⃣ Tracking a Sample BESS Across Files")
print("-" * 80)
sample_bess = "NF_BRP_BES1"  # This one showed revenue in our results

# Check in DAM
if dam_files:
    for file in dam_files[-3:]:  # Last 3 files
        df = pd.read_csv(file)
        if 'Resource Name' in df.columns:
            bess_data = df[df['Resource Name'] == sample_bess]
            if len(bess_data) > 0:
                print(f"\n{os.path.basename(file)}:")
                print(f"  Found {len(bess_data)} records for {sample_bess}")
                # Show non-zero awards
                for col in df.columns:
                    if 'Awarded' in col and col in bess_data.columns:
                        values = pd.to_numeric(bess_data[col], errors='coerce')
                        non_zero = values[values > 0]
                        if len(non_zero) > 0:
                            print(f"  {col}: {len(non_zero)} awards, avg={non_zero.mean():.2f}")

# 5. Look for AS deployment/dispatch data
print("\n5️⃣ Looking for AS Deployment Data")
print("-" * 80)
# Check for AS deployment files
for dir_name, dir_path in disclosure_dirs.items():
    deployment_files = glob.glob(os.path.join(dir_path, "*Deploy*"))
    if deployment_files:
        print(f"\n{dir_name} has {len(deployment_files)} deployment files")
        for f in deployment_files[:3]:
            print(f"  - {os.path.basename(f)}")