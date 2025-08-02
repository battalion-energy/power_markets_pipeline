#!/usr/bin/env python3
"""
Deep dive into BESS AS revenue structure
"""

import pandas as pd
import numpy as np
from pathlib import Path
import glob

print("=" * 100)
print("🔍 DEEP ANALYSIS OF BESS ANCILLARY SERVICE REVENUE STRUCTURE")
print("=" * 100)

# 1. Check SASM for AS clearing prices and awards
print("\n1️⃣ SASM Files Analysis (AS Awards and Prices)")
print("-" * 80)
sasm_dir = Path("/Users/enrico/data/ERCOT_data/60-Day_SASM_Disclosure_Reports/csv")
sasm_files = list(sasm_dir.glob("*Generation_Resource_AS_Offer_Awards*.csv"))

if sasm_files:
    # Check a recent file
    recent_file = sorted(sasm_files)[-1]
    print(f"Inspecting: {recent_file.name}")
    
    df = pd.read_csv(recent_file, nrows=1000)
    print(f"\nColumns: {list(df.columns)}")
    
    # Check for BESS
    if 'Resource Type' in df.columns:
        bess_df = df[df['Resource Type'] == 'PWRSTR']
        print(f"\nBESS records: {len(bess_df)}")
        
        # Check AS types
        as_cols = [col for col in df.columns if any(svc in col for svc in ['Reg', 'RRS', 'ECRS', 'NonSpin'])]
        print(f"\nAS columns found: {as_cols}")

# 2. Look for AS deployment/performance data
print("\n\n2️⃣ Looking for AS Deployment/Performance Data")
print("-" * 80)

# Check SCED for AS deployment columns
sced_dir = Path("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv")
sced_gen_files = list(sced_dir.glob("*SCED_Gen_Resource_Data*.csv"))

if sced_gen_files:
    recent_file = sorted(sced_gen_files)[-1]
    print(f"Checking SCED file: {recent_file.name}")
    
    # Read a chunk
    df = pd.read_csv(recent_file, nrows=5000)
    
    # Look for AS deployment columns
    as_deployment_cols = [col for col in df.columns if 'Ancillary Service' in col]
    print(f"\nAS Deployment columns in SCED: {as_deployment_cols}")
    
    # Check a BESS
    if 'Resource Type' in df.columns:
        bess_df = df[df['Resource Type'] == 'PWRSTR']
        if len(bess_df) > 0:
            print(f"\nSample BESS AS deployment data:")
            for col in as_deployment_cols:
                if col in bess_df.columns:
                    values = pd.to_numeric(bess_df[col], errors='coerce')
                    non_zero = values[values != 0]
                    if len(non_zero) > 0:
                        print(f"  {col}: {len(non_zero)} non-zero values, avg={non_zero.mean():.2f}")

# 3. Analyze DAM AS award patterns over time
print("\n\n3️⃣ DAM AS Award Patterns Over Time")
print("-" * 80)

dam_dir = Path("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv")
dam_files = sorted(dam_dir.glob("*DAM_Gen_Resource_Data*.csv"))

# Group by year
years_data = {}
for file in dam_files:
    # Extract year from filename
    parts = file.stem.split('-')
    if len(parts) >= 3:
        year_str = parts[-1]
        try:
            year = int(year_str)
            if year < 50:
                year = 2000 + year
            else:
                year = 1900 + year
            
            if year not in years_data:
                years_data[year] = []
            years_data[year].append(file)
        except:
            pass

# Analyze a sample from each year
for year in sorted(years_data.keys())[-5:]:  # Last 5 years
    files = years_data[year]
    sample_file = files[len(files)//2]  # Middle file of the year
    
    print(f"\n{year} - Analyzing {sample_file.name}")
    
    df = pd.read_csv(sample_file)
    bess_df = df[df['Resource Type'] == 'PWRSTR']
    
    if len(bess_df) > 0:
        print(f"  BESS resources: {bess_df['Resource Name'].nunique()}")
        
        # Energy vs AS awards
        energy_awards = pd.to_numeric(bess_df['Awarded Quantity'], errors='coerce')
        energy_revenue = (energy_awards * pd.to_numeric(bess_df['Energy Settlement Point Price'], errors='coerce')).sum()
        
        # AS awards and revenues
        as_services = {
            'RegUp': ('RegUp Awarded', 'RegUp MCPC'),
            'RegDown': ('RegDown Awarded', 'RegDown MCPC'),
            'RRS': ('RRSPFR Awarded', 'RRS MCPC'),  # Using RRSPFR as primary RRS
            'ECRS': ('ECRSSD Awarded', 'ECRS MCPC'),
            'NonSpin': ('NonSpin Awarded', 'NonSpin MCPC')
        }
        
        as_revenues = {}
        for service, (award_col, price_col) in as_services.items():
            if award_col in bess_df.columns and price_col in df.columns:
                # Get the MCPC for this service (same for all resources in the hour)
                mcpcs = pd.to_numeric(df[price_col], errors='coerce').dropna()
                if len(mcpcs) > 0:
                    avg_mcpc = mcpcs[mcpcs > 0].mean() if (mcpcs > 0).any() else 0
                    awards = pd.to_numeric(bess_df[award_col], errors='coerce').fillna(0)
                    revenue = (awards * avg_mcpc).sum()
                    as_revenues[service] = revenue
                    
                    # Count resources with awards
                    resources_with_awards = (awards > 0).sum()
                    print(f"    {service}: {resources_with_awards} BESS with awards, revenue=${revenue:,.0f}")
        
        total_as_revenue = sum(as_revenues.values())
        print(f"  Energy revenue: ${energy_revenue:,.0f}")
        print(f"  Total AS revenue: ${total_as_revenue:,.0f}")
        if energy_revenue + total_as_revenue > 0:
            print(f"  AS % of total: {total_as_revenue/(energy_revenue + total_as_revenue)*100:.1f}%")

# 4. Check for missing AS deployment revenues
print("\n\n4️⃣ Checking for AS Deployment/Performance Payments")
print("-" * 80)
print("Note: AS revenues should include both capacity payments (MCPC * MW awarded)")
print("and potentially performance/deployment payments when actually called upon.")

# Look for SMNE files which might have deployment data
smne_files = list(sced_dir.glob("*SMNE*.csv"))
if smne_files:
    print(f"\nFound {len(smne_files)} SMNE files (Settlement Metered Net Energy)")
    sample = smne_files[0]
    df = pd.read_csv(sample, nrows=100)
    print(f"SMNE columns: {list(df.columns)}")

# 5. Verify Base Point includes AS deployment
print("\n\n5️⃣ Analyzing Base Point for AS Deployment")
print("-" * 80)
print("When providing AS, BESS Base Point should reflect deployment:")
print("- RegUp deployment: Base Point increases")
print("- RegDown deployment: Base Point decreases")
print("- This energy is settled at RT price, AS gets additional payment")