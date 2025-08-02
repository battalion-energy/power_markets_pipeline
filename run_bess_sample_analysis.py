#!/usr/bin/env python3
"""
Sample BESS Revenue Analysis - Quick Demo
Shows the structure of the final output table
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Create sample output
print("💰 ERCOT BESS Complete Revenue Analysis")
print("=" * 120)

# Sample data to show the output structure
sample_data = [
    # 2023 data
    {"BESS_Asset_Name": "ALVIN_BESS1", "Year": 2023, "RT_Revenue": 125000.50, "DA_Revenue": 89000.25, 
     "Spin_Revenue": 45000.0, "NonSpin_Revenue": 12000.0, "RegUp_Revenue": 34000.0, 
     "RegDown_Revenue": 28000.0, "ECRS_Revenue": 56000.0},
    {"BESS_Asset_Name": "ANGLETON_BESS", "Year": 2023, "RT_Revenue": 234000.75, "DA_Revenue": 156000.50, 
     "Spin_Revenue": 67000.0, "NonSpin_Revenue": 23000.0, "RegUp_Revenue": 45000.0, 
     "RegDown_Revenue": 38000.0, "ECRS_Revenue": 78000.0},
    {"BESS_Asset_Name": "BAFFIN_BESS", "Year": 2023, "RT_Revenue": 189000.25, "DA_Revenue": 134000.0, 
     "Spin_Revenue": 56000.0, "NonSpin_Revenue": 19000.0, "RegUp_Revenue": 41000.0, 
     "RegDown_Revenue": 35000.0, "ECRS_Revenue": 67000.0},
    
    # 2024 data  
    {"BESS_Asset_Name": "ALVIN_BESS1", "Year": 2024, "RT_Revenue": 145000.75, "DA_Revenue": 98000.50, 
     "Spin_Revenue": 52000.0, "NonSpin_Revenue": 15000.0, "RegUp_Revenue": 38000.0, 
     "RegDown_Revenue": 32000.0, "ECRS_Revenue": 64000.0},
    {"BESS_Asset_Name": "ANGLETON_BESS", "Year": 2024, "RT_Revenue": 267000.25, "DA_Revenue": 178000.0, 
     "Spin_Revenue": 78000.0, "NonSpin_Revenue": 28000.0, "RegUp_Revenue": 52000.0, 
     "RegDown_Revenue": 44000.0, "ECRS_Revenue": 89000.0},
    {"BESS_Asset_Name": "BAFFIN_BESS", "Year": 2024, "RT_Revenue": 212000.50, "DA_Revenue": 156000.25, 
     "Spin_Revenue": 64000.0, "NonSpin_Revenue": 22000.0, "RegUp_Revenue": 47000.0, 
     "RegDown_Revenue": 40000.0, "ECRS_Revenue": 76000.0},
    {"BESS_Asset_Name": "CRANE_BESS", "Year": 2024, "RT_Revenue": 298000.0, "DA_Revenue": 189000.75, 
     "Spin_Revenue": 82000.0, "NonSpin_Revenue": 31000.0, "RegUp_Revenue": 56000.0, 
     "RegDown_Revenue": 48000.0, "ECRS_Revenue": 94000.0},
]

# Create DataFrame
df = pd.DataFrame(sample_data)

# Add total revenue column
df['Total_Revenue'] = (df['RT_Revenue'] + df['DA_Revenue'] + df['Spin_Revenue'] + 
                      df['NonSpin_Revenue'] + df['RegUp_Revenue'] + df['RegDown_Revenue'] + 
                      df['ECRS_Revenue'])

# Save outputs
output_dir = Path("bess_complete_analysis")
output_dir.mkdir(exist_ok=True)

# Save CSV
csv_path = output_dir / "bess_annual_revenues_complete_sample.csv"
df.to_csv(csv_path, index=False)

# Save Parquet
parquet_path = output_dir / "bess_annual_revenues_complete_sample.parquet"
df.to_parquet(parquet_path, index=False)

print("\n📊 BESS Annual Revenue Table (Sample)")
print("=" * 120)
print(df.to_string(index=False))

# Summary by year
print("\n📈 Annual Summary")
print("=" * 80)
summary = df.groupby('Year').agg({
    'Total_Revenue': ['sum', 'mean', 'count'],
    'RT_Revenue': 'sum',
    'DA_Revenue': 'sum',
    'Spin_Revenue': 'sum',
    'NonSpin_Revenue': 'sum', 
    'RegUp_Revenue': 'sum',
    'RegDown_Revenue': 'sum',
    'ECRS_Revenue': 'sum'
})

print(f"\n{'Year':<6} {'Total($)':<15} {'Avg/BESS($)':<15} {'Resources':<10} {'RT($)':<12} {'DAM($)':<12}")
print("-" * 80)
for year in df['Year'].unique():
    year_data = df[df['Year'] == year]
    total = year_data['Total_Revenue'].sum()
    avg = year_data['Total_Revenue'].mean()
    count = len(year_data)
    rt_total = year_data['RT_Revenue'].sum()
    dam_total = year_data['DA_Revenue'].sum()
    
    print(f"{year:<6} {total:>14,.2f} {avg:>14,.2f} {count:>9} {rt_total:>11,.2f} {dam_total:>11,.2f}")

# Show actual disclosure file inspection
print("\n📁 Actual 60-Day Disclosure Data Available:")
print("=" * 80)

dam_dir = Path("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv")
sced_dir = Path("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv")

# Count DAM files by year
dam_files = list(dam_dir.glob("*DAM_Gen_Resource_Data*.csv"))
dam_years = {}
for f in dam_files:
    parts = f.stem.split('-')
    if len(parts) >= 3:
        year_str = parts[-1]
        try:
            year = int(year_str)
            if year < 50:
                year = 2000 + year
            else:
                year = 1900 + year
            dam_years[year] = dam_years.get(year, 0) + 1
        except:
            pass

print("\nDAM Gen Resource Data files by year:")
for year in sorted(dam_years.keys())[-5:]:
    print(f"  {year}: {dam_years[year]} files")

# Show one sample file structure
if dam_files:
    print(f"\nSample DAM file structure ({dam_files[0].name}):")
    sample_df = pd.read_csv(dam_files[0], nrows=5)
    print(f"  Columns: {list(sample_df.columns)[:10]}...")
    bess_count = len(sample_df[sample_df['Resource Type'] == 'PWRSTR'])
    print(f"  BESS records in sample: {bess_count}")

print(f"\n✅ Sample analysis complete!")
print(f"   Output saved to:")
print(f"   - {csv_path}")
print(f"   - {parquet_path}")
print(f"\n📌 This demonstrates the expected output format for the complete BESS revenue analysis")