#!/usr/bin/env python3
"""
Quick check of AS revenue calculation
Focus on a single recent file to verify the calculations
"""

import pandas as pd
import numpy as np
from pathlib import Path

print("=" * 80)
print("🔍 QUICK AS REVENUE CHECK")
print("=" * 80)

# Check a recent DAM file
dam_dir = Path("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv")
dam_files = sorted(dam_dir.glob("*DAM_Gen_Resource_Data*24.csv"))

if dam_files:
    # Pick a file from mid-2024
    test_file = dam_files[len(dam_files)//2]
    print(f"\nAnalyzing: {test_file.name}")
    
    df = pd.read_csv(test_file)
    print(f"Total records: {len(df)}")
    
    # Filter for BESS
    bess_df = df[df['Resource Type'] == 'PWRSTR'].copy()
    print(f"BESS records: {len(bess_df)}")
    print(f"Unique BESS: {bess_df['Resource Name'].nunique()}")
    
    # Check AS awards
    print("\n📊 AS Awards Summary:")
    as_services = {
        'RegUp': 'RegUp Awarded',
        'RegDown': 'RegDown Awarded', 
        'RRSPFR': 'RRSPFR Awarded',
        'RRSFFR': 'RRSFFR Awarded',
        'RRSUFR': 'RRSUFR Awarded',
        'ECRS': 'ECRSSD Awarded',
        'NonSpin': 'NonSpin Awarded'
    }
    
    for service, col in as_services.items():
        if col in bess_df.columns:
            awards = pd.to_numeric(bess_df[col], errors='coerce')
            awarded = awards[awards > 0]
            if len(awarded) > 0:
                print(f"  {service}: {len(awarded)} awards, avg={awarded.mean():.1f} MW, total={awarded.sum():.0f} MW")
    
    # Check MCPCs
    print("\n💰 AS Clearing Prices (MCPC):")
    mcpc_cols = ['RegUp MCPC', 'RegDown MCPC', 'RRS MCPC', 'ECRS MCPC', 'NonSpin MCPC']
    for col in mcpc_cols:
        if col in df.columns:
            prices = pd.to_numeric(df[col], errors='coerce')
            non_zero = prices[prices > 0]
            if len(non_zero) > 0:
                print(f"  {col}: min=${non_zero.min():.2f}, avg=${non_zero.mean():.2f}, max=${non_zero.max():.2f}")
    
    # Calculate sample revenues
    print("\n💵 Sample Revenue Calculations:")
    
    # Pick a sample hour with good AS activity
    sample_date = bess_df['Delivery Date'].iloc[0]
    sample_hour = bess_df['Hour Ending'].iloc[0]
    
    hour_data = bess_df[(bess_df['Delivery Date'] == sample_date) & 
                        (bess_df['Hour Ending'] == sample_hour)]
    
    print(f"\nSample Hour: {sample_date} HE{sample_hour}")
    print(f"BESS in this hour: {len(hour_data)}")
    
    # Get MCPCs for this hour
    hour_all = df[(df['Delivery Date'] == sample_date) & 
                  (df['Hour Ending'] == sample_hour)]
    
    if len(hour_all) > 0:
        regup_mcpc = pd.to_numeric(hour_all['RegUp MCPC'].iloc[0], errors='coerce')
        regdn_mcpc = pd.to_numeric(hour_all['RegDown MCPC'].iloc[0], errors='coerce')
        rrs_mcpc = pd.to_numeric(hour_all['RRS MCPC'].iloc[0], errors='coerce')
        ecrs_mcpc = pd.to_numeric(hour_all['ECRS MCPC'].iloc[0], errors='coerce')
        
        print(f"\nMCPCs for this hour:")
        print(f"  RegUp: ${regup_mcpc:.2f}/MW")
        print(f"  RegDown: ${regdn_mcpc:.2f}/MW")
        print(f"  RRS: ${rrs_mcpc:.2f}/MW")
        print(f"  ECRS: ${ecrs_mcpc:.2f}/MW")
        
        # Calculate revenues for each BESS
        total_as_revenue = 0
        for _, bess in hour_data.iterrows():
            bess_revenue = 0
            
            # RegUp
            regup_mw = pd.to_numeric(bess['RegUp Awarded'], errors='coerce')
            if pd.notna(regup_mw) and regup_mw > 0 and pd.notna(regup_mcpc):
                bess_revenue += regup_mw * regup_mcpc
            
            # RegDown  
            regdn_mw = pd.to_numeric(bess['RegDown Awarded'], errors='coerce')
            if pd.notna(regdn_mw) and regdn_mw > 0 and pd.notna(regdn_mcpc):
                bess_revenue += regdn_mw * regdn_mcpc
                
            # RRS (all types)
            rrs_total = 0
            for rrs_col in ['RRSPFR Awarded', 'RRSFFR Awarded', 'RRSUFR Awarded']:
                if rrs_col in bess:
                    rrs_mw = pd.to_numeric(bess[rrs_col], errors='coerce')
                    if pd.notna(rrs_mw):
                        rrs_total += rrs_mw
            if rrs_total > 0 and pd.notna(rrs_mcpc):
                bess_revenue += rrs_total * rrs_mcpc
                
            # ECRS
            ecrs_mw = pd.to_numeric(bess['ECRSSD Awarded'], errors='coerce')
            if pd.notna(ecrs_mw) and ecrs_mw > 0 and pd.notna(ecrs_mcpc):
                bess_revenue += ecrs_mw * ecrs_mcpc
                
            if bess_revenue > 0:
                print(f"\n  {bess['Resource Name']}: ${bess_revenue:.2f}")
                total_as_revenue += bess_revenue
        
        print(f"\nTotal AS revenue this hour: ${total_as_revenue:.2f}")

# Now check SCED for AS deployment
print("\n" + "="*80)
print("🔍 CHECKING SCED AS DEPLOYMENT")
print("="*80)

sced_dir = Path("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv")
sced_files = sorted(sced_dir.glob("*SCED_Gen_Resource_Data*24.csv"))

if sced_files:
    test_file = sced_files[0]
    print(f"\nAnalyzing: {test_file.name}")
    
    # Read first 10000 rows
    df = pd.read_csv(test_file, nrows=10000)
    bess_df = df[df['Resource Type'] == 'PWRSTR'].copy()
    
    print(f"BESS records in sample: {len(bess_df)}")
    
    # Check AS deployment columns
    as_deploy_cols = ['Ancillary Service REGUP', 'Ancillary Service REGDN', 
                      'Ancillary Service RRS', 'Ancillary Service ECRS']
    
    print("\n📊 AS Deployment Statistics:")
    for col in as_deploy_cols:
        if col in bess_df.columns:
            values = pd.to_numeric(bess_df[col], errors='coerce')
            deployed = values[values > 0]
            if len(deployed) > 0:
                print(f"  {col}: {len(deployed)} deployments, avg={deployed.mean():.1f} MW")
    
    # Check Base Point vs AS deployment
    print("\n⚡ Base Point Analysis:")
    base_points = pd.to_numeric(bess_df['Base Point'], errors='coerce')
    print(f"  Positive (discharge): {(base_points > 0).sum()}")
    print(f"  Negative (charge): {(base_points < 0).sum()}")  
    print(f"  Zero: {(base_points == 0).sum()}")
    
    # Show sample with AS deployment
    for col in as_deploy_cols:
        if col in bess_df.columns:
            deployed_mask = pd.to_numeric(bess_df[col], errors='coerce') > 0
            if deployed_mask.any():
                sample = bess_df[deployed_mask].iloc[0]
                print(f"\nSample {col} deployment:")
                print(f"  Resource: {sample['Resource Name']}")
                print(f"  Base Point: {sample['Base Point']} MW")
                print(f"  {col}: {sample[col]} MW")
                break