#!/usr/bin/env python3
"""
Final BESS Revenue Analysis - Properly calculating all revenue streams
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

# Setup
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

OUTPUT_DIR = Path("bess_complete_analysis")
OUTPUT_DIR.mkdir(exist_ok=True)

logger.info("="*80)
logger.info("💰 FINAL BESS REVENUE ANALYSIS")
logger.info("="*80)

# Paths
DAM_DIR = Path("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv")
SCED_DIR = Path("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv")
PRICE_DIR = Path("annual_output/Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones")

# Load BESS resources
bess_df = pd.read_csv("bess_analysis/bess_resources_master_list.csv")
bess_map = {row['Resource_Name']: row['Settlement_Point'] for _, row in bess_df.iterrows()}

def analyze_year_sample(year):
    """Analyze a sample of data for one year to show revenue breakdown"""
    logger.info(f"\n{'='*60}")
    logger.info(f"📅 Analyzing {year}")
    logger.info(f"{'='*60}")
    
    results = []
    
    # Get sample files
    dam_files = sorted(DAM_DIR.glob(f"*DAM_Gen_Resource_Data*{year % 100:02d}.csv"))
    sced_files = sorted(SCED_DIR.glob(f"*SCED_Gen_Resource_Data*{year % 100:02d}.csv"))
    
    if not dam_files:
        logger.info(f"No DAM files found for {year}")
        return results
        
    # Process one DAM file as sample
    sample_file = dam_files[min(len(dam_files)//2, len(dam_files)-1)]
    logger.info(f"\nSample DAM file: {sample_file.name}")
    
    df = pd.read_csv(sample_file)
    bess_data = df[df['Resource Type'] == 'PWRSTR'].copy()
    
    logger.info(f"BESS resources in file: {bess_data['Resource Name'].nunique()}")
    logger.info(f"Total BESS records: {len(bess_data)}")
    
    # Initialize revenue tracking
    revenues = {}
    
    # Process each hour
    for (date, hour), hour_group in df.groupby(['Delivery Date', 'Hour Ending']):
        hour_bess = hour_group[hour_group['Resource Type'] == 'PWRSTR']
        
        if len(hour_bess) == 0:
            continue
            
        # Get MCPCs for this hour (same for all resources)
        mcpcs = {}
        for service, col in [('RegUp', 'RegUp MCPC'), ('RegDown', 'RegDown MCPC'), 
                            ('RRS', 'RRS MCPC'), ('ECRS', 'ECRS MCPC'), 
                            ('NonSpin', 'NonSpin MCPC')]:
            if col in hour_group.columns:
                mcpcs[service] = pd.to_numeric(hour_group[col].iloc[0], errors='coerce')
            else:
                mcpcs[service] = np.nan
        
        energy_price = pd.to_numeric(hour_group['Energy Settlement Point Price'].iloc[0], errors='coerce')
        
        # Calculate revenues for each BESS in this hour
        for _, bess in hour_bess.iterrows():
            name = bess['Resource Name']
            if name not in revenues:
                revenues[name] = {
                    'dam_energy': 0, 'rt_energy': 0, 'reg_up': 0, 'reg_down': 0,
                    'rrs': 0, 'ecrs': 0, 'non_spin': 0
                }
            
            # Energy
            energy_mw = pd.to_numeric(bess['Awarded Quantity'], errors='coerce')
            if pd.notna(energy_mw) and pd.notna(energy_price):
                revenues[name]['dam_energy'] += energy_mw * energy_price
                
            # RegUp
            mw = pd.to_numeric(bess['RegUp Awarded'], errors='coerce')
            if pd.notna(mw) and mw > 0 and pd.notna(mcpcs['RegUp']):
                revenues[name]['reg_up'] += mw * mcpcs['RegUp']
                
            # RegDown
            mw = pd.to_numeric(bess['RegDown Awarded'], errors='coerce')
            if pd.notna(mw) and mw > 0 and pd.notna(mcpcs['RegDown']):
                revenues[name]['reg_down'] += mw * mcpcs['RegDown']
                
            # RRS (all types)
            rrs_total = 0
            for col in ['RRSPFR Awarded', 'RRSFFR Awarded', 'RRSUFR Awarded']:
                mw = pd.to_numeric(bess.get(col, 0), errors='coerce')
                if pd.notna(mw):
                    rrs_total += mw
            if rrs_total > 0 and pd.notna(mcpcs['RRS']):
                revenues[name]['rrs'] += rrs_total * mcpcs['RRS']
                
            # ECRS
            if 'ECRSSD Awarded' in bess.index:
                mw = pd.to_numeric(bess['ECRSSD Awarded'], errors='coerce')
                if pd.notna(mw) and mw > 0 and pd.notna(mcpcs['ECRS']):
                    revenues[name]['ecrs'] += mw * mcpcs['ECRS']
                
            # NonSpin
            mw = pd.to_numeric(bess['NonSpin Awarded'], errors='coerce')
            if pd.notna(mw) and mw > 0 and pd.notna(mcpcs['NonSpin']):
                revenues[name]['non_spin'] += mw * mcpcs['NonSpin']
    
    # Add RT energy sample
    if sced_files:
        # Load RT prices
        price_file = PRICE_DIR / f"Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_{year}.parquet"
        if price_file.exists():
            logger.info(f"\nLoading RT prices...")
            price_df = pd.read_parquet(price_file)
            # Simple average by settlement point
            avg_prices = price_df.groupby('SettlementPointName')['SettlementPointPrice'].mean().to_dict()
        else:
            avg_prices = {}
            
        # Process one SCED file
        sced_file = sced_files[0]
        logger.info(f"Sample SCED file: {sced_file.name}")
        
        # Read first 50k rows
        sced_df = pd.read_csv(sced_file, nrows=50000)
        sced_bess = sced_df[sced_df['Resource Type'] == 'PWRSTR']
        
        for _, row in sced_bess.iterrows():
            name = row['Resource Name']
            if name in revenues:
                base_mw = pd.to_numeric(row['Base Point'], errors='coerce')
                if pd.notna(base_mw) and base_mw != 0:
                    # Get price
                    sp = bess_map.get(name, '')
                    price = avg_prices.get(sp, 50)  # Default $50
                    # 5-minute energy
                    revenues[name]['rt_energy'] += base_mw * price * (5/60)
    
    # Create results
    for name, rev in revenues.items():
        total_as = rev['reg_up'] + rev['reg_down'] + rev['rrs'] + rev['ecrs'] + rev['non_spin']
        total = sum(rev.values())
        
        if total > 0:
            results.append({
                'BESS_Asset_Name': name,
                'Year': year,
                'RT_Revenue': rev['rt_energy'],
                'DA_Revenue': rev['dam_energy'],
                'Spin_Revenue': rev['rrs'],
                'NonSpin_Revenue': rev['non_spin'],
                'RegUp_Revenue': rev['reg_up'],
                'RegDown_Revenue': rev['reg_down'],
                'ECRS_Revenue': rev['ecrs'],
                'Total_Revenue': total,
                'Energy_Revenue': rev['dam_energy'] + rev['rt_energy'],
                'AS_Revenue': total_as,
                'Energy_Pct': (rev['dam_energy'] + rev['rt_energy'])/total*100 if total > 0 else 0,
                'AS_Pct': total_as/total*100 if total > 0 else 0
            })
    
    # Summary
    if results:
        df_results = pd.DataFrame(results)
        total_revenue = df_results['Total_Revenue'].sum()
        energy_revenue = df_results['Energy_Revenue'].sum()
        as_revenue = df_results['AS_Revenue'].sum()
        
        logger.info(f"\n📊 {year} Sample Results:")
        logger.info(f"  BESS with revenue: {len(df_results)}")
        logger.info(f"  Total revenue: ${total_revenue:,.0f}")
        logger.info(f"  Energy revenue: ${energy_revenue:,.0f} ({energy_revenue/total_revenue*100:.1f}%)")
        logger.info(f"  AS revenue: ${as_revenue:,.0f} ({as_revenue/total_revenue*100:.1f}%)")
        
        # Show AS breakdown
        logger.info(f"\n  AS Revenue Breakdown:")
        logger.info(f"    RegUp: ${df_results['RegUp_Revenue'].sum():,.0f}")
        logger.info(f"    RegDown: ${df_results['RegDown_Revenue'].sum():,.0f}")
        logger.info(f"    RRS/Spin: ${df_results['Spin_Revenue'].sum():,.0f}")
        logger.info(f"    ECRS: ${df_results['ECRS_Revenue'].sum():,.0f}")
        logger.info(f"    NonSpin: ${df_results['NonSpin_Revenue'].sum():,.0f}")
        
        # Top earners
        logger.info(f"\n  Top 5 BESS by Total Revenue:")
        top5 = df_results.nlargest(5, 'Total_Revenue')
        for _, row in top5.iterrows():
            logger.info(f"    {row['BESS_Asset_Name']}: ${row['Total_Revenue']:,.0f} (Energy: {row['Energy_Pct']:.0f}%, AS: {row['AS_Pct']:.0f}%)")
    
    return results

# Analyze sample years
all_results = []
for year in [2022, 2023, 2024, 2025]:
    results = analyze_year_sample(year)
    all_results.extend(results)

# Save final results
if all_results:
    df_final = pd.DataFrame(all_results)
    
    csv_path = OUTPUT_DIR / "bess_revenues_final_sample.csv"
    df_final.to_csv(csv_path, index=False)
    
    logger.info(f"\n💾 Results saved to: {csv_path}")
    
    # Overall summary
    logger.info("\n" + "="*80)
    logger.info("📊 OVERALL SUMMARY (Sample Data)")
    logger.info("="*80)
    
    for year in sorted(df_final['Year'].unique()):
        year_data = df_final[df_final['Year'] == year]
        logger.info(f"\n{year}:")
        logger.info(f"  Average Energy %: {year_data['Energy_Pct'].mean():.1f}%")
        logger.info(f"  Average AS %: {year_data['AS_Pct'].mean():.1f}%")

logger.info("\n✅ Analysis complete!")