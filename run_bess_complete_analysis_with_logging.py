#!/usr/bin/env python3
"""
Complete BESS Revenue Analysis with Logging
Processes 60-day disclosure data to calculate comprehensive revenues
"""

import pandas as pd
import numpy as np
from pathlib import Path
import glob
from datetime import datetime
import os
import sys
import logging

# Set up logging to both file and console
LOG_FILE = "bess_complete_analysis/bess_analysis.log"
Path("bess_complete_analysis").mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Paths
DAM_DIR = Path("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv")
SCED_DIR = Path("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv")
PRICE_DIR = Path("annual_output/Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones")
OUTPUT_DIR = Path("bess_complete_analysis")
OUTPUT_DIR.mkdir(exist_ok=True)

logger.info("=" * 80)
logger.info("💰 ERCOT BESS Complete Revenue Analysis")
logger.info("=" * 80)
logger.info(f"Log file: {LOG_FILE}")
logger.info(f"You can monitor progress with: tail -f {LOG_FILE}")
logger.info("")

# Load BESS master list
logger.info("📋 Loading BESS resources...")
bess_df = pd.read_csv("bess_analysis/bess_resources_master_list.csv")
bess_resources = {row['Resource_Name']: row for _, row in bess_df.iterrows()}
logger.info(f"   Found {len(bess_resources)} BESS resources")

# Key BESS to track
key_bess = list(bess_resources.keys())[:10]  # Track first 10 for detailed logging

def process_year(year):
    """Process all data for a given year"""
    logger.info(f"\n{'='*60}")
    logger.info(f"📅 Processing year {year}")
    logger.info(f"{'='*60}")
    
    # Initialize results
    results = []
    
    # Get DAM files for this year
    dam_pattern = f"*DAM_Gen_Resource_Data*{year % 100:02d}.csv"
    dam_files = list(DAM_DIR.glob(dam_pattern))
    logger.info(f"Found {len(dam_files)} DAM files for year {year}")
    
    # Get SCED files for this year
    sced_pattern = f"*SCED_Gen_Resource_Data*{year % 100:02d}.csv"
    sced_files = list(SCED_DIR.glob(sced_pattern))
    logger.info(f"Found {len(sced_files)} SCED files for year {year}")
    
    if not dam_files and not sced_files:
        logger.warning(f"No files found for year {year}, skipping")
        return results
    
    # Initialize revenue accumulators
    revenues = {}
    for name in bess_resources:
        revenues[name] = {
            'rt_energy': 0.0,
            'dam_energy': 0.0,
            'reg_up': 0.0,
            'reg_down': 0.0,
            'spin': 0.0,  # RRS
            'non_spin': 0.0,
            'ecrs': 0.0,
            'records_processed': 0
        }
    
    # Process DAM files
    logger.info(f"\n📊 Processing DAM data for {year}...")
    for i, file in enumerate(dam_files):
        logger.info(f"  Processing DAM file {i+1}/{len(dam_files)}: {file.name}")
        try:
            df = pd.read_csv(file)
            total_rows = len(df)
            
            # Filter for BESS
            bess_data = df[df['Resource Type'] == 'PWRSTR'].copy()
            bess_rows = len(bess_data)
            logger.info(f"    Found {bess_rows} BESS records out of {total_rows} total")
            
            if bess_rows == 0:
                continue
                
            # Energy revenue
            if 'Awarded Quantity' in bess_data.columns and 'Energy Settlement Point Price' in bess_data.columns:
                bess_data['dam_revenue'] = pd.to_numeric(bess_data['Awarded Quantity'], errors='coerce').fillna(0) * \
                                          pd.to_numeric(bess_data['Energy Settlement Point Price'], errors='coerce').fillna(0)
                
                for name in bess_data['Resource Name'].unique():
                    if name in revenues:
                        rev = bess_data[bess_data['Resource Name'] == name]['dam_revenue'].sum()
                        revenues[name]['dam_energy'] += rev
                        revenues[name]['records_processed'] += len(bess_data[bess_data['Resource Name'] == name])
                        
                        if name in key_bess and rev > 0:
                            logger.debug(f"      {name}: DAM energy revenue +${rev:,.2f}")
            
            # AS revenues
            as_services = {
                'RegUp Awarded': ('RegUp MCPC', 'reg_up'),
                'RegDown Awarded': ('RegDown MCPC', 'reg_down'),
                'ECRSSD Awarded': ('ECRS MCPC', 'ecrs'),
                'NonSpin Awarded': ('NonSpin MCPC', 'non_spin')
            }
            
            for award_col, (price_col, revenue_key) in as_services.items():
                if award_col in bess_data.columns and price_col in df.columns:
                    # Get the price for all resources (not just BESS)
                    price_df = df[[price_col]].drop_duplicates()
                    if len(price_df) > 0:
                        price = pd.to_numeric(price_df[price_col].iloc[0], errors='coerce')
                        if pd.notna(price) and price > 0:
                            bess_data[f'{revenue_key}_revenue'] = pd.to_numeric(bess_data[award_col], errors='coerce').fillna(0) * price
                            
                            for name in bess_data['Resource Name'].unique():
                                if name in revenues:
                                    rev = bess_data[bess_data['Resource Name'] == name][f'{revenue_key}_revenue'].sum()
                                    revenues[name][revenue_key] += rev
                                    
                                    if name in key_bess and rev > 0:
                                        logger.debug(f"      {name}: {revenue_key} revenue +${rev:,.2f}")
            
            # RRS (Spin) - combine all RRS types
            rrs_types = ['RRSPFR Awarded', 'RRSFFR Awarded', 'RRSUFR Awarded']
            if 'RRS MCPC' in df.columns:
                price_df = df[['RRS MCPC']].drop_duplicates()
                if len(price_df) > 0:
                    rrs_price = pd.to_numeric(price_df['RRS MCPC'].iloc[0], errors='coerce')
                    if pd.notna(rrs_price) and rrs_price > 0:
                        for rrs_col in rrs_types:
                            if rrs_col in bess_data.columns:
                                bess_data['rrs_revenue'] = pd.to_numeric(bess_data[rrs_col], errors='coerce').fillna(0) * rrs_price
                                
                                for name in bess_data['Resource Name'].unique():
                                    if name in revenues:
                                        rev = bess_data[bess_data['Resource Name'] == name]['rrs_revenue'].sum()
                                        revenues[name]['spin'] += rev
                                        
                                        if name in key_bess and rev > 0:
                                            logger.debug(f"      {name}: RRS ({rrs_col}) revenue +${rev:,.2f}")
                        
        except Exception as e:
            logger.error(f"    Error processing {file.name}: {e}")
    
    # Load RT prices
    logger.info(f"\n📈 Loading RT prices for {year}...")
    price_file = PRICE_DIR / f"Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_{year}.parquet"
    rt_prices = {}
    
    if price_file.exists():
        try:
            price_df = pd.read_parquet(price_file)
            logger.info(f"  Loaded {len(price_df):,} price records")
            
            # Create average price by settlement point (simplified)
            avg_prices = price_df.groupby('SettlementPointName')['SettlementPointPrice'].mean()
            rt_prices = avg_prices.to_dict()
            logger.info(f"  Created price lookup for {len(rt_prices):,} settlement points")
            
        except Exception as e:
            logger.error(f"  Error loading price file: {e}")
    else:
        logger.warning(f"  Price file not found: {price_file}")
    
    # Process SCED files
    logger.info(f"\n⚡ Processing SCED data for {year}...")
    for i, file in enumerate(sced_files[:5]):  # Process first 5 files as example
        logger.info(f"  Processing SCED file {i+1}/5: {file.name}")
        try:
            # Read in chunks due to large size
            chunk_size = 100000
            total_bess_records = 0
            
            for chunk_num, chunk in enumerate(pd.read_csv(file, chunksize=chunk_size)):
                bess_data = chunk[chunk['Resource Type'] == 'PWRSTR'].copy()
                total_bess_records += len(bess_data)
                
                if len(bess_data) == 0:
                    continue
                
                # RT revenue calculation
                for name in bess_data['Resource Name'].unique():
                    if name in revenues and name in bess_resources:
                        sp = bess_resources[name].get('Settlement_Point', '')
                        if sp in rt_prices:
                            base_points = pd.to_numeric(bess_data[bess_data['Resource Name'] == name]['Base Point'], errors='coerce').fillna(0)
                            # RT revenue = MW * $/MWh * hours (5 min = 1/12 hour)
                            rev = base_points.sum() * rt_prices[sp] * (5/60)
                            revenues[name]['rt_energy'] += rev
                            
                            if name in key_bess and rev > 0:
                                logger.debug(f"      {name}: RT energy revenue +${rev:,.2f}")
                
                if chunk_num % 10 == 0:
                    logger.info(f"    Processed {chunk_num + 1} chunks, found {total_bess_records} BESS records so far")
                            
        except Exception as e:
            logger.error(f"    Error processing {file.name}: {e}")
    
    # Create results
    logger.info(f"\n📋 Creating results for {year}...")
    active_resources = 0
    total_revenue = 0
    
    for name, rev in revenues.items():
        total = sum([rev[k] for k in ['rt_energy', 'dam_energy', 'spin', 'non_spin', 'reg_up', 'reg_down', 'ecrs']])
        if total > 0:  # Only include resources with revenue
            active_resources += 1
            total_revenue += total
            results.append({
                'BESS_Asset_Name': name,
                'Year': year,
                'RT_Revenue': rev['rt_energy'],
                'DA_Revenue': rev['dam_energy'],
                'Spin_Revenue': rev['spin'],
                'NonSpin_Revenue': rev['non_spin'],
                'RegUp_Revenue': rev['reg_up'],
                'RegDown_Revenue': rev['reg_down'],
                'ECRS_Revenue': rev['ecrs'],
                'Total_Revenue': total
            })
    
    logger.info(f"  Active BESS resources: {active_resources}")
    logger.info(f"  Total revenue: ${total_revenue:,.2f}")
    
    return results

# Get available years
logger.info("\n🔍 Finding available years...")
years = set()
for file in DAM_DIR.glob("*DAM_Gen_Resource_Data*.csv"):
    # Extract year from filename
    parts = file.stem.split('-')
    if len(parts) >= 3:
        year_str = parts[-1]
        try:
            year_val = int(year_str)
            if year_val < 50:
                years.add(2000 + year_val)
            else:
                years.add(1900 + year_val)
        except:
            pass

years = sorted(list(years))
logger.info(f"Years available: {years}")

# Process each year
all_results = []
for year in years:
    year_results = process_year(year)
    all_results.extend(year_results)
    logger.info(f"\nCompleted year {year}: {len(year_results)} BESS resources with revenue")

# Create final dataframe
logger.info("\n💾 Saving results...")
if all_results:
    results_df = pd.DataFrame(all_results)
    
    # Save to CSV
    csv_path = OUTPUT_DIR / "bess_annual_revenues_complete.csv"
    results_df.to_csv(csv_path, index=False)
    logger.info(f"Saved CSV: {csv_path}")
    
    # Save to Parquet
    parquet_path = OUTPUT_DIR / "bess_annual_revenues_complete.parquet"
    results_df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved Parquet: {parquet_path}")
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info("📊 BESS Revenue Summary by Year")
    logger.info("="*80)
    logger.info(f"{'Year':<6} {'Resources':<12} {'Total($M)':<15} {'RT($M)':<12} {'DAM($M)':<12} {'AS($M)':<12}")
    logger.info("-"*80)
    
    for year in sorted(results_df['Year'].unique()):
        year_data = results_df[results_df['Year'] == year]
        total_rev = year_data['Total_Revenue'].sum() / 1e6
        rt_rev = year_data['RT_Revenue'].sum() / 1e6
        dam_rev = year_data['DA_Revenue'].sum() / 1e6
        as_rev = (year_data['Spin_Revenue'].sum() + year_data['NonSpin_Revenue'].sum() + 
                  year_data['RegUp_Revenue'].sum() + year_data['RegDown_Revenue'].sum() + 
                  year_data['ECRS_Revenue'].sum()) / 1e6
        n_resources = len(year_data)
        
        logger.info(f"{year:<6} {n_resources:<12} {total_rev:<15.2f} {rt_rev:<12.2f} {dam_rev:<12.2f} {as_rev:<12.2f}")
    
    # Show top revenue generators
    logger.info("\n📈 Top 10 BESS by Total Revenue (All Years)")
    logger.info("-"*80)
    top_bess = results_df.groupby('BESS_Asset_Name')['Total_Revenue'].sum().sort_values(ascending=False).head(10)
    for i, (name, revenue) in enumerate(top_bess.items(), 1):
        logger.info(f"{i:2d}. {name:<30} ${revenue:>15,.2f}")
    
else:
    logger.error("❌ No results generated")

logger.info("\n✅ Analysis complete!")
logger.info(f"Check the log file for details: {LOG_FILE}")