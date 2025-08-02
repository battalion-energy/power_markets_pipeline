import pandas as pd
import glob
import os

def inspect_disclosure_folder(folder_path, folder_name):
    print(f"\n{'='*80}")
    print(f"📁 {folder_name}")
    print(f"{'='*80}")
    
    # Find CSV files
    csv_files = glob.glob(os.path.join(folder_path, "csv", "*.csv"))
    
    if not csv_files:
        print("No CSV files found")
        return
    
    # Get unique file patterns
    file_patterns = {}
    for f in csv_files:
        base = os.path.basename(f)
        # Extract pattern by removing date
        pattern = '-'.join(base.split('-')[:-3]) + '.csv'
        if pattern not in file_patterns:
            file_patterns[pattern] = f
    
    print(f"Found {len(csv_files)} CSV files")
    print(f"Unique file types: {len(file_patterns)}")
    
    # Inspect each type
    for pattern, sample_file in list(file_patterns.items())[:10]:  # Show more types
        print(f"\n📄 {pattern}")
        try:
            df = pd.read_csv(sample_file, nrows=1000)
            print(f"  Columns ({len(df.columns)}): {list(df.columns)[:10]}...")
            print(f"  Rows in sample: {len(df)}")
            
            # Check for BESS data
            if 'Resource Type' in df.columns:
                bess_count = len(df[df['Resource Type'] == 'PWRSTR'])
                if bess_count > 0:
                    print(f"  ⚡ BESS records found: {bess_count}")
                    
                    # Show sample BESS resource names
                    bess_names = df[df['Resource Type'] == 'PWRSTR']['Resource Name'].unique()[:5]
                    print(f"  Sample BESS: {list(bess_names)}")
                    
        except Exception as e:
            print(f"  Error reading file: {e}")

# Inspect each disclosure folder
folders = [
    ("/Users/enrico/data/ERCOT_data/60-Day_COP_Adjustment_Period_Snapshot", "COP Adjustment Period Snapshot"),
    ("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports", "DAM Disclosure Reports"),
    ("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports", "SCED Disclosure Reports"),
    ("/Users/enrico/data/ERCOT_data/60-Day_SASM_Disclosure_Reports", "SASM Disclosure Reports"),
    ("/Users/enrico/data/ERCOT_data/60-Day_COP_All_Updates", "COP All Updates")
]

for folder_path, folder_name in folders:
    if os.path.exists(folder_path):
        inspect_disclosure_folder(folder_path, folder_name)
    else:
        print(f"\n❌ Folder not found: {folder_path}")

# Now show specific analysis for revenue calculation
print("\n" + "="*80)
print("📊 BESS Revenue Data Sources")
print("="*80)

# Check DAM Gen Resource Data
dam_files = glob.glob("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv/*Gen_Resource_Data*.csv")
if dam_files:
    print(f"\nDAM Gen Resource Data: {len(dam_files)} files")
    df = pd.read_csv(dam_files[-1])  # Most recent
    bess_df = df[df['Resource Type'] == 'PWRSTR']
    
    print(f"Latest file: {os.path.basename(dam_files[-1])}")
    print(f"BESS resources in file: {len(bess_df['Resource Name'].unique())}")
    print(f"Total BESS records: {len(bess_df)}")
    
    # Check for AS awards
    as_cols = ['RegUp Awarded', 'RegDown Awarded', 'RRS MCPC', 'ECRSSD Awarded', 'NonSpin Awarded']
    available_as = [col for col in as_cols if col in df.columns]
    print(f"Available AS columns: {available_as}")

# Check SCED Gen Resource Data  
sced_files = glob.glob("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv/*Gen_Resource_Data*.csv")
if sced_files:
    print(f"\nSCED Gen Resource Data: {len(sced_files)} files")
    df = pd.read_csv(sced_files[-1], nrows=10000)  # Sample only
    if 'Resource Type' in df.columns:
        bess_df = df[df['Resource Type'] == 'PWRSTR']
        print(f"Latest file: {os.path.basename(sced_files[-1])}")
        print(f"BESS dispatch records in sample: {len(bess_df)}")