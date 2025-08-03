#!/usr/bin/env python3
import sys
try:
    import pandas as pd
    import pyarrow.parquet as pq
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "/Users/enrico/data/ERCOT_data/processed/spp/annual/2024/spp_all_2024.parquet"
    
    # Read schema
    schema = pq.read_schema(file_path)
    print(f"Schema for {file_path}:")
    print("=" * 60)
    for field in schema:
        print(f"{field.name}: {field.type}")
    
    # Read first few rows
    df = pd.read_parquet(file_path, nrows=5)
    print(f"\nFirst 5 rows:")
    print("=" * 60)
    print(df)
    
    print(f"\nShape: {pq.read_metadata(file_path).num_rows} rows")
    
except ImportError:
    print("Please install pandas and pyarrow: pip install pandas pyarrow")
except Exception as e:
    print(f"Error: {e}")