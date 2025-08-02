use anyhow::Result;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::io::BufRead;
use std::sync::Arc;

pub struct AnnualProcessor {
    base_dir: PathBuf,
    output_dir: PathBuf,
}

impl AnnualProcessor {
    pub fn new(base_dir: PathBuf, output_dir: PathBuf) -> Self {
        Self { base_dir, output_dir }
    }
    
    pub fn process_all_extracted_data(&self) -> Result<()> {
        println!("🚀 Annual Data Processor for Extracted CSV Files");
        println!("Using {} CPU cores", rayon::current_num_threads());
        println!("{}", "=".repeat(80));
        
        // Read directories from CSV file
        let csv_file = "ercot_directories.csv";
        if !Path::new(csv_file).exists() {
            return Err(anyhow::anyhow!("File {} not found", csv_file));
        }

        let file = fs::File::open(csv_file)?;
        let reader = std::io::BufReader::new(file);
        
        let mut directories = Vec::new();
        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;
            let line = line.trim();
            
            // Skip header line and empty lines
            if line_num == 0 || line.is_empty() || line == "directory_name" {
                continue;
            }
            
            directories.push(line.to_string());
        }

        println!("Found {} directories to process", directories.len());
        
        // Create output directory
        fs::create_dir_all(&self.output_dir)?;
        
        // Process each directory
        for (idx, dir_name) in directories.iter().enumerate() {
            let csv_dir = self.base_dir.join(dir_name).join("csv");
            
            if !csv_dir.exists() {
                println!("⚠️  CSV directory not found: {}", csv_dir.display());
                continue;
            }
            
            println!("\n🔄 [{}/{}] Processing: {}", idx + 1, directories.len(), dir_name);
            self.process_directory(&csv_dir, dir_name)?;
        }
        
        println!("\n✅ Annual processing complete!");
        Ok(())
    }
    
    fn process_directory(&self, csv_dir: &Path, dir_name: &str) -> Result<()> {
        // Find all CSV files
        let csv_files: Vec<PathBuf> = fs::read_dir(csv_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension()?.to_str()? == "csv" {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        
        if csv_files.is_empty() {
            println!("  ⚠️  No CSV files found in {}", csv_dir.display());
            return Ok(());
        }
        
        println!("  📊 Found {} CSV files", csv_files.len());
        
        // Special handling for DAM_Hourly_LMPs which contains two different file types
        if dir_name == "DAM_Hourly_LMPs" {
            println!("  📝 Special handling for DAM_Hourly_LMPs - separating file types");
            
            // Separate DAMHRLMPNP4183 (LMP) and DAMSPNP4190 (Settlement Point Price) files
            let lmp_files: Vec<PathBuf> = csv_files.iter()
                .filter(|f| f.to_str().unwrap_or("").contains("DAMHRLMPNP4183"))
                .cloned()
                .collect();
            
            let spp_files: Vec<PathBuf> = csv_files.iter()
                .filter(|f| f.to_str().unwrap_or("").contains("DAMSPNP4190"))
                .cloned()
                .collect();
            
            println!("  📁 Found {} DAMHRLMPNP4183 (LMP) files", lmp_files.len());
            println!("  📁 Found {} DAMSPNP4190 (Settlement Point Price) files", spp_files.len());
            
            // Process LMP files
            if !lmp_files.is_empty() {
                println!("\n  Processing DAMHRLMPNP4183 (LMP) files...");
                self.process_file_group(&lmp_files, "DAM_Hourly_LMPs_BusLevel")?;
            }
            
            // Process Settlement Point Price files
            if !spp_files.is_empty() {
                println!("\n  Processing DAMSPNP4190 (Settlement Point Price) files...");
                self.process_file_group(&spp_files, "DAM_Settlement_Point_Prices_Hourly")?;
            }
        } else {
            // Normal processing for other directories
            self.process_file_group(&csv_files, dir_name)?;
        }
        
        Ok(())
    }
    
    fn process_file_group(&self, csv_files: &[PathBuf], output_name: &str) -> Result<()> {
        // Group files by year
        let mut files_by_year: HashMap<i32, Vec<PathBuf>> = HashMap::new();
        
        for csv_file in csv_files {
            if let Some(year) = self.extract_year_from_filename(csv_file) {
                files_by_year.entry(year).or_insert_with(Vec::new).push(csv_file.clone());
            }
        }
        
        if files_by_year.is_empty() {
            println!("  ⚠️  No files with recognizable year patterns");
            return Ok(());
        }
        
        let mut years: Vec<i32> = files_by_year.keys().cloned().collect();
        years.sort();
        
        println!("  📅 Years found: {:?}", years);
        
        // Process each year
        for year in years {
            let year_files = &files_by_year[&year];
            println!("  🔄 Processing year {}: {} files", year, year_files.len());
            
            match self.process_year_files(year, year_files, output_name) {
                Ok(()) => println!("  ✅ Completed year {}", year),
                Err(e) => println!("  ❌ Failed year {}: {}", year, e),
            }
        }
        
        Ok(())
    }
    
    fn extract_year_from_filename(&self, file_path: &Path) -> Option<i32> {
        let filename = file_path.file_name()?.to_str()?;
        
        // Look for patterns like YYYYMMDD or _YYYY_ or .YYYY.
        for (start, _part) in filename.match_indices(char::is_numeric) {
            if let Some(year_str) = filename.get(start..start + 4) {
                if let Ok(year) = year_str.parse::<i32>() {
                    if year >= 2009 && year <= 2025 {
                        return Some(year);
                    }
                }
            }
        }
        
        None
    }
    
    fn normalize_dataframe(&self, df: LazyFrame, target_schema: &HashSet<String>) -> LazyFrame {
        // Get current columns
        let df_sample = df.clone().limit(1).collect().unwrap();
        let current_cols: HashSet<String> = df_sample.get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        
        let mut result = df;
        
        // Add missing columns with null values
        for col in target_schema {
            if !current_cols.contains(col) {
                // Add missing column as null with appropriate type
                if col.to_lowercase().contains("flag") || col == "DSTFlag" {
                    // String type for flag columns
                    result = result.with_column(lit(NULL).cast(DataType::Utf8).alias(col));
                } else if col.to_lowercase().contains("price") || 
                          col.to_lowercase().contains("lmp") || 
                          col.to_lowercase().contains("mcpc") {
                    // Float type for price columns
                    result = result.with_column(lit(NULL).cast(DataType::Float64).alias(col));
                } else {
                    // Default to string
                    result = result.with_column(lit(NULL).cast(DataType::Utf8).alias(col));
                }
            }
        }
        
        // Select only the columns we want, in consistent order
        let mut cols_to_select: Vec<String> = target_schema.iter().cloned().collect();
        cols_to_select.sort();
        
        let select_exprs: Vec<Expr> = cols_to_select
            .iter()
            .map(|c| col(c))
            .collect();
        
        result.select(&select_exprs)
    }
    
    fn process_year_files(&self, year: i32, files: &[PathBuf], dir_name: &str) -> Result<()> {
        // First pass: determine all columns across all files
        println!("    🔍 Analyzing schema across all files...");
        let mut all_columns = HashSet::new();
        let mut sample_count = 0;
        
        for (i, file) in files.iter().enumerate() {
            if i % 50 == 0 {  // Sample every 50th file
                if let Ok(df) = CsvReader::new(std::fs::File::open(file)?)
                    .has_header(true)
                    .finish() {
                    for col in df.get_column_names() {
                        all_columns.insert(col.to_string());
                    }
                    sample_count += 1;
                }
            }
        }
        
        println!("    📋 Found {} unique columns across {} sampled files", all_columns.len(), sample_count);
        
        // Read and combine all CSV files for the year
        let mut all_dataframes = Vec::new();
        
        // Process files in batches to avoid memory issues
        let batch_size = 50;
        let total_batches = (files.len() + batch_size - 1) / batch_size;
        for (batch_idx, batch) in files.chunks(batch_size).enumerate() {
            // Only log every 10th batch or first/last batch
            if batch_idx == 0 || batch_idx == total_batches - 1 || batch_idx % 10 == 0 {
                println!("    🔄 Processing batch {}/{}", batch_idx + 1, total_batches);
            }
            
            let batch_dfs: Vec<LazyFrame> = batch
                .par_iter()
                .filter_map(|file| {
                    // Define schema overrides for price columns
                    let mut schema_overrides = Schema::new();
                    
                    // Force all price-related columns to Float64
                    let price_columns = vec![
                        "SettlementPointPrice", "LMP", "Price", "ShadowPrice",
                        "MCPCValue", "MCPC", "EnergyPrice", "CongestionPrice", 
                        "LossPrice", "EnergyComponent", "CongestionComponent", 
                        "LossComponent", "RegUp MCPC", "RegDown MCPC", 
                        "RRS MCPC", "NonSpin MCPC", "ECRS MCPC"
                    ];
                    
                    for col in price_columns {
                        schema_overrides.with_column(col.to_string().into(), DataType::Float64);
                    }
                    
                    // Read CSV with schema overrides
                    match CsvReader::new(std::fs::File::open(file).ok()?)
                        .has_header(true)
                        .infer_schema(Some(50000))  // Much larger schema inference
                        .with_dtypes(Some(Arc::new(schema_overrides)))
                        .finish() {
                        Ok(df) => {
                            let cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
                            let mut lazy_df = df.lazy();
                            
                            // Standardize column names: BusName -> SettlementPoint
                            if cols.contains(&"BusName".to_string()) && !cols.contains(&"SettlementPoint".to_string()) {
                                lazy_df = lazy_df.with_column(
                                    col("BusName").alias("SettlementPoint")
                                );
                            }
                            
                            // Double-check and force cast any column that might be a price
                            // This catches any columns that weren't in our predefined list
                            for col_name in &cols {
                                if col_name.to_lowercase().contains("price") || 
                                   col_name.to_lowercase().contains("mcpc") || 
                                   col_name.to_lowercase().contains("lmp") || 
                                   col_name.to_lowercase().contains("component") ||
                                   col_name.to_lowercase().contains("shadow") ||
                                   col_name.to_lowercase().contains("energy") ||
                                   col_name.to_lowercase().contains("congestion") ||
                                   col_name.to_lowercase().contains("loss") {
                                    lazy_df = lazy_df.with_column(
                                        col(col_name).cast(DataType::Float64)
                                    );
                                }
                            }
                            
                            // Normalize the dataframe to have all expected columns
                            lazy_df = self.normalize_dataframe(lazy_df, &all_columns);
                            
                            Some(lazy_df)
                        },
                        Err(e) => {
                            eprintln!("    ⚠️  Failed to read {}: {}", file.display(), e);
                            None
                        }
                    }
                })
                .collect();
            
            if !batch_dfs.is_empty() {
                all_dataframes.extend(batch_dfs);
            }
        }
        
        if all_dataframes.is_empty() {
            println!("    ⚠️  No valid dataframes for year {}", year);
            return Ok(());
        }
        
        println!("    📊 Combining {} dataframes", all_dataframes.len());
        
        // Concatenate all dataframes with relaxed column matching
        let combined = concat(
            all_dataframes,
            UnionArgs {
                parallel: true,
                rechunk: true,
                to_supertypes: true,
                ..Default::default()
            },
        )?;
        
        // Create datetime column if needed and sort
        let processed = self.process_datetime_columns(combined)?;
        
        // Remove duplicates and sort - find the best column to sort by
        let sort_column = self.find_sort_column(&processed)?;
        
        let final_df = processed
            .unique(None, UniqueKeepStrategy::First)
            .sort(&sort_column, Default::default())
            .collect()?;
        
        println!("    📊 Final record count: {}", final_df.height());
        
        // Save in multiple formats
        let safe_dir_name = dir_name.replace(",", "_").replace(" ", "_");
        let base_filename = format!("{}_{}", safe_dir_name, year);
        
        // Create output directory for this dataset
        let dataset_output_dir = self.output_dir.join(&safe_dir_name);
        fs::create_dir_all(&dataset_output_dir)?;
        
        // Skip CSV for large datasets to save disk space
        // CSV files can be 20-50x larger than Parquet
        let skip_csv = std::env::var("SKIP_CSV").unwrap_or_default() == "1" || 
                       final_df.height() > 10_000_000;  // Skip CSV for datasets > 10M rows
        
        if !skip_csv {
            // CSV
            let csv_path = dataset_output_dir.join(format!("{}.csv", base_filename));
            println!("    💾 Saving CSV: {}", csv_path.display());
            CsvWriter::new(fs::File::create(&csv_path)?)
                .finish(&mut final_df.clone())?;
        } else {
            println!("    ⏭️  Skipping CSV output (dataset too large: {} rows)", final_df.height());
        }
        
        // Parquet - ALWAYS save this as it's highly compressed
        let parquet_path = dataset_output_dir.join(format!("{}.parquet", base_filename));
        println!("    📦 Saving Parquet: {}", parquet_path.display());
        ParquetWriter::new(fs::File::create(&parquet_path)?)
            .finish(&mut final_df.clone())?;
        
        // Arrow IPC - Optional, controlled by environment variable
        if std::env::var("SAVE_ARROW").unwrap_or_default() == "1" {
            let arrow_path = dataset_output_dir.join(format!("{}.arrow", base_filename));
            println!("    🏹 Saving Arrow: {}", arrow_path.display());
            IpcWriter::new(fs::File::create(&arrow_path)?)
                .finish(&mut final_df.clone())?;
        }
        
        Ok(())
    }
    
    fn find_sort_column(&self, df: &LazyFrame) -> Result<String> {
        let df_collected = df.clone().limit(1).collect()?;
        let columns: Vec<String> = df_collected.get_column_names().iter().map(|s| s.to_string()).collect();
        
        // Priority order for sorting columns
        let sort_priorities = vec![
            "datetime", "DeliveryDate", "Date", "timestamp", "Time", 
            "DeliveryHour", "Hour", "Interval"
        ];
        
        for priority_col in sort_priorities {
            if columns.contains(&priority_col.to_string()) {
                return Ok(priority_col.to_string());
            }
        }
        
        // If no preferred columns found, use the first column
        Ok(columns.first().unwrap_or(&"column_0".to_string()).clone())
    }
    
    fn process_datetime_columns(&self, df: LazyFrame) -> Result<LazyFrame> {
        // Try to create a datetime column from available date/time columns
        let df_collected = df.clone().limit(1).collect()?;
        let columns: Vec<String> = df_collected.get_column_names().iter().map(|s| s.to_string()).collect();
        
        // Look for common datetime patterns
        if columns.contains(&"datetime".to_string()) {
            // Already has datetime column
            return Ok(df);
        }
        
        // Try to create datetime from DeliveryDate + DeliveryHour + DeliveryInterval
        if columns.contains(&"DeliveryDate".to_string()) && columns.contains(&"DeliveryHour".to_string()) {
            return Ok(df.with_columns([
                // Create a simple datetime approximation
                col("DeliveryDate").alias("datetime")
            ]));
        }
        
        // If no date columns found, use the dataframe as-is
        Ok(df)
    }
}

pub fn process_all_annual_data() -> Result<()> {
    let base_dir = PathBuf::from("/Users/enrico/data/ERCOT_data");
    let output_dir = PathBuf::from("annual_output");
    
    let processor = AnnualProcessor::new(base_dir, output_dir);
    processor.process_all_extracted_data()
}