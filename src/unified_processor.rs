use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, Datelike, Duration};
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use ::zip::ZipArchive;

pub struct UnifiedDataProcessor {
    base_dir: PathBuf,
    output_dir: PathBuf,
    column_history: Arc<Mutex<HashMap<String, HashSet<String>>>>,
}

impl UnifiedDataProcessor {
    pub fn new(base_dir: PathBuf, output_dir: PathBuf) -> Self {
        Self { 
            base_dir, 
            output_dir,
            column_history: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    pub fn process_all_data(&self) -> Result<()> {
        println!("🚀 ERCOT Unified Data Processor");
        println!("Using {} CPU cores", rayon::current_num_threads());
        println!("{}", "=".repeat(80));
        
        // Step 1: Recursively unzip all files
        println!("\n📦 Step 1: Extracting all ZIP files recursively...");
        self.recursive_unzip_all()?;
        
        // Step 2: Process CSV files by year
        println!("\n📅 Step 2: Processing CSV files by year...");
        self.process_csv_by_year()?;
        
        // Step 3: Report column changes over time
        println!("\n📊 Step 3: Column evolution report...");
        self.report_column_changes();
        
        Ok(())
    }
    
    fn recursive_unzip_all(&self) -> Result<()> {
        // Find all directories to process
        let dirs_to_process = vec![
            "Settlement_Point_Prices_at_Resource_Nodes,_Hubs_and_Load_Zones",
            "LMPs by Resource Nodes, Load Zones and Trading Hubs",
            "DAM_Settlement_Point_Prices",
            "DAM_Hourly_LMPs",
            "DAM_Clearing_Prices_for_Capacity",
            "SCED_Shadow_Prices_and_Binding_Transmission_Constraints",
            "DAM_Shadow_Prices",
        ];
        
        for dir_name in dirs_to_process {
            let source_dir = self.base_dir.join(dir_name);
            if !source_dir.exists() {
                println!("  ⚠️  Directory not found: {}", dir_name);
                continue;
            }
            
            let unzipped_dir = source_dir.join("unzipped");
            fs::create_dir_all(&unzipped_dir)?;
            
            println!("\n  📁 Processing: {}", dir_name);
            self.recursive_unzip(&source_dir, &unzipped_dir)?;
        }
        
        Ok(())
    }
    
    fn recursive_unzip(&self, source_dir: &Path, unzipped_dir: &Path) -> Result<()> {
        // Find all ZIP files in the source directory
        let pattern = source_dir.join("*.zip");
        let zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        if zip_files.is_empty() {
            println!("    No ZIP files found in {}", source_dir.display());
            return Ok(());
        }
        
        println!("    Found {} ZIP files", zip_files.len());
        
        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} Extracting")
            .unwrap());
        
        // Process ZIP files in parallel
        let nested_zips = Arc::new(Mutex::new(Vec::new()));
        
        zip_files.par_iter().for_each(|zip_path| {
            pb.inc(1);
            
            if let Ok(file) = fs::File::open(zip_path) {
                if let Ok(mut archive) = ZipArchive::new(file) {
                    for i in 0..archive.len() {
                        if let Ok(mut file) = archive.by_index(i) {
                            let file_name = file.name().to_string();
                            
                            if file_name.ends_with(".zip") {
                                // Nested ZIP - save path for later processing
                                let nested_path = unzipped_dir.join(&file_name);
                                if let Some(parent) = nested_path.parent() {
                                    let _ = fs::create_dir_all(parent);
                                }
                                
                                if let Ok(mut out_file) = fs::File::create(&nested_path) {
                                    let mut buffer = Vec::new();
                                    if file.read_to_end(&mut buffer).is_ok() {
                                        let _ = out_file.write_all(&buffer);
                                        nested_zips.lock().unwrap().push(nested_path);
                                    }
                                }
                            } else if file_name.ends_with(".csv") {
                                // CSV file - extract directly
                                let out_path = unzipped_dir.join(&file_name);
                                if let Some(parent) = out_path.parent() {
                                    let _ = fs::create_dir_all(parent);
                                }
                                
                                if let Ok(mut out_file) = fs::File::create(&out_path) {
                                    let mut buffer = Vec::new();
                                    if file.read_to_end(&mut buffer).is_ok() {
                                        let _ = out_file.write_all(&buffer);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        
        pb.finish_with_message("Initial extraction complete");
        
        // Process nested ZIPs
        let nested = match Arc::try_unwrap(nested_zips) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        };
        
        if !nested.is_empty() {
            println!("    Found {} nested ZIP files, extracting...", nested.len());
            
            let pb_nested = ProgressBar::new(nested.len() as u64);
            pb_nested.set_style(ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} Nested ZIPs")
                .unwrap());
            
            nested.par_iter().for_each(|zip_path| {
                pb_nested.inc(1);
                
                if let Ok(file) = fs::File::open(zip_path) {
                    if let Ok(mut archive) = ZipArchive::new(file) {
                        for i in 0..archive.len() {
                            if let Ok(mut file) = archive.by_index(i) {
                                if file.name().ends_with(".csv") {
                                    let out_path = zip_path.with_extension("").join(file.name());
                                    if let Some(parent) = out_path.parent() {
                                        let _ = fs::create_dir_all(parent);
                                    }
                                    
                                    if let Ok(mut out_file) = fs::File::create(&out_path) {
                                        let mut buffer = Vec::new();
                                        if file.read_to_end(&mut buffer).is_ok() {
                                            let _ = out_file.write_all(&buffer);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Remove the nested ZIP after extraction
                    let _ = fs::remove_file(zip_path);
                }
            });
            
            pb_nested.finish_with_message("Nested extraction complete");
        }
        
        Ok(())
    }
    
    fn process_csv_by_year(&self) -> Result<()> {
        // Process each data type
        let datasets = vec![
            ("Settlement_Point_Prices_at_Resource_Nodes,_Hubs_and_Load_Zones", "RT_Settlement_Point_Prices"),
            ("LMPs by Resource Nodes, Load Zones and Trading Hubs", "RT_LMPs"),
            ("DAM_Settlement_Point_Prices", "DAM_Settlement_Point_Prices"),
            ("DAM_Hourly_LMPs", "DAM_Hourly_LMPs"),
            ("DAM_Clearing_Prices_for_Capacity", "DAM_Ancillary_Services"),
            ("SCED_Shadow_Prices_and_Binding_Transmission_Constraints", "SCED_Shadow_Prices"),
            ("DAM_Shadow_Prices", "DAM_Shadow_Prices"),
        ];
        
        for (dir_name, output_prefix) in datasets {
            let unzipped_dir = self.base_dir.join(dir_name).join("unzipped");
            if !unzipped_dir.exists() {
                continue;
            }
            
            println!("\n📊 Processing dataset: {}", output_prefix);
            
            // Find all CSV files
            let pattern = unzipped_dir.join("**/*.csv");
            let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
                .filter_map(Result::ok)
                .collect();
            
            if csv_files.is_empty() {
                println!("  No CSV files found");
                continue;
            }
            
            println!("  Found {} CSV files", csv_files.len());
            
            // Group files by year
            let files_by_year = self.group_files_by_year(&csv_files)?;
            
            // Process each year
            for (year, files) in files_by_year {
                if files.is_empty() {
                    continue;
                }
                
                println!("\n  📅 Processing year {}: {} files", year, files.len());
                self.process_year_data(year, &files, output_prefix)?;
            }
        }
        
        Ok(())
    }
    
    fn group_files_by_year(&self, files: &[PathBuf]) -> Result<HashMap<i32, Vec<PathBuf>>> {
        let mut files_by_year: HashMap<i32, Vec<PathBuf>> = HashMap::new();
        
        for file_path in files {
            // Extract year from filename
            let filename = file_path.file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("");
            
            // Try different patterns to extract year
            let year = self.extract_year_from_filename(filename)
                .or_else(|| self.extract_year_from_csv_content(file_path).ok().flatten());
            
            if let Some(y) = year {
                if y >= 2010 && y <= 2030 { // Sanity check
                    files_by_year.entry(y).or_insert_with(Vec::new).push(file_path.clone());
                }
            }
        }
        
        Ok(files_by_year)
    }
    
    fn extract_year_from_filename(&self, filename: &str) -> Option<i32> {
        // Try patterns like .20240823. or _2024_
        let patterns = vec![
            r"\.20(\d{2})\d{4}\.",  // .YYYYMMDD.
            r"_20(\d{2})_",         // _YYYY_
            r"_20(\d{2})\.",        // _YYYY.
            r"\b20(\d{2})\b",       // standalone YYYY
        ];
        
        for pattern in patterns {
            if let Ok(re) = regex::Regex::new(pattern) {
                if let Some(caps) = re.captures(filename) {
                    if let Some(year_suffix) = caps.get(1) {
                        if let Ok(suffix) = year_suffix.as_str().parse::<i32>() {
                            return Some(2000 + suffix);
                        }
                    }
                }
            }
        }
        
        None
    }
    
    fn extract_year_from_csv_content(&self, file_path: &Path) -> Result<Option<i32>> {
        // Read first few lines to determine year
        let df = CsvReader::new(fs::File::open(file_path)?)
            .has_header(true)
            .with_n_rows(Some(10))
            .finish()?;
        
        // Try to find date columns - expanded list for different data types
        let date_columns = vec![
            "DeliveryDate", 
            "SCEDTimestamp", 
            "Date", 
            "OperatingDate",
            "TradeDate",
            "Interval",
            "SCED_TIMESTAMP"
        ];
        
        for col_name in date_columns {
            if let Ok(col) = df.column(col_name) {
                if let Ok(str_col) = col.utf8() {
                    if let Some(first_val) = str_col.get(0) {
                        // Try parsing different date formats
                        if let Ok(date) = NaiveDate::parse_from_str(first_val, "%m/%d/%Y") {
                            return Ok(Some(date.year()));
                        }
                        if let Ok(datetime) = NaiveDateTime::parse_from_str(first_val, "%m/%d/%Y %H:%M:%S") {
                            return Ok(Some(datetime.year()));
                        }
                        // Try SCED timestamp format with AM/PM
                        if let Ok(datetime) = NaiveDateTime::parse_from_str(first_val, "%m/%d/%Y %I:%M:%S %p") {
                            return Ok(Some(datetime.year()));
                        }
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    fn process_year_data(&self, year: i32, files: &[PathBuf], output_prefix: &str) -> Result<()> {
        let output_dir = self.output_dir.join(format!("{}_{}", output_prefix, year));
        fs::create_dir_all(&output_dir)?;
        
        // Process files in batches to manage memory
        let batch_size = 100; // Process 100 files at a time for better memory management
        let total_batches = (files.len() + batch_size - 1) / batch_size;
        
        println!("    Total files: {}, Batch size: {}, Total batches: {}", 
                 files.len(), batch_size, total_batches);
        
        let mut all_batch_results = Vec::new();
        
        for (batch_idx, batch) in files.chunks(batch_size).enumerate() {
            println!("    Processing batch {}/{} ({} files)...", 
                     batch_idx + 1, total_batches, batch.len());
            
            let batch_df = self.process_batch(batch, year)?;
            if let Some(df) = batch_df {
                all_batch_results.push(df);
            }
        }
        
        if all_batch_results.is_empty() {
            println!("    ⚠️  No valid data found for year {}", year);
            return Ok(());
        }
        
        // Combine all batches
        println!("    📦 Combining {} batches...", all_batch_results.len());
        let combined_df = self.combine_and_deduplicate(all_batch_results)?;
        
        // Save annual files
        self.save_annual_files(&combined_df, &output_dir, output_prefix, year)?;
        
        Ok(())
    }
    
    fn process_batch(&self, files: &[PathBuf], year: i32) -> Result<Option<DataFrame>> {
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        let column_history = self.column_history.clone();
        let dfs: Vec<DataFrame> = files.par_iter()
            .filter_map(|file_path| {
                pb.inc(1);
                
                // Read CSV file
                let mut df = CsvReader::new(fs::File::open(file_path).ok()?)
                    .has_header(true)
                    .finish()
                    .ok()?;
                
                // Standardize column names for consistency across different datasets
                let column_mappings = vec![
                    ("BusName", "SettlementPoint"),
                    ("Bus Name", "SettlementPoint"),
                    ("Bus", "SettlementPoint"),
                    ("ResourceName", "SettlementPoint"),
                    ("Resource Name", "SettlementPoint"),
                    ("HourEnding", "DeliveryHour"),
                    ("Hour Ending", "DeliveryHour"),
                    ("LMP", "SettlementPointPrice"),
                    ("Price", "SettlementPointPrice"),
                ];
                
                for (old_name, new_name) in column_mappings {
                    if df.get_column_names().contains(&old_name) && !df.get_column_names().contains(&new_name) {
                        // Rename by selecting all columns with the new name
                        let cols = df.get_column_names();
                        let new_cols: Vec<_> = cols.iter()
                            .map(|&c| {
                                if c == old_name {
                                    col(c).alias(new_name)
                                } else {
                                    col(c)
                                }
                            })
                            .collect();
                        
                        if let Ok(renamed_df) = df.clone().lazy().select(&new_cols).collect() {
                            df = renamed_df;
                        }
                    }
                }
                
                // Ensure consistent data types for common columns
                let type_mappings = vec![
                    // Price columns should be float64
                    ("SettlementPointPrice", DataType::Float64),
                    ("LMP", DataType::Float64),
                    ("Price", DataType::Float64),
                    ("ShadowPrice", DataType::Float64),
                    ("MCPCValue", DataType::Float64),
                    ("EnergyPrice", DataType::Float64),
                    ("CongestionPrice", DataType::Float64),
                    ("LossPrice", DataType::Float64),
                    // String columns
                    ("DSTFlag", DataType::Utf8),
                    ("SettlementPointName", DataType::Utf8),
                    ("SettlementPointType", DataType::Utf8),
                    ("SettlementPoint", DataType::Utf8),
                    ("BusName", DataType::Utf8),
                    ("ResourceName", DataType::Utf8),
                    ("Resource Name", DataType::Utf8),
                    // Integer columns
                    ("DeliveryHour", DataType::Int32),
                    ("HourEnding", DataType::Int32),
                    ("DeliveryInterval", DataType::Int32),
                ];
                
                for (col_name, target_type) in type_mappings {
                    if df.get_column_names().contains(&col_name) {
                        if let Ok(col) = df.column(col_name) {
                            // Cast to target type if different
                            if col.dtype() != &target_type {
                                if let Ok(cast_col) = col.cast(&target_type) {
                                    // with_column modifies the dataframe in place
                                    let _ = df.with_column(cast_col);
                                }
                            }
                        }
                    }
                }
                
                // Track columns for this dataset type
                let cols = df.get_column_names();
                if cols.is_empty() {
                    return None;
                }
                
                // Extract dataset type from file path
                let dataset_type = if file_path.to_str().unwrap_or("").contains("Settlement_Point_Prices") {
                    "RT_Settlement_Point_Prices"
                } else if file_path.to_str().unwrap_or("").contains("LMPs") && file_path.to_str().unwrap_or("").contains("DAM") {
                    "DAM_LMPs"
                } else if file_path.to_str().unwrap_or("").contains("DAM_Settlement") {
                    "DAM_Settlement_Point_Prices"
                } else {
                    "Unknown"
                };
                
                // Track column changes
                if let Ok(mut history) = column_history.lock() {
                    let columns_set = history.entry(dataset_type.to_string()).or_insert_with(HashSet::new);
                    for col in &cols {
                        columns_set.insert(col.to_string());
                    }
                }
                
                // Filter by year if we can verify it
                if let Some(extracted_year) = self.verify_year(&df) {
                    if extracted_year != year {
                        return None;
                    }
                }
                
                Some(df)
            })
            .collect();
        
        pb.finish_and_clear();
        
        if dfs.is_empty() {
            return Ok(None);
        }
        
        // Combine dataframes - handle different schemas
        
        // Find all unique columns across all dataframes
        let mut all_columns = std::collections::HashSet::new();
        for df in &dfs {
            for col in df.get_column_names() {
                all_columns.insert(col.to_string());
            }
        }
        
        // Align all dataframes to have the same columns
        let aligned_dfs: Vec<DataFrame> = dfs.into_iter()
            .map(|mut df| {
                // Add missing columns as nulls
                for col in &all_columns {
                    if !df.get_column_names().contains(&col.as_str()) {
                        // Add null column of appropriate type based on column name
                        let null_series = if col == "DSTFlag" {
                            // DSTFlag should be string
                            Series::new(col, vec![None::<&str>; df.height()])
                        } else if col.contains("Price") || col.contains("LMP") || col.contains("MCPC") {
                            // Price columns should be float
                            Series::new(col, vec![None::<f64>; df.height()])
                        } else if col.contains("Hour") || col.contains("Interval") {
                            // Hour/Interval columns should be int
                            Series::new(col, vec![None::<i32>; df.height()])
                        } else {
                            // Default to string for other columns
                            Series::new(col, vec![None::<&str>; df.height()])
                        };
                        let _ = df.with_column(null_series);
                    }
                }
                df
            })
            .collect();
        
        // Now combine with aligned schemas
        let lazy_dfs: Vec<LazyFrame> = aligned_dfs.iter()
            .map(|df| df.clone().lazy())
            .collect();
        
        let combined = match concat(
            lazy_dfs.iter().map(|lf| lf.clone()).collect::<Vec<_>>().as_slice(),
            UnionArgs::default(),
        ) {
            Ok(lf) => lf,
            Err(e) => {
                println!("      ⚠️  Error concatenating dataframes: {}", e);
                println!("      Attempting to diagnose schema issues...");
                
                // Print column info for first few dataframes
                for (i, df) in aligned_dfs.iter().take(3).enumerate() {
                    println!("      DataFrame {}: {} columns", i, df.width());
                    for col in df.get_column_names() {
                        println!("        - {}", col);
                    }
                }
                
                return Err(anyhow::anyhow!("Failed to concatenate: {}", e));
            }
        };
        
        Ok(Some(combined.collect()?))
    }
    
    fn verify_year(&self, df: &DataFrame) -> Option<i32> {
        // Try to extract year from the dataframe content
        let date_columns = vec![
            "DeliveryDate", 
            "SCEDTimestamp", 
            "Date", 
            "OperatingDate",
            "TradeDate",
            "Interval",
            "SCED_TIMESTAMP"
        ];
        
        for col_name in date_columns {
            if let Ok(col) = df.column(col_name) {
                if let Ok(str_col) = col.utf8() {
                    if let Some(first_val) = str_col.get(0) {
                        if let Ok(date) = NaiveDate::parse_from_str(first_val, "%m/%d/%Y") {
                            return Some(date.year());
                        }
                        if let Ok(datetime) = NaiveDateTime::parse_from_str(first_val, "%m/%d/%Y %H:%M:%S") {
                            return Some(datetime.year());
                        }
                        // Try SCED timestamp format with AM/PM
                        if let Ok(datetime) = NaiveDateTime::parse_from_str(first_val, "%m/%d/%Y %I:%M:%S %p") {
                            return Some(datetime.year());
                        }
                    }
                }
            }
        }
        
        None
    }
    
    fn combine_and_deduplicate(&self, dfs: Vec<DataFrame>) -> Result<DataFrame> {
        println!("      🔄 Combining dataframes...");
        
        if dfs.is_empty() {
            return Err(anyhow::anyhow!("No dataframes to combine"));
        }
        
        // First, find all unique columns across all dataframes
        let mut all_columns = HashSet::new();
        for df in &dfs {
            for col in df.get_column_names() {
                all_columns.insert(col.to_string());
            }
        }
        
        // Ensure all price columns are float64 before concatenation
        let price_columns = vec!["SettlementPointPrice", "LMP", "Price", "ShadowPrice",
                                "MCPCValue", "EnergyPrice", "CongestionPrice", "LossPrice",
                                "Energy", "Congestion", "Loss"];
        
        let aligned_dfs: Vec<DataFrame> = dfs.into_iter()
            .map(|mut df| {
                // First add missing columns
                for col in &all_columns {
                    if !df.get_column_names().contains(&col.as_str()) {
                        // Add null column of appropriate type based on column name
                        let null_series = if col == "DSTFlag" {
                            Series::new(col, vec![None::<&str>; df.height()])
                        } else if col.contains("Price") || col.contains("LMP") || col.contains("MCPC") {
                            Series::new(col, vec![None::<f64>; df.height()])
                        } else if col.contains("Hour") || col.contains("Interval") {
                            Series::new(col, vec![None::<i32>; df.height()])
                        } else {
                            Series::new(col, vec![None::<&str>; df.height()])
                        };
                        let _ = df.with_column(null_series);
                    }
                }
                
                // Cast price columns to float64
                for price_col in &price_columns {
                    if df.get_column_names().contains(price_col) {
                        if let Ok(col) = df.column(price_col) {
                            if col.dtype() != &DataType::Float64 {
                                if let Ok(cast_col) = col.cast(&DataType::Float64) {
                                    let _ = df.with_column(cast_col);
                                }
                            }
                        }
                    }
                }
                
                // Also ensure DSTFlag is string
                if df.get_column_names().contains(&"DSTFlag") {
                    if let Ok(col) = df.column("DSTFlag") {
                        if col.dtype() != &DataType::Utf8 {
                            if let Ok(cast_col) = col.cast(&DataType::Utf8) {
                                let _ = df.with_column(cast_col);
                            }
                        }
                    }
                }
                
                df
            })
            .collect();
        
        // Combine all dataframes with relaxed concat
        let lazy_dfs: Vec<LazyFrame> = aligned_dfs.into_iter()
            .map(|df| df.lazy())
            .collect();
        
        let mut union_args = UnionArgs::default();
        union_args.parallel = true;
        
        let mut combined = concat(
            lazy_dfs.iter().map(|lf| lf.clone()).collect::<Vec<_>>().as_slice(),
            union_args,
        )?.collect()?;
        
        // Create datetime column if possible
        if let Ok(datetime_df) = self.create_datetime_column(&combined) {
            combined = datetime_df;
        }
        
        // Identify price columns (columns to exclude from deduplication)
        let price_columns: HashSet<&str> = vec![
            "SettlementPointPrice", "LMP", "Price", "ShadowPrice",
            "MCPCValue", "EnergyPrice", "CongestionPrice", "LossPrice",
            "Energy", "Congestion", "Loss"
        ].into_iter().collect();
        
        // Get all columns except price columns for deduplication
        let all_columns = combined.get_column_names();
        let dedup_columns: Vec<String> = all_columns.iter()
            .filter(|col| !price_columns.contains(*col))
            .map(|s| s.to_string())
            .collect();
        
        println!("      🧹 Deduplicating on {} columns (excluding price fields)...", dedup_columns.len());
        
        // Remove duplicates
        let unique_df = combined.unique(Some(&dedup_columns), UniqueKeepStrategy::Last, None)?;
        
        println!("      📊 Records before dedup: {}, after: {}", 
                 combined.height(), unique_df.height());
        
        // Sort by datetime if available
        let sorted_df = if unique_df.get_column_names().contains(&"datetime") {
            println!("      🔄 Sorting by datetime...");
            unique_df.lazy()
                .sort("datetime", Default::default())
                .collect()?
        } else if unique_df.get_column_names().contains(&"DeliveryDate") {
            // Try to sort by delivery date and hour if available
            let mut sort_cols = vec![col("DeliveryDate")];
            if unique_df.get_column_names().contains(&"DeliveryHour") {
                sort_cols.push(col("DeliveryHour"));
            }
            if unique_df.get_column_names().contains(&"DeliveryInterval") {
                sort_cols.push(col("DeliveryInterval"));
            }
            
            println!("      🔄 Sorting by date fields...");
            unique_df.lazy()
                .sort_by_exprs(&sort_cols, vec![false; sort_cols.len()], false, false)
                .collect()?
        } else {
            unique_df
        };
        
        Ok(sorted_df)
    }
    
    fn create_datetime_column(&self, df: &DataFrame) -> Result<DataFrame> {
        let mut result_df = df.clone();
        
        // Check if we have date/time columns
        let cols = df.get_column_names();
        
        // Handle SCED timestamp format first
        if cols.contains(&"SCEDTimestamp") || cols.contains(&"SCED_TIMESTAMP") {
            let timestamp_col = if cols.contains(&"SCEDTimestamp") { "SCEDTimestamp" } else { "SCED_TIMESTAMP" };
            let timestamps = df.column(timestamp_col)?;
            let timestamps_str = timestamps.utf8()?;
            
            let mut datetimes = Vec::new();
            for i in 0..df.height() {
                if let Some(ts_str) = timestamps_str.get(i) {
                    // Try different timestamp formats
                    if let Ok(dt) = NaiveDateTime::parse_from_str(ts_str, "%m/%d/%Y %H:%M:%S") {
                        datetimes.push(Some(dt.and_utc().timestamp_millis()));
                    } else if let Ok(dt) = NaiveDateTime::parse_from_str(ts_str, "%m/%d/%Y %I:%M:%S %p") {
                        datetimes.push(Some(dt.and_utc().timestamp_millis()));
                    } else {
                        datetimes.push(None);
                    }
                } else {
                    datetimes.push(None);
                }
            }
            
            let datetime_series = Series::new("datetime", datetimes);
            result_df.with_column(datetime_series)?;
        } else if cols.contains(&"DeliveryDate") {
            let dates = df.column("DeliveryDate")?;
            let dates_str = dates.utf8()?;
            
            let has_hour = cols.contains(&"DeliveryHour") || cols.contains(&"HourEnding");
            let has_interval = cols.contains(&"DeliveryInterval");
            
            let mut datetimes = Vec::new();
            
            if has_interval {
                // RT data with 5-minute intervals
                let hours = df.column("DeliveryHour")?;
                let intervals = df.column("DeliveryInterval")?;
                let hours_cast = hours.cast(&DataType::Int32)?;
                let hours_i32 = hours_cast.i32()?;
                let intervals_cast = intervals.cast(&DataType::Int32)?;
                let intervals_i32 = intervals_cast.i32()?;
                
                for i in 0..df.height() {
                    if let (Some(date_str), Some(hour), Some(interval)) = (
                        dates_str.get(i),
                        hours_i32.get(i),
                        intervals_i32.get(i)
                    ) {
                        if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                            let hour_adj = if hour == 24 { 0 } else { hour - 1 } as u32;
                            let minute = ((interval - 1) * 15) as u32;
                            let mut dt = date.and_hms_opt(hour_adj, minute, 0).unwrap();
                            if hour == 24 {
                                dt = dt + Duration::days(1);
                            }
                            datetimes.push(Some(dt.and_utc().timestamp_millis()));
                        } else {
                            datetimes.push(None);
                        }
                    } else {
                        datetimes.push(None);
                    }
                }
            } else if has_hour {
                // DAM data with hourly intervals
                let hour_col = if cols.contains(&"HourEnding") { "HourEnding" } else { "DeliveryHour" };
                let hours = df.column(hour_col)?;
                let hours_cast = hours.cast(&DataType::Int32)?;
                let hours_i32 = hours_cast.i32()?;
                
                for i in 0..df.height() {
                    if let (Some(date_str), Some(hour)) = (
                        dates_str.get(i),
                        hours_i32.get(i)
                    ) {
                        if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                            let hour_adj = if hour == 24 { 0 } else { hour - 1 } as u32;
                            let mut dt = date.and_hms_opt(hour_adj, 0, 0).unwrap();
                            if hour == 24 {
                                dt = dt + Duration::days(1);
                            }
                            datetimes.push(Some(dt.and_utc().timestamp_millis()));
                        } else {
                            datetimes.push(None);
                        }
                    } else {
                        datetimes.push(None);
                    }
                }
            } else {
                // Daily data
                for i in 0..df.height() {
                    if let Some(date_str) = dates_str.get(i) {
                        if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                            let dt = date.and_hms_opt(0, 0, 0).unwrap();
                            datetimes.push(Some(dt.and_utc().timestamp_millis()));
                        } else {
                            datetimes.push(None);
                        }
                    } else {
                        datetimes.push(None);
                    }
                }
            }
            
            let datetime_series = Series::new("datetime", datetimes);
            result_df.with_column(datetime_series)?;
        }
        
        Ok(result_df)
    }
    
    fn save_annual_files(&self, df: &DataFrame, output_dir: &Path, prefix: &str, year: i32) -> Result<()> {
        let base_name = format!("{}_{}", prefix, year);
        
        println!("    💾 Saving annual files...");
        println!("      Total records: {}", df.height());
        
        // Save in parallel
        rayon::scope(|s| {
            // CSV
            let csv_path = output_dir.join(format!("{}.csv", base_name));
            let df_csv = df.clone();
            s.spawn(move |_| {
                if let Ok(file) = fs::File::create(&csv_path) {
                    let mut df_mut = df_csv.clone();
                    if CsvWriter::new(file).finish(&mut df_mut).is_ok() {
                        println!("      ✓ Saved CSV: {}", csv_path.display());
                    }
                }
            });
            
            // Parquet
            let parquet_path = output_dir.join(format!("{}.parquet", base_name));
            let df_parquet = df.clone();
            s.spawn(move |_| {
                if let Ok(file) = fs::File::create(&parquet_path) {
                    let mut df_mut = df_parquet.clone();
                    if ParquetWriter::new(file).finish(&mut df_mut).is_ok() {
                        println!("      ✓ Saved Parquet: {}", parquet_path.display());
                    }
                }
            });
            
            // Arrow
            let arrow_path = output_dir.join(format!("{}.arrow", base_name));
            let df_arrow = df.clone();
            s.spawn(move |_| {
                if let Ok(file) = fs::File::create(&arrow_path) {
                    let mut df_mut = df_arrow.clone();
                    if IpcWriter::new(file).finish(&mut df_mut).is_ok() {
                        println!("      ✓ Saved Arrow: {}", arrow_path.display());
                    }
                }
            });
        });
        
        Ok(())
    }
    
    fn report_column_changes(&self) {
        if let Ok(history) = self.column_history.lock() {
            println!("\n📋 Column Evolution Report");
            println!("{}", "=".repeat(80));
            
            for (dataset_type, columns) in history.iter() {
                let mut cols_vec: Vec<String> = columns.iter().cloned().collect();
                cols_vec.sort();
                
                println!("\n🗂️  Dataset: {}", dataset_type);
                println!("   Total unique columns found: {}", cols_vec.len());
                
                // Group common columns vs rare columns
                let price_cols: Vec<&String> = cols_vec.iter()
                    .filter(|c| c.contains("Price") || c.contains("LMP") || c.contains("MCPC"))
                    .collect();
                    
                let date_cols: Vec<&String> = cols_vec.iter()
                    .filter(|c| c.contains("Date") || c.contains("Time") || c.contains("Hour") || c.contains("Interval"))
                    .collect();
                    
                let location_cols: Vec<&String> = cols_vec.iter()
                    .filter(|c| c.contains("SettlementPoint") || c.contains("Bus") || c.contains("Resource") || c.contains("Zone"))
                    .collect();
                
                println!("\n   📈 Price columns: {:?}", price_cols);
                println!("   📅 Date/Time columns: {:?}", date_cols);
                println!("   📍 Location columns: {:?}", location_cols);
                
                // Show all columns for debugging
                println!("\n   All columns:");
                for col in &cols_vec {
                    println!("      - {}", col);
                }
            }
            
            println!("\n{}", "=".repeat(80));
            println!("💡 Note: Different columns indicate ERCOT format changes over time");
            println!("   This helps identify when file formats were updated");
        }
    }
}

pub fn process_unified_data() -> Result<()> {
    // Check for environment variable override
    let base_dir = if let Ok(custom_dir) = std::env::var("ERCOT_DATA_BASE_DIR") {
        println!("Using custom data directory: {}", custom_dir);
        PathBuf::from(custom_dir)
    } else {
        PathBuf::from("/Users/enrico/data/ERCOT_data")
    };
    let output_dir = PathBuf::from("unified_processed_data");
    
    let processor = UnifiedDataProcessor::new(base_dir, output_dir);
    processor.process_all_data()
}