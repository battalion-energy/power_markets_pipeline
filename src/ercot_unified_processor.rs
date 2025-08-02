use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, Datelike, Duration};
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct UnifiedProcessor {
    base_dir: PathBuf,
    output_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DatasetConfig {
    pub name: &'static str,
    pub source_dir: &'static str,
    pub output_prefix: &'static str,
    pub date_column: &'static str,
    pub datetime_format: &'static str,
    pub key_columns: Vec<&'static str>,
}

impl UnifiedProcessor {
    pub fn new(base_dir: PathBuf, output_dir: PathBuf) -> Self {
        Self { base_dir, output_dir }
    }
    
    pub fn process_all_datasets(&self) -> Result<()> {
        // Configure Rayon to use all available cores
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .build_global()
            .unwrap_or_else(|_| {});
            
        println!("🚀 ERCOT Unified Data Processor");
        println!("Using {} CPU cores", rayon::current_num_threads());
        
        let datasets = vec![
            // RT Market SPPs and LMPs
            DatasetConfig {
                name: "RT LMPs by Resource Nodes",
                source_dir: "LMPs by Resource Nodes, Load Zones and Trading Hubs",
                output_prefix: "RT_LMPs",
                date_column: "DeliveryDate",
                datetime_format: "%m/%d/%Y",
                key_columns: vec!["SettlementPoint", "SettlementPointPrice"],
            },
            DatasetConfig {
                name: "RT Settlement Point Prices",
                source_dir: "Settlement_Point_Prices_at_Resource_Nodes,_Hubs_and_Load_Zones",
                output_prefix: "RT_Settlement_Point_Prices",
                date_column: "DeliveryDate",
                datetime_format: "%m/%d/%Y",
                key_columns: vec!["DeliveryDate", "DeliveryHour", "DeliveryInterval", "SettlementPointName"],
            },
            // DAM Hourly SPPs and LMPs
            DatasetConfig {
                name: "DAM Hourly LMPs",
                source_dir: "DAM_Hourly_LMPs",
                output_prefix: "DAM_Hourly_LMPs",
                date_column: "DeliveryDate",
                datetime_format: "%m/%d/%Y",
                key_columns: vec!["DeliveryDate", "HourEnding", "BusName"],
            },
            DatasetConfig {
                name: "DAM Settlement Point Prices",
                source_dir: "DAM_Settlement_Point_Prices",
                output_prefix: "DAM_Settlement_Point_Prices",
                date_column: "DeliveryDate",
                datetime_format: "%m/%d/%Y",
                key_columns: vec!["DeliveryDate", "HourEnding", "SettlementPoint"],
            },
            // Ancillary Services
            DatasetConfig {
                name: "DAM Clearing Prices for Capacity",
                source_dir: "DAM_Clearing_Prices_for_Capacity",
                output_prefix: "DAM_Clearing_Prices_Capacity",
                date_column: "DeliveryDate",
                datetime_format: "%m/%d/%Y",
                key_columns: vec!["DeliveryDate", "HourEnding", "AncillaryType"],
            },
            // Shadow Prices
            DatasetConfig {
                name: "SCED Shadow Prices",
                source_dir: "SCED_Shadow_Prices_and_Binding_Transmission_Constraints",
                output_prefix: "SCED_Shadow_Prices",
                date_column: "SCEDTimestamp",
                datetime_format: "%m/%d/%Y %H:%M:%S",
                key_columns: vec!["ConstraintName", "ShadowPrice"],
            },
            DatasetConfig {
                name: "DAM Shadow Prices",
                source_dir: "DAM_Shadow_Prices",
                output_prefix: "DAM_Shadow_Prices",
                date_column: "DeliveryDate",
                datetime_format: "%m/%d/%Y",
                key_columns: vec!["ConstraintName", "ShadowPrice"],
            },
        ];
        
        // Process datasets sequentially (but each dataset uses parallel processing internally)
        let multi_progress = Arc::new(MultiProgress::new());
        
        for config in datasets.iter() {
            println!("\n{}", "=".repeat(80));
            println!("Processing: {}", config.name);
            println!("{}", "=".repeat(80));
            
            if let Err(e) = self.process_dataset(config, multi_progress.clone()) {
                eprintln!("Error processing {}: {}", config.name, e);
            }
        }
        
        Ok(())
    }
    
    fn process_dataset(&self, config: &DatasetConfig, multi_progress: Arc<MultiProgress>) -> Result<()> {
        let source_path = self.base_dir.join(config.source_dir);
        if !source_path.exists() {
            println!("Source directory not found: {}", source_path.display());
            return Ok(());
        }
        
        // Step 1: Extract all ZIP files recursively (in parallel)
        println!("Step 1: Extracting ZIP files in parallel...");
        let csv_files = self.extract_all_zips_parallel(&source_path, multi_progress.clone())?;
        println!("Found {} CSV files after extraction", csv_files.len());
        
        if csv_files.is_empty() {
            println!("No CSV files found in {}", config.source_dir);
            return Ok(());
        }
        
        // Step 2: Process CSV files by year (in parallel)
        println!("Step 2: Processing CSV files by year in parallel...");
        let yearly_data = self.process_csv_files_by_year_parallel(&csv_files, config, multi_progress.clone())?;
        
        // Step 3: Save annual files (in parallel)
        println!("Step 3: Saving annual files in parallel...");
        self.save_annual_files_parallel(&yearly_data, config)?;
        
        Ok(())
    }
    
    fn extract_all_zips_parallel(&self, dir: &Path, multi_progress: Arc<MultiProgress>) -> Result<Vec<Vec<u8>>> {
        // Find all initial ZIP files
        let mut zip_files = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("zip") {
                zip_files.push(path);
            }
        }
        
        println!("Found {} top-level ZIP files", zip_files.len());
        
        let pb = multi_progress.add(ProgressBar::new(zip_files.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} Extracting ZIPs")
            .unwrap());
        
        // Shared storage for CSV contents
        let csv_contents = Arc::new(Mutex::new(Vec::new()));
        
        // Process ZIP files in parallel with reasonable batch size
        let batch_size = 100; // Process 100 files at a time to avoid stack overflow
        
        for chunk in zip_files.chunks(batch_size) {
            chunk.par_iter().for_each(|zip_path| {
                pb.inc(1);
                
                if let Ok(file) = fs::File::open(zip_path) {
                    if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                        let mut local_csvs = Vec::new();
                        let mut nested_zips = Vec::new();
                        
                        for i in 0..archive.len() {
                            if let Ok(mut file) = archive.by_index(i) {
                                let name = file.name().to_string();
                                
                                if name.ends_with(".zip") {
                                    // Nested ZIP - extract to temp
                                    let mut buffer = Vec::new();
                                    if file.read_to_end(&mut buffer).is_ok() {
                                        nested_zips.push(buffer);
                                    }
                                } else if name.ends_with(".csv") {
                                    // CSV file - read into memory
                                    let mut buffer = Vec::new();
                                    if file.read_to_end(&mut buffer).is_ok() {
                                        local_csvs.push(buffer);
                                    }
                                }
                            }
                        }
                        
                        // Process nested ZIPs
                        for zip_buffer in nested_zips {
                            let cursor = std::io::Cursor::new(zip_buffer);
                            if let Ok(mut nested_archive) = ::zip::ZipArchive::new(cursor) {
                                for i in 0..nested_archive.len() {
                                    if let Ok(mut file) = nested_archive.by_index(i) {
                                        if file.name().ends_with(".csv") {
                                            let mut buffer = Vec::new();
                                            if file.read_to_end(&mut buffer).is_ok() {
                                                local_csvs.push(buffer);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Add all CSVs to shared storage
                        if !local_csvs.is_empty() {
                            csv_contents.lock().unwrap().extend(local_csvs);
                        }
                    }
                }
            });
        }
        
        pb.finish_with_message("Extraction complete");
        
        let final_contents = Arc::try_unwrap(csv_contents)
            .map(|mutex| mutex.into_inner().unwrap())
            .unwrap_or_else(|arc| arc.lock().unwrap().clone());
        Ok(final_contents)
    }
    
    fn process_csv_files_by_year_parallel(&self, csv_contents: &[Vec<u8>], config: &DatasetConfig, 
                                         multi_progress: Arc<MultiProgress>) -> Result<HashMap<i32, Vec<DataFrame>>> {
        
        let yearly_dfs = Arc::new(Mutex::new(HashMap::new()));
        
        let pb = multi_progress.add(ProgressBar::new(csv_contents.len() as u64));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} Processing CSVs")
            .unwrap());
        
        // Process CSVs in parallel batches to avoid stack overflow
        let csv_batch_size = 1000;
        for batch in csv_contents.chunks(csv_batch_size) {
            batch.par_iter().for_each(|csv_data| {
            pb.inc(1);
            
            // Parse CSV from memory
            let cursor = std::io::Cursor::new(csv_data);
            if let Ok(df) = CsvReader::new(cursor)
                .has_header(true)
                .finish() {
                
                // Check if date column exists
                if df.column(config.date_column).is_err() {
                    return;
                }
                
                // Extract year from date column
                if let Ok(dates) = df.column(config.date_column) {
                    if let Ok(date_str) = dates.utf8() {
                        if let Some(first_date) = date_str.get(0) {
                            let year = if config.datetime_format.contains("%H") {
                                // DateTime format
                                NaiveDateTime::parse_from_str(first_date, config.datetime_format)
                                    .ok()
                                    .map(|dt| dt.year())
                            } else {
                                // Date only format
                                NaiveDate::parse_from_str(first_date, config.datetime_format)
                                    .ok()
                                    .map(|d| d.year())
                            };
                            
                            if let Some(year) = year {
                                if year >= 2010 && year <= 2025 { // Sanity check
                                    yearly_dfs.lock().unwrap()
                                        .entry(year)
                                        .or_insert_with(Vec::new)
                                        .push(df);
                                }
                            }
                        }
                    }
                }
            }
            });
        }
        
        pb.finish_with_message("CSV processing complete");
        
        let yearly_data = Arc::try_unwrap(yearly_dfs)
            .map(|mutex| mutex.into_inner().unwrap())
            .unwrap_or_else(|arc| arc.lock().unwrap().clone());
        
        // Report statistics
        for (year, dfs) in &yearly_data {
            let total_rows: usize = dfs.iter().map(|df| df.height()).sum();
            println!("  Year {}: {} files, {} total rows", year, dfs.len(), total_rows);
        }
        
        Ok(yearly_data)
    }
    
    fn save_annual_files_parallel(&self, yearly_data: &HashMap<i32, Vec<DataFrame>>, config: &DatasetConfig) 
        -> Result<()> {
        
        // Create output directory
        let dataset_output_dir = self.output_dir.join(config.output_prefix);
        fs::create_dir_all(&dataset_output_dir)?;
        
        // Process years sequentially to avoid memory issues with large datasets
        let mut sorted_years: Vec<(&i32, &Vec<DataFrame>)> = yearly_data.iter().collect();
        sorted_years.sort_by_key(|(year, _)| *year);
        
        for (year, dfs) in sorted_years {
            if dfs.is_empty() {
                continue;
            }
            
            println!("  Processing year {} ({} files)...", year, dfs.len());
            
            // For very large datasets, process in batches to avoid memory exhaustion
            let total_rows: usize = dfs.iter().map(|df| df.height()).sum();
            let estimated_memory_mb = (total_rows * 100) / 1_000_000; // More conservative estimate
            println!("    Total rows: {} (estimated memory: {}MB)", total_rows, estimated_memory_mb);
            
            // Get available memory (rough estimate)
            let available_memory_gb = 8; // Conservative estimate for most systems
            let available_memory_mb = available_memory_gb * 1024;
            
            if estimated_memory_mb > available_memory_mb / 2 {
                println!("    ⚠️  Large dataset detected, using aggressive batching");
            }
            
            let batch_size = if estimated_memory_mb > available_memory_mb / 2 {
                // For very large memory usage, use tiny batches
                50
            } else if total_rows > 50_000_000 {
                // For datasets with >50M rows, process in smaller batches
                100
            } else if total_rows > 10_000_000 {
                // For datasets with >10M rows, use medium batches
                300
            } else {
                // For smaller datasets, process all at once
                dfs.len()
            };
            
            println!("    Using batch size: {} files per batch", batch_size);
            
            let mut all_processed_dfs = Vec::new();
            
            // Process in batches
            for (batch_idx, batch) in dfs.chunks(batch_size).enumerate() {
                println!("    Processing batch {} of {} ({} files)...", 
                         batch_idx + 1, 
                         (dfs.len() + batch_size - 1) / batch_size,
                         batch.len());
                
                let lazy_dfs: Vec<LazyFrame> = batch.iter()
                    .map(|df| df.clone().lazy())
                    .collect();
                
                if let Ok(combined) = concat(
                    lazy_dfs.iter().map(|lf| lf.clone()).collect::<Vec<_>>().as_slice(),
                    UnionArgs::default(),
                ) {
                    // Collect the combined dataframe first
                    if let Ok(mut final_df) = combined.collect() {
                    // Create proper datetime column based on data type
                    let has_hour = final_df.get_column_names().contains(&"DeliveryHour") || 
                                   final_df.get_column_names().contains(&"HourEnding");
                    let has_interval = final_df.get_column_names().contains(&"DeliveryInterval");
                    
                    let datetime_col = if config.date_column == "DeliveryDate" {
                        let datetime_created = (|| -> Result<bool> {
                            let dates = final_df.column("DeliveryDate")?;
                            let dates_str = dates.utf8()?;
                            
                            let mut datetimes = Vec::new();
                            
                            if has_interval {
                                // RT data with 5-minute intervals
                                let hours = final_df.column("DeliveryHour")?;
                                let intervals = final_df.column("DeliveryInterval")?;
                                let hours_cast = hours.cast(&DataType::Int32)?;
                                let hours_i32 = hours_cast.i32()?;
                                let intervals_cast = intervals.cast(&DataType::Int32)?;
                                let intervals_i32 = intervals_cast.i32()?;
                                
                                for i in 0..final_df.height() {
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
                                let hour_col = if final_df.get_column_names().contains(&"HourEnding") {
                                    "HourEnding"
                                } else {
                                    "DeliveryHour"
                                };
                                let hours = final_df.column(hour_col)?;
                                let hours_cast = hours.cast(&DataType::Int32)?;
                                let hours_i32 = hours_cast.i32()?;
                                
                                for i in 0..final_df.height() {
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
                                // Daily data or other
                                for i in 0..final_df.height() {
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
                            final_df.with_column(datetime_series)?;
                            Ok(true)
                        })();
                        
                        if datetime_created.is_ok() {
                            "datetime"
                        } else {
                            config.date_column
                        }
                    } else {
                        config.date_column
                    };
                    
                    // Remove duplicates using the specified key columns
                    if !config.key_columns.is_empty() {
                        let mut unique_cols = Vec::new();
                        
                        // Use all key columns that exist in the dataframe
                        for key_col in &config.key_columns {
                            if final_df.get_column_names().contains(key_col) {
                                unique_cols.push(key_col.to_string());
                            }
                        }
                        
                        if !unique_cols.is_empty() {
                            println!("  🧹 Removing duplicates on columns: {:?}", unique_cols);
                            if let Ok(unique_df) = final_df.unique(Some(&unique_cols), UniqueKeepStrategy::Last, None) {
                                final_df = unique_df;
                            }
                        }
                    }
                    
                    // Sort by datetime column
                    println!("  🔄 Sorting by {}", datetime_col);
                    let sorted_df = final_df.clone().lazy()
                        .sort(datetime_col, Default::default())
                        .collect();
                    if let Ok(sorted) = sorted_df {
                        final_df = sorted;
                    }
                    
                    // Store the processed dataframe for this batch
                    all_processed_dfs.push(final_df);
                }
            }
            }
            
            // Now combine all batches and save the final result
            if !all_processed_dfs.is_empty() {
                println!("    📦 Combining {} processed batches...", all_processed_dfs.len());
                
                let final_lazy_dfs: Vec<LazyFrame> = all_processed_dfs.iter()
                    .map(|df| df.clone().lazy())
                    .collect();
                
                if let Ok(final_combined) = concat(
                    final_lazy_dfs.iter().map(|lf| lf.clone()).collect::<Vec<_>>().as_slice(),
                    UnionArgs::default(),
                ) {
                    if let Ok(mut year_df) = final_combined.collect() {
                        // Final deduplication across all batches
                        if !config.key_columns.is_empty() {
                            let mut unique_cols = Vec::new();
                            for key_col in &config.key_columns {
                                if year_df.get_column_names().contains(key_col) {
                                    unique_cols.push(key_col.to_string());
                                }
                            }
                            
                            if !unique_cols.is_empty() {
                                println!("    🧹 Final deduplication on columns: {:?}", unique_cols);
                                if let Ok(unique_df) = year_df.unique(Some(&unique_cols), UniqueKeepStrategy::Last, None) {
                                    year_df = unique_df;
                                }
                            }
                        }
                        
                        // Final sort
                        let datetime_col = if year_df.get_column_names().contains(&"datetime") {
                            "datetime"
                        } else {
                            config.date_column
                        };
                        
                        println!("    🔄 Final sorting by {}", datetime_col);
                        let sorted_df = year_df.clone().lazy()
                            .sort(datetime_col, Default::default())
                            .collect();
                        if let Ok(sorted) = sorted_df {
                            year_df = sorted;
                        }
                        
                        let base_name = format!("{}_{}", config.output_prefix, year);
                        
                        // Save files in parallel using rayon tasks
                        let csv_path = dataset_output_dir.join(format!("{}.csv", base_name));
                        let parquet_path = dataset_output_dir.join(format!("{}.parquet", base_name));
                        let arrow_path = dataset_output_dir.join(format!("{}.arrow", base_name));
                        
                        println!("    💾 Saving final files for year {}...", year);
                        
                        rayon::scope(|s| {
                            let df_csv = year_df.clone();
                            s.spawn(move |_| {
                                if let Ok(file) = fs::File::create(&csv_path) {
                                    let mut df_mut = df_csv.clone();
                                    if CsvWriter::new(file).finish(&mut df_mut).is_ok() {
                                        println!("      ✓ Saved CSV: {}", csv_path.display());
                                    }
                                }
                            });
                            
                            let df_parquet = year_df.clone();
                            s.spawn(move |_| {
                                if let Ok(file) = fs::File::create(&parquet_path) {
                                    let mut df_mut = df_parquet.clone();
                                    if ParquetWriter::new(file).finish(&mut df_mut).is_ok() {
                                        println!("      ✓ Saved Parquet: {}", parquet_path.display());
                                    }
                                }
                            });
                            
                            let df_arrow = year_df;
                            s.spawn(move |_| {
                                if let Ok(file) = fs::File::create(&arrow_path) {
                                    let mut df_mut = df_arrow.clone();
                                    if IpcWriter::new(file).finish(&mut df_mut).is_ok() {
                                        println!("      ✓ Saved Arrow: {}", arrow_path.display());
                                    }
                                }
                            });
                        });
                    }
                }
            }
        }
        
        Ok(())
    }
}

pub fn process_all_ercot_data() -> Result<()> {
    let base_dir = PathBuf::from("/Users/enrico/data/ERCOT_data");
    let output_dir = PathBuf::from("processed_ercot_data");
    
    let processor = UnifiedProcessor::new(base_dir, output_dir);
    processor.process_all_datasets()
}