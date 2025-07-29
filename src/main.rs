use anyhow::Result;
use chrono::{Duration, NaiveDate};
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod ercot_processor;
mod comprehensive_processor;
mod process_historical;
mod dam_processor;
mod ancillary_processor;
mod lmp_processor;
mod lmp_fast_processor;
mod lmp_full_processor;
mod disclosure_processor;
mod disclosure_fast_processor;
mod bess_analyzer;
mod bess_revenue_calculator;

fn extract_year_from_filename(filename: &str) -> Option<u16> {
    // Look for pattern like .20240823. (YYYYMMDD) or _20240823_
    // Try first pattern
    if let Some(start) = filename.find(".20") {
        if let Some(year_str) = filename.get(start + 1..start + 5) {
            if let Ok(year) = year_str.parse::<u16>() {
                if year >= 2000 && year <= 2100 {
                    return Some(year);
                }
            }
        }
    }
    
    // Try second pattern
    if let Some(start) = filename.find("_20") {
        if let Some(year_str) = filename.get(start + 1..start + 5) {
            if let Ok(year) = year_str.parse::<u16>() {
                if year >= 2000 && year <= 2100 {
                    return Some(year);
                }
            }
        }
    }
    
    None
}

fn process_year_files(year: u16, files: &[PathBuf], output_dir: &Path) -> Result<()> {
    println!("\n📅 Processing year {}: {} files", year, files.len());
    
    // Create progress bar
    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap());
    
    // Process files in parallel batches
    let batch_size = 100;
    let mut all_dfs = Vec::new();
    
    for chunk in files.chunks(batch_size) {
        let chunk_dfs: Vec<DataFrame> = chunk
            .par_iter()
            .filter_map(|file| {
                pb.inc(1);
                
                // Read CSV with Polars, forcing price column to be float
                let schema = Arc::new(Schema::from_iter([
                    Field::new("SettlementPointPrice", DataType::Float64),
                ]));
                
                let df = CsvReader::new(std::fs::File::open(file).ok()?)
                    .has_header(true)
                    .with_dtypes(Some(schema))
                    .finish()
                    .ok()?;
                
                // Check if it has required columns
                let cols = df.get_column_names();
                if !cols.contains(&"DeliveryDate") {
                    return None;
                }
                
                // Handle different column names for settlement point
                let df = if cols.contains(&"SettlementPointName") && !cols.contains(&"SettlementPoint") {
                    df.lazy()
                        .with_column(col("SettlementPointName").alias("SettlementPoint"))
                        .collect()
                        .ok()?
                } else if !cols.contains(&"SettlementPoint") {
                    return None;
                } else {
                    df
                };
                
                Some(df)
            })
            .collect();
        
        all_dfs.extend(chunk_dfs);
    }
    
    pb.finish_with_message("Files loaded");
    
    if all_dfs.is_empty() {
        println!("  ❌ No valid data for year {}", year);
        return Ok(());
    }
    
    println!("  📊 Combining {} dataframes...", all_dfs.len());
    
    // Concatenate all dataframes
    let mut combined = concat(
        all_dfs.iter().map(|df| df.clone().lazy()).collect::<Vec<_>>().as_slice(),
        UnionArgs::default(),
    )?
    .collect()?;
    
    // Create datetime column
    println!("  🕐 Creating datetime column...");
    let delivery_dates = combined.column("DeliveryDate")?;
    let delivery_hours = combined.column("DeliveryHour")?.cast(&DataType::Int32)?;
    let delivery_intervals = combined.column("DeliveryInterval")?.cast(&DataType::Int32)?;
    
    // Calculate datetime components
    let hours = delivery_hours.i32()?
        .apply(|v| if v.unwrap_or(0) == 24 { Some(0) } else { v.map(|x| x - 1) });
    
    let minutes = delivery_intervals.i32()?
        .apply(|i| i.map(|v| (v - 1) * 15));
    
    // Parse dates and create datetime
    let mut datetimes = Vec::new();
    for i in 0..combined.height() {
        if let Some(date_str) = delivery_dates.utf8()?.get(i) {
            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                let hour = hours.get(i).unwrap_or(0) as u32;
                let minute = minutes.get(i).unwrap_or(0) as u32;
                let mut datetime = date.and_hms_opt(hour, minute, 0).unwrap();
                
                // Handle hour 24
                if delivery_hours.i32()?.get(i) == Some(24) {
                    datetime = datetime + Duration::days(1);
                }
                
                datetimes.push(Some(datetime.and_utc().timestamp_millis())); // milliseconds
            } else {
                datetimes.push(None);
            }
        } else {
            datetimes.push(None);
        }
    }
    
    let datetime_series = Series::new("datetime", datetimes);
    combined.with_column(datetime_series)?;
    
    // Select and rename columns
    println!("  📋 Selecting columns...");
    let cols = combined.get_column_names();
    let price_col = if cols.contains(&"SettlementPointPrice") {
        col("SettlementPointPrice")
    } else if cols.contains(&"LMP") {
        col("LMP")
    } else {
        return Err(anyhow::anyhow!("No price column found"));
    };
    
    let final_df = combined.lazy()
        .select([
            col("datetime"),
            col("SettlementPoint"),
            price_col.alias("SettlementPointPrice"),
        ])
        .sort("datetime", Default::default())
        .collect()?;
    
    // Remove duplicates
    let unique_df = final_df.unique(Some(&["datetime".to_string(), "SettlementPoint".to_string()]), UniqueKeepStrategy::Last, None)?;
    
    println!("  📊 Final record count: {}", unique_df.height());
    
    // Save files
    let base_name = format!("RT_Settlement_Point_Prices_{}", year);
    
    // CSV
    let csv_path = output_dir.join(format!("{}.csv", base_name));
    println!("  💾 Saving CSV...");
    CsvWriter::new(std::fs::File::create(&csv_path)?)
        .finish(&mut unique_df.clone())?;
    
    // Parquet
    let parquet_path = output_dir.join(format!("{}.parquet", base_name));
    println!("  📦 Saving Parquet...");
    ParquetWriter::new(std::fs::File::create(&parquet_path)?)
        .finish(&mut unique_df.clone())?;
    
    // Arrow IPC (similar to .arrow)
    let arrow_path = output_dir.join(format!("{}.arrow", base_name));
    println!("  🏹 Saving Arrow IPC...");
    IpcWriter::new(std::fs::File::create(&arrow_path)?)
        .finish(&mut unique_df.clone())?;
    
    println!("  ✅ Completed year {}", year);
    Ok(())
}

fn main() -> Result<()> {
    // Set Rayon to use all available cores
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .unwrap();
    
    // Check command line arguments
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() > 1 && args[1] == "--all" {
        // Process all ERCOT data types
        comprehensive_processor::process_all_ercot_data()?;
    } else if args.len() > 1 && args[1] == "--extract" {
        // Extract historical data
        process_historical::extract_and_process_historical()?;
    } else if args.len() > 1 && args[1] == "--dam" {
        // Process DAM settlement data
        dam_processor::process_all_dam_data()?;
    } else if args.len() > 1 && args[1] == "--ancillary" {
        // Process ancillary services data
        ancillary_processor::process_all_ancillary_data()?;
    } else if args.len() > 1 && args[1] == "--lmp" {
        // Process LMP data with nested extraction
        lmp_processor::process_all_lmp_data()?;
    } else if args.len() > 1 && args[1] == "--lmp-fast" {
        // Process existing LMP CSV files only
        lmp_fast_processor::process_existing_lmp_csv()?;
    } else if args.len() > 1 && args[1] == "--lmp-sample" {
        // Process sample of LMP data
        let sample_size = if args.len() > 2 {
            args[2].parse().unwrap_or(1000)
        } else {
            1000
        };
        lmp_fast_processor::process_lmp_sample(sample_size)?;
    } else if args.len() > 1 && args[1] == "--lmp-all" {
        // Process ALL LMP historical data
        lmp_full_processor::process_all_lmp_historical()?;
    } else if args.len() > 1 && args[1] == "--disclosure" {
        // Process 60-Day disclosure reports
        disclosure_processor::process_all_disclosures()?;
    } else if args.len() > 1 && args[1] == "--disclosure-fast" {
        // Process already extracted disclosure CSV files
        disclosure_fast_processor::process_disclosure_fast()?;
    } else if args.len() > 1 && args[1] == "--bess" {
        // Analyze BESS resources
        bess_analyzer::analyze_bess_resources()?;
    } else if args.len() > 1 && args[1] == "--bess-revenue" {
        // Calculate BESS revenues
        bess_revenue_calculator::calculate_bess_revenues()?;
    } else {
        // Process only RT Settlement Point Prices (original functionality)
        println!("🚀 ERCOT RT Settlement Point Prices - Rust Processor");
        println!("Using {} CPU cores", num_cpus::get());
        println!("Rayon thread pool configured with {} threads", rayon::current_num_threads());
        println!("{}", "=".repeat(60));
        
        // Use test data directory for testing
        let data_dir = if std::env::args().any(|arg| arg == "--test") {
            PathBuf::from("test_data")
        } else {
            PathBuf::from("/Users/enrico/data/ERCOT_data/Settlement_Point_Prices_at_Resource_Nodes,_Hubs_and_Load_Zones/csv")
        };
        
        let output_dir = PathBuf::from("annual_data");
        std::fs::create_dir_all(&output_dir)?;
    
    // Find all CSV files
    let pattern = data_dir.join("*.csv");
    let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
        .filter_map(Result::ok)
        .collect();
    
    println!("Found {} RT CSV files", csv_files.len());
    
    // Group files by year
    let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
    for file in csv_files {
        if let Some(year) = extract_year_from_filename(file.file_name().unwrap().to_str().unwrap()) {
            files_by_year.entry(year).or_insert_with(Vec::new).push(file);
        }
    }
    
    let mut years: Vec<u16> = files_by_year.keys().cloned().collect();
    years.sort();
    println!("Years found: {:?}", years);
    
    // Process each year
    let start = std::time::Instant::now();
    
    for year in years {
        let year_files = &files_by_year[&year];
        process_year_files(year, year_files, &output_dir)?;
    }
    
        let duration = start.elapsed();
        println!("\n✅ Processing complete in {:?}!", duration);
    }
    
    Ok(())
}