use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct LmpFullProcessor {
    underscores_dir: PathBuf,
    csv_dir: PathBuf,
    output_dir: PathBuf,
}

impl LmpFullProcessor {
    pub fn new() -> Result<Self> {
        let underscores_dir = PathBuf::from("/Users/enrico/data/ERCOT_data/LMPs_by_Resource_Nodes,_Load_Zones_and_Trading_Hubs");
        let csv_dir = underscores_dir.join("csv");
        let output_dir = PathBuf::from("lmp_annual_data");
        
        // Create directories
        std::fs::create_dir_all(&csv_dir)?;
        std::fs::create_dir_all(&output_dir)?;
        
        Ok(Self {
            underscores_dir,
            csv_dir,
            output_dir,
        })
    }

    pub fn extract_all_and_process(&self) -> Result<()> {
        println!("🚀 LMP Full Historical Processing - ALL YEARS");
        println!("{}", "=".repeat(60));
        
        // Step 1: Extract ALL remaining ZIP files
        self.extract_all_remaining_zips()?;
        
        // Step 2: Create complete annual rollups
        self.create_complete_annual_rollups()?;
        
        Ok(())
    }

    fn extract_all_remaining_zips(&self) -> Result<()> {
        println!("🗜️  Extracting ALL remaining ZIP files...");
        
        // Find all ZIP files
        let pattern = self.underscores_dir.join("*.zip");
        let zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} total ZIP files", zip_files.len());
        
        // Get existing CSV files to avoid re-extraction
        let csv_pattern = self.csv_dir.join("*.csv");
        let existing_csvs: HashSet<String> = glob(csv_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(String::from))
            .collect();
        
        println!("Found {} existing CSV files", existing_csvs.len());
        
        // Filter out already processed files
        let mut unprocessed_zips = Vec::new();
        for zip_path in zip_files {
            let zip_stem = zip_path.file_stem().unwrap().to_str().unwrap();
            let has_csv = existing_csvs.iter().any(|csv| csv.contains(zip_stem));
            if !has_csv {
                unprocessed_zips.push(zip_path);
            }
        }
        
        println!("Need to extract {} ZIP files", unprocessed_zips.len());
        
        if unprocessed_zips.is_empty() {
            println!("✅ All ZIP files already extracted");
            return Ok(());
        }
        
        // Process in parallel batches
        let pb = ProgressBar::new(unprocessed_zips.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} - {msg}")
            .unwrap());
        
        let extracted_count = Arc::new(Mutex::new(0));
        let batch_size = 1000;
        
        for batch in unprocessed_zips.chunks(batch_size) {
            let batch_extracted: usize = batch
                .par_iter()
                .map(|zip_path| {
                    let zip_stem = zip_path.file_stem().unwrap().to_str().unwrap();
                    pb.set_message(format!("Processing {}", zip_stem));
                    pb.inc(1);
                    
                    // Extract the ZIP file
                    if let Ok(file) = std::fs::File::open(zip_path) {
                        if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                            let mut local_extracted = 0;
                            
                            for i in 0..archive.len() {
                                if let Ok(mut zip_file) = archive.by_index(i) {
                                    if zip_file.name().ends_with(".csv") {
                                        let csv_name = format!("{}_{}", zip_stem, zip_file.name().replace("/", "_"));
                                        let csv_path = self.csv_dir.join(csv_name);
                                        
                                        if let Ok(mut output) = std::fs::File::create(&csv_path) {
                                            if std::io::copy(&mut zip_file, &mut output).is_ok() {
                                                local_extracted += 1;
                                            }
                                        }
                                    }
                                }
                            }
                            
                            return local_extracted;
                        }
                    }
                    
                    0
                })
                .sum();
            
            let mut count = extracted_count.lock().unwrap();
            *count += batch_extracted;
            pb.set_message(format!("Extracted {} CSV files", *count));
        }
        
        let final_count = *extracted_count.lock().unwrap();
        pb.finish_with_message(format!("Extracted {} CSV files total", final_count));
        Ok(())
    }

    fn create_complete_annual_rollups(&self) -> Result<()> {
        println!("\n📊 Creating complete annual rollups...");
        
        // Find all CSV files
        let pattern = self.csv_dir.join("*.csv");
        let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} total CSV files to process", csv_files.len());
        
        if csv_files.is_empty() {
            println!("❌ No CSV files found");
            return Ok(());
        }
        
        // Group files by year
        let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
        
        for csv_path in csv_files {
            let filename = csv_path.file_stem().unwrap().to_str().unwrap();
            
            if let Some(year) = self.extract_year_from_filename(filename) {
                files_by_year.entry(year).or_insert_with(Vec::new).push(csv_path);
            }
        }
        
        let mut years: Vec<u16> = files_by_year.keys().cloned().collect();
        years.sort();
        println!("Complete years found: {:?}", years);
        
        // Process each year
        for (year, year_files) in files_by_year {
            // Skip if we already have output for this year (unless it's a small file indicating incomplete processing)
            let existing_parquet = self.output_dir.join(format!("LMPs_by_Resource_Nodes_Load_Zones_Trading_Hubs_{}.parquet", year));
            if existing_parquet.exists() {
                if let Ok(metadata) = std::fs::metadata(&existing_parquet) {
                    // If the parquet file is larger than 10MB, assume it's complete
                    if metadata.len() > 10_000_000 {
                        println!("⏭️  Skipping year {} (already processed)", year);
                        continue;
                    }
                }
            }
            
            self.process_year_lmp_files(year, &year_files)?;
        }
        
        Ok(())
    }

    fn extract_year_from_filename(&self, filename: &str) -> Option<u16> {
        // Look for patterns like .20091201. (8-digit date)
        if let Some(pos) = filename.find(".200") {
            if let Some(year_str) = filename.get(pos + 1..pos + 5) {
                if let Ok(year) = year_str.parse::<u16>() {
                    if year >= 2000 && year <= 2030 {
                        return Some(year);
                    }
                }
            }
        }
        
        // Look for patterns like .201x or .202x
        if let Some(pos) = filename.find(".201") {
            if let Some(year_str) = filename.get(pos + 1..pos + 5) {
                if let Ok(year) = year_str.parse::<u16>() {
                    if year >= 2010 && year <= 2030 {
                        return Some(year);
                    }
                }
            }
        }
        
        // Look for patterns like _200x_ or _201x_ or _202x_
        for decade in ["200", "201", "202"] {
            if let Some(pos) = filename.find(&format!("_{}", decade)) {
                if let Some(year_str) = filename.get(pos + 1..pos + 5) {
                    if let Ok(year) = year_str.parse::<u16>() {
                        if year >= 2000 && year <= 2030 {
                            return Some(year);
                        }
                    }
                }
            }
        }
        
        None
    }

    fn process_year_lmp_files(&self, year: u16, files: &[PathBuf]) -> Result<()> {
        println!("\n📅 Processing LMP year {}: {} files", year, files.len());
        
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) - {msg}")
            .unwrap());
        
        // Process files in parallel batches
        let batch_size = 200;
        let mut all_dfs = Vec::new();
        
        for chunk in files.chunks(batch_size) {
            pb.set_message(format!("Loading batch of {} files", chunk.len()));
            
            let chunk_dfs: Vec<DataFrame> = chunk
                .par_iter()
                .filter_map(|file| {
                    pb.inc(1);
                    
                    // Read CSV with error handling
                    match CsvReader::new(std::fs::File::open(file).ok()?)
                        .has_header(true)
                        .finish() {
                        Ok(df) => {
                            // Basic validation - must have some data
                            if df.height() > 0 {
                                Some(df)
                            } else {
                                None
                            }
                        },
                        Err(_) => None
                    }
                })
                .collect();
            
            all_dfs.extend(chunk_dfs);
            pb.set_message(format!("Loaded {} dataframes so far", all_dfs.len()));
        }
        
        pb.finish_with_message(format!("Loaded {} dataframes", all_dfs.len()));
        
        if all_dfs.is_empty() {
            println!("  ❌ No valid data for year {}", year);
            return Ok(());
        }
        
        println!("  📊 Combining {} dataframes...", all_dfs.len());
        
        // Concatenate all dataframes
        let combined = concat(
            all_dfs.iter().map(|df| df.clone().lazy()).collect::<Vec<_>>().as_slice(),
            UnionArgs::default(),
        )?.collect()?;
        
        println!("  📊 Combined records: {}", combined.height());
        
        // Save files
        let base_name = format!("LMPs_by_Resource_Nodes_Load_Zones_Trading_Hubs_{}", year);
        
        // CSV
        let csv_path = self.output_dir.join(format!("{}.csv", base_name));
        println!("  💾 Saving CSV...");
        CsvWriter::new(std::fs::File::create(&csv_path)?)
            .finish(&mut combined.clone())?;
        
        // Parquet
        let parquet_path = self.output_dir.join(format!("{}.parquet", base_name));
        println!("  📦 Saving Parquet...");
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut combined.clone())?;
        
        // Arrow IPC
        let arrow_path = self.output_dir.join(format!("{}.arrow", base_name));
        println!("  🏹 Saving Arrow IPC...");
        IpcWriter::new(std::fs::File::create(&arrow_path)?)
            .finish(&mut combined.clone())?;
        
        println!("  ✅ Completed LMP year {} - {} records", year, combined.height());
        Ok(())
    }
}

pub fn process_all_lmp_historical() -> Result<()> {
    let processor = LmpFullProcessor::new()?;
    processor.extract_all_and_process()?;
    Ok(())
}