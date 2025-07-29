use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::io::{Read, Write};

pub struct DamProcessor {
    data_dir: PathBuf,
    output_dir: PathBuf,
    extracted_dir: PathBuf,
}

impl DamProcessor {
    pub fn new(data_dir: PathBuf, output_dir: PathBuf) -> Self {
        let extracted_dir = output_dir.join("extracted_csv");
        std::fs::create_dir_all(&extracted_dir).unwrap();
        std::fs::create_dir_all(&output_dir).unwrap();
        
        Self {
            data_dir,
            output_dir,
            extracted_dir,
        }
    }

    pub fn process_dam_settlement_prices(&self) -> Result<()> {
        println!("\n📅 Processing DAM Settlement Point Prices");
        println!("{}", "=".repeat(60));
        
        // Step 1: Extract any unextracted zip files
        self.extract_new_zip_files()?;
        
        // Step 2: Process extracted CSV files into annual rollups
        self.create_annual_rollups()?;
        
        Ok(())
    }

    fn extract_new_zip_files(&self) -> Result<()> {
        println!("🔍 Checking for new ZIP files to extract...");
        
        // Find all zip files
        let pattern = self.data_dir.join("*.zip");
        let zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        // Get already extracted files
        let extracted_pattern = self.extracted_dir.join("*.csv");
        let extracted_files: HashSet<String> = glob(extracted_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(String::from))
            .collect();
        
        let mut new_zips = 0;
        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({msg})")
            .unwrap());
        
        for zip_path in zip_files {
            pb.inc(1);
            
            let stem = zip_path.file_stem().unwrap().to_str().unwrap();
            
            // Skip if already extracted
            if extracted_files.contains(stem) {
                continue;
            }
            
            pb.set_message(format!("Extracting {}", zip_path.file_name().unwrap().to_str().unwrap()));
            
            // Extract ZIP
            let file = std::fs::File::open(&zip_path)?;
            let mut archive = ::zip::ZipArchive::new(file)?;
            
            for i in 0..archive.len() {
                let mut file = archive.by_index(i)?;
                if file.name().ends_with(".csv") {
                    let output_path = self.extracted_dir.join(format!("{}.csv", stem));
                    let mut output_file = std::fs::File::create(&output_path)?;
                    std::io::copy(&mut file, &mut output_file)?;
                    new_zips += 1;
                    break;
                }
            }
        }
        
        pb.finish_with_message(format!("Extracted {} new files", new_zips));
        Ok(())
    }

    fn create_annual_rollups(&self) -> Result<()> {
        println!("\n📊 Creating annual rollups...");
        
        // Find all extracted CSV files
        let pattern = self.extracted_dir.join("*.csv");
        let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} CSV files to process", csv_files.len());
        
        // Group files by year
        let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
        
        for csv_path in csv_files {
            let filename = csv_path.file_stem().unwrap().to_str().unwrap();
            
            // Extract year from filename (looking for patterns like .20240430.)
            if let Some(date_start) = filename.find(".202") {
                if let Some(year_str) = filename.get(date_start + 1..date_start + 5) {
                    if let Ok(year) = year_str.parse::<u16>() {
                        files_by_year.entry(year).or_insert_with(Vec::new).push(csv_path);
                    }
                }
            }
        }
        
        // Process each year
        for (year, year_files) in files_by_year {
            self.process_year_dam_files(year, &year_files)?;
        }
        
        Ok(())
    }

    fn process_year_dam_files(&self, year: u16, files: &[PathBuf]) -> Result<()> {
        println!("\n📅 Processing DAM year {}: {} files", year, files.len());
        
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap());
        
        // Process files in parallel batches
        let batch_size = 50;
        let mut all_dfs = Vec::new();
        
        for chunk in files.chunks(batch_size) {
            let chunk_dfs: Vec<DataFrame> = chunk
                .par_iter()
                .filter_map(|file| {
                    pb.inc(1);
                    
                    // Read CSV
                    let df = CsvReader::new(std::fs::File::open(file).ok()?)
                        .has_header(true)
                        .finish()
                        .ok()?;
                    
                    // Basic validation
                    let cols = df.get_column_names();
                    if cols.is_empty() {
                        return None;
                    }
                    
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
        let combined = concat(
            all_dfs.iter().map(|df| df.clone().lazy()).collect::<Vec<_>>().as_slice(),
            UnionArgs::default(),
        )?.collect()?;
        
        println!("  📊 Combined records: {}", combined.height());
        
        // Sort by timestamp if available
        let sorted = if combined.get_column_names().contains(&"DeliveryDate") {
            combined.lazy()
                .sort("DeliveryDate", Default::default())
                .collect()?
        } else {
            combined
        };
        
        // Save files
        let base_name = format!("DAM_Settlement_Point_Prices_{}", year);
        
        // CSV
        let csv_path = self.output_dir.join(format!("{}.csv", base_name));
        println!("  💾 Saving CSV...");
        CsvWriter::new(std::fs::File::create(&csv_path)?)
            .finish(&mut sorted.clone())?;
        
        // Parquet
        let parquet_path = self.output_dir.join(format!("{}.parquet", base_name));
        println!("  📦 Saving Parquet...");
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut sorted.clone())?;
        
        // Arrow IPC
        let arrow_path = self.output_dir.join(format!("{}.arrow", base_name));
        println!("  🏹 Saving Arrow IPC...");
        IpcWriter::new(std::fs::File::create(&arrow_path)?)
            .finish(&mut sorted.clone())?;
        
        println!("  ✅ Completed DAM year {}", year);
        Ok(())
    }
}

pub fn process_all_dam_data() -> Result<()> {
    let data_dir = PathBuf::from("/Users/enrico/data/ERCOT_data/DAM_Settlement_Point_Prices");
    let output_dir = PathBuf::from("dam_annual_data");
    
    let processor = DamProcessor::new(data_dir, output_dir);
    processor.process_dam_settlement_prices()?;
    
    Ok(())
}