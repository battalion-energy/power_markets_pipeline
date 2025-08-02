use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::io::Read;

// Commenting out unused enum - may be used in future
// pub enum DataType {
//     RealtimeSettlement,
//     DayAheadSettlement,
//     HistoricalDAM,
//     HistoricalRTM,
//     AncillaryServices,
// }

pub struct ErcotProcessor {
    output_dir: PathBuf,
}

impl ErcotProcessor {
    pub fn new(output_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&output_dir).unwrap();
        Self { output_dir }
    }

    pub fn process_historical_dam(&self, data_dir: &Path) -> Result<()> {
        println!("\n🏛️  Processing Historical DAM Load Zone and Hub Prices");
        println!("{}", "=".repeat(60));
        
        let pattern = data_dir.join("*.zip");
        let zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} historical DAM files", zip_files.len());
        
        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap());
        
        for zip_path in zip_files {
            pb.inc(1);
            
            // Extract year from filename
            let filename = zip_path.file_stem().unwrap().to_str().unwrap();
            let year = if let Some(year_pos) = filename.rfind("_") {
                filename[year_pos+1..].parse::<u16>().ok()
            } else {
                None
            };
            
            if let Some(year) = year {
                let output_path = self.output_dir.join(format!("Historical_DAM_Prices_{}.parquet", year));
                
                // Process zip file
                let file = std::fs::File::open(&zip_path)?;
                let mut archive = ::zip::ZipArchive::new(file)?;
                
                for i in 0..archive.len() {
                    let mut file = archive.by_index(i)?;
                    if file.name().ends_with(".csv") {
                        let mut contents = String::new();
                        file.read_to_string(&mut contents)?;
                        
                        // Process CSV data
                        let df = CsvReader::new(std::io::Cursor::new(contents.as_bytes()))
                            .has_header(true)
                            .finish()?;
                        
                        // Save as parquet
                        ParquetWriter::new(std::fs::File::create(&output_path)?)
                            .finish(&mut df.clone())?;
                        
                        break;
                    }
                }
            }
        }
        
        pb.finish_with_message("Historical DAM processing complete");
        Ok(())
    }

    pub fn process_historical_rtm(&self, data_dir: &Path) -> Result<()> {
        println!("\n🏛️  Processing Historical RTM Load Zone and Hub Prices");
        println!("{}", "=".repeat(60));
        
        let pattern = data_dir.join("*.zip");
        let zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} historical RTM files", zip_files.len());
        
        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap());
        
        for zip_path in zip_files {
            pb.inc(1);
            
            // Extract year from filename
            let filename = zip_path.file_stem().unwrap().to_str().unwrap();
            let year = if let Some(year_pos) = filename.rfind("_") {
                filename[year_pos+1..].parse::<u16>().ok()
            } else {
                None
            };
            
            if let Some(year) = year {
                let output_path = self.output_dir.join(format!("Historical_RTM_Prices_{}.parquet", year));
                
                // Process zip file
                let file = std::fs::File::open(&zip_path)?;
                let mut archive = ::zip::ZipArchive::new(file)?;
                
                for i in 0..archive.len() {
                    let mut file = archive.by_index(i)?;
                    if file.name().ends_with(".csv") {
                        let mut contents = String::new();
                        file.read_to_string(&mut contents)?;
                        
                        // Process CSV data
                        let df = CsvReader::new(std::io::Cursor::new(contents.as_bytes()))
                            .has_header(true)
                            .finish()?;
                        
                        // Save as parquet
                        ParquetWriter::new(std::fs::File::create(&output_path)?)
                            .finish(&mut df.clone())?;
                        
                        break;
                    }
                }
            }
        }
        
        pb.finish_with_message("Historical RTM processing complete");
        Ok(())
    }

    pub fn process_daily_dam(&self, data_dir: &Path) -> Result<()> {
        println!("\n📅 Processing Daily DAM Settlement Point Prices");
        println!("{}", "=".repeat(60));
        
        // Find all CSV files (unzipped)
        let pattern = data_dir.join("*.csv");
        let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} DAM CSV files", csv_files.len());
        
        if csv_files.is_empty() {
            // Try processing zip files
            let zip_pattern = data_dir.join("*.zip");
            let zip_files: Vec<PathBuf> = glob(zip_pattern.to_str().unwrap())?
                .filter_map(Result::ok)
                .collect();
            
            println!("Found {} DAM ZIP files to process", zip_files.len());
            
            // Group by year
            let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
            
            for zip_path in zip_files {
                let filename = zip_path.file_stem().unwrap().to_str().unwrap();
                // Extract year from filename pattern like .20240430.
                if let Some(date_start) = filename.find(".202") {
                    if let Some(year_str) = filename.get(date_start + 1..date_start + 5) {
                        if let Ok(year) = year_str.parse::<u16>() {
                            files_by_year.entry(year).or_insert_with(Vec::new).push(zip_path);
                        }
                    }
                }
            }
            
            for (year, year_files) in files_by_year {
                println!("\n📅 Processing DAM year {}: {} files", year, year_files.len());
                
                let pb = ProgressBar::new(year_files.len() as u64);
                pb.set_style(ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                    .unwrap());
                
                let mut all_dfs = Vec::new();
                
                for zip_path in year_files {
                    pb.inc(1);
                    
                    // Extract and process ZIP
                    if let Ok(file) = std::fs::File::open(&zip_path) {
                        if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                            for i in 0..archive.len() {
                                if let Ok(mut file) = archive.by_index(i) {
                                    if file.name().ends_with(".csv") {
                                        let mut contents = String::new();
                                        if file.read_to_string(&mut contents).is_ok() {
                                            if let Ok(df) = CsvReader::new(std::io::Cursor::new(contents.as_bytes()))
                                                .has_header(true)
                                                .finish() {
                                                all_dfs.push(df);
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                
                pb.finish();
                
                if !all_dfs.is_empty() {
                    println!("  📊 Combining {} DAM dataframes...", all_dfs.len());
                    
                    // Concatenate all dataframes
                    let combined = concat(
                        all_dfs.iter().map(|df| df.clone().lazy()).collect::<Vec<_>>().as_slice(),
                        UnionArgs::default(),
                    )?.collect()?;
                    
                    // Save as parquet
                    let output_path = self.output_dir.join(format!("DAM_Settlement_Point_Prices_{}.parquet", year));
                    ParquetWriter::new(std::fs::File::create(&output_path)?)
                        .finish(&mut combined.clone())?;
                    
                    println!("  ✅ Saved DAM data for year {}", year);
                }
            }
        }
        
        Ok(())
    }
}