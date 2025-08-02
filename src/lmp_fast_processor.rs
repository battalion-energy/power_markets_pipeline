use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

pub struct LmpFastProcessor {
    underscores_dir: PathBuf,
    csv_dir: PathBuf,
    output_dir: PathBuf,
}

impl LmpFastProcessor {
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

    pub fn process_existing_csv_files(&self) -> Result<()> {
        println!("🚀 Fast LMP Processing - Using Existing CSV Files");
        println!("{}", "=".repeat(60));
        
        // Just process the CSV files that are already extracted
        self.create_annual_rollups()?;
        
        Ok(())
    }

    pub fn extract_sample_and_process(&self, sample_size: usize) -> Result<()> {
        println!("🚀 LMP Sample Processing - {} files", sample_size);
        println!("{}", "=".repeat(60));
        
        // Process a sample of ZIP files first
        self.extract_sample_zip_files(sample_size)?;
        
        // Then create annual rollups
        self.create_annual_rollups()?;
        
        Ok(())
    }

    fn extract_sample_zip_files(&self, sample_size: usize) -> Result<()> {
        println!("🗜️  Extracting sample of {} ZIP files...", sample_size);
        
        // Find all ZIP files
        let pattern = self.underscores_dir.join("*.zip");
        let mut zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        // Take a sample
        zip_files.truncate(sample_size);
        println!("Processing {} ZIP files", zip_files.len());
        
        // Get existing CSV files to avoid re-extraction
        let csv_pattern = self.csv_dir.join("*.csv");
        let existing_csvs: HashSet<String> = glob(csv_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(String::from))
            .collect();
        
        println!("Found {} existing CSV files", existing_csvs.len());
        
        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} - {msg}")
            .unwrap());
        
        let extracted_count = zip_files
            .par_iter()
            .map(|zip_path| {
                let zip_stem = zip_path.file_stem().unwrap().to_str().unwrap();
                
                // Skip if we already have CSVs for this zip
                let has_csv = existing_csvs.iter().any(|csv| csv.contains(zip_stem));
                if has_csv {
                    return 0;
                }
                
                // Extract the ZIP file
                if let Ok(file) = std::fs::File::open(zip_path) {
                    if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                        let mut extracted = 0;
                        
                        for i in 0..archive.len() {
                            if let Ok(mut zip_file) = archive.by_index(i) {
                                if zip_file.name().ends_with(".csv") {
                                    let csv_name = format!("{}_{}", zip_stem, zip_file.name());
                                    let csv_path = self.csv_dir.join(csv_name);
                                    
                                    if let Ok(mut output) = std::fs::File::create(&csv_path) {
                                        let _ = std::io::copy(&mut zip_file, &mut output);
                                        extracted += 1;
                                    }
                                }
                            }
                        }
                        
                        return extracted;
                    }
                }
                
                0
            })
            .sum::<usize>();
        
        pb.finish_with_message(format!("Extracted {} CSV files", extracted_count));
        Ok(())
    }

    fn create_annual_rollups(&self) -> Result<()> {
        println!("\n📊 Creating annual rollups from CSV files...");
        
        // Find all CSV files
        let pattern = self.csv_dir.join("*.csv");
        let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} CSV files to process", csv_files.len());
        
        if csv_files.is_empty() {
            println!("❌ No CSV files found");
            return Ok(());
        }
        
        // Group files by year
        let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
        
        for csv_path in csv_files {
            let filename = csv_path.file_stem().unwrap().to_str().unwrap();
            
            // Extract year from filename (look for pattern like .20240825.)
            if let Some(year) = self.extract_year_from_filename(filename) {
                files_by_year.entry(year).or_insert_with(Vec::new).push(csv_path);
            }
        }
        
        let mut years: Vec<u16> = files_by_year.keys().cloned().collect();
        years.sort();
        println!("Years found: {:?}", years);
        
        // Process each year
        for (year, year_files) in files_by_year {
            self.process_year_lmp_files(year, &year_files)?;
        }
        
        Ok(())
    }

    fn extract_year_from_filename(&self, filename: &str) -> Option<u16> {
        // Look for patterns like .20240825. (8-digit date)
        if let Some(pos) = filename.find(".202") {
            if let Some(year_str) = filename.get(pos + 1..pos + 5) {
                if let Ok(year) = year_str.parse::<u16>() {
                    if year >= 2010 && year <= 2030 {
                        return Some(year);
                    }
                }
            }
        }
        
        // Look for patterns like _20240825_ 
        if let Some(pos) = filename.find("_202") {
            if let Some(year_str) = filename.get(pos + 1..pos + 5) {
                if let Ok(year) = year_str.parse::<u16>() {
                    if year >= 2010 && year <= 2030 {
                        return Some(year);
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
                    
                    // Try to read CSV
                    CsvReader::new(std::fs::File::open(file).ok()?)
                        .has_header(true)
                        .finish()
                        .ok()
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
        
        println!("  ✅ Completed LMP year {}", year);
        Ok(())
    }
}

pub fn process_existing_lmp_csv() -> Result<()> {
    let processor = LmpFastProcessor::new()?;
    processor.process_existing_csv_files()?;
    Ok(())
}

pub fn process_lmp_sample(sample_size: usize) -> Result<()> {
    let processor = LmpFastProcessor::new()?;
    processor.extract_sample_and_process(sample_size)?;
    Ok(())
}