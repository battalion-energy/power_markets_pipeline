use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct LmpProcessor {
    spaces_dir: PathBuf,
    underscores_dir: PathBuf,
    csv_dir: PathBuf,
    output_dir: PathBuf,
}

impl LmpProcessor {
    pub fn new() -> Result<Self> {
        let spaces_dir = PathBuf::from("/Users/enrico/data/ERCOT_data/LMPs by Resource Nodes, Load Zones and Trading Hubs");
        let underscores_dir = PathBuf::from("/Users/enrico/data/ERCOT_data/LMPs_by_Resource_Nodes,_Load_Zones_and_Trading_Hubs");
        let csv_dir = underscores_dir.join("csv");
        let output_dir = PathBuf::from("lmp_annual_data");
        
        // Create directories
        std::fs::create_dir_all(&underscores_dir)?;
        std::fs::create_dir_all(&csv_dir)?;
        std::fs::create_dir_all(&output_dir)?;
        
        Ok(Self {
            spaces_dir,
            underscores_dir,
            csv_dir,
            output_dir,
        })
    }

    pub fn process_all_lmp_data(&self) -> Result<()> {
        println!("🏗️  LMP Data Processing Pipeline");
        println!("{}", "=".repeat(60));
        
        // Step 1: Move files from spaces folder to underscores folder
        self.move_files_to_underscores_folder()?;
        
        // Step 2: Extract nested zip files and organize CSV files
        self.extract_and_organize_csv_files()?;
        
        // Step 3: Create annual rollups
        self.create_annual_rollups()?;
        
        Ok(())
    }

    fn move_files_to_underscores_folder(&self) -> Result<()> {
        if !self.spaces_dir.exists() {
            println!("📁 Spaces folder does not exist, skipping move step");
            return Ok(());
        }
        
        println!("📦 Moving files from spaces folder to underscores folder...");
        
        // Find all files in spaces folder
        let pattern = self.spaces_dir.join("*.zip");
        let space_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        if space_files.is_empty() {
            println!("  ✅ No files to move");
            return Ok(());
        }
        
        let pb = ProgressBar::new(space_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({msg})")
            .unwrap());
        
        let mut moved_count = 0;
        for file in space_files {
            pb.inc(1);
            let filename = file.file_name().unwrap();
            let dest = self.underscores_dir.join(filename);
            
            if !dest.exists() {
                std::fs::rename(&file, &dest)?;
                moved_count += 1;
                pb.set_message(format!("Moved {}", filename.to_str().unwrap()));
            }
        }
        
        pb.finish_with_message(format!("Moved {} files", moved_count));
        Ok(())
    }

    fn extract_and_organize_csv_files(&self) -> Result<()> {
        println!("\n🗜️  Extracting nested ZIP files and organizing CSV files...");
        
        // Find all ZIP files in underscores directory
        let pattern = self.underscores_dir.join("*.zip");
        let zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} ZIP files to process", zip_files.len());
        
        // Get existing CSV files to avoid re-extraction
        let csv_pattern = self.csv_dir.join("*.csv");
        let existing_csvs: HashSet<String> = glob(csv_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(String::from))
            .collect();
        
        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({msg})")
            .unwrap());
        
        let mut extracted_count = 0;
        
        for zip_path in zip_files {
            pb.inc(1);
            let zip_stem = zip_path.file_stem().unwrap().to_str().unwrap();
            pb.set_message(format!("Processing {}", zip_stem));
            
            // Skip if we already have CSVs for this zip
            let has_csv = existing_csvs.iter().any(|csv| csv.contains(zip_stem));
            if has_csv {
                continue;
            }
            
            // Extract the main ZIP file
            let temp_extract_dir = self.underscores_dir.join(format!("temp_{}", zip_stem));
            std::fs::create_dir_all(&temp_extract_dir)?;
            
            if let Ok(file) = std::fs::File::open(&zip_path) {
                if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                    // Extract all files from the main ZIP
                    for i in 0..archive.len() {
                        if let Ok(mut file) = archive.by_index(i) {
                            let outpath = temp_extract_dir.join(file.name());
                            
                            if let Some(parent) = outpath.parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            
                            if let Ok(mut outfile) = std::fs::File::create(&outpath) {
                                let _ = std::io::copy(&mut file, &mut outfile);
                            }
                        }
                    }
                    
                    // Now look for nested ZIP files and extract them
                    self.extract_nested_zips(&temp_extract_dir)?;
                    
                    // Find all CSV files in the extracted directory and move them to csv/
                    self.collect_csv_files(&temp_extract_dir, zip_stem)?;
                    
                    extracted_count += 1;
                }
            }
            
            // Clean up temp directory
            let _ = std::fs::remove_dir_all(&temp_extract_dir);
        }
        
        pb.finish_with_message(format!("Extracted {} ZIP files", extracted_count));
        Ok(())
    }

    fn extract_nested_zips(&self, dir: &Path) -> Result<()> {
        // Find all ZIP files in the directory
        let pattern = dir.join("**/*.zip");
        let nested_zips: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        for zip_path in nested_zips {
            let extract_dir = zip_path.parent().unwrap();
            
            if let Ok(file) = std::fs::File::open(&zip_path) {
                if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                    for i in 0..archive.len() {
                        if let Ok(mut file) = archive.by_index(i) {
                            let outpath = extract_dir.join(file.name());
                            
                            if let Some(parent) = outpath.parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            
                            if let Ok(mut outfile) = std::fs::File::create(&outpath) {
                                let _ = std::io::copy(&mut file, &mut outfile);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    fn collect_csv_files(&self, extract_dir: &Path, zip_stem: &str) -> Result<()> {
        // Find all CSV files recursively
        let pattern = extract_dir.join("**/*.csv");
        let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        for (i, csv_path) in csv_files.iter().enumerate() {
            let new_name = if csv_files.len() == 1 {
                format!("{}.csv", zip_stem)
            } else {
                format!("{}_{}.csv", zip_stem, i)
            };
            
            let dest_path = self.csv_dir.join(new_name);
            let _ = std::fs::copy(csv_path, dest_path);
        }
        
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
        
        println!("Years found: {:?}", {
            let mut years: Vec<u16> = files_by_year.keys().cloned().collect();
            years.sort();
            years
        });
        
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

pub fn process_all_lmp_data() -> Result<()> {
    let processor = LmpProcessor::new()?;
    processor.process_all_lmp_data()?;
    Ok(())
}