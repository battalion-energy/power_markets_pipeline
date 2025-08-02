use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct DisclosureProcessor {
    base_dir: PathBuf,
    output_dir: PathBuf,
}

impl DisclosureProcessor {
    pub fn new(base_dir: PathBuf, output_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&output_dir)?;
        Ok(Self { base_dir, output_dir })
    }

    pub fn process_all_60_day_disclosures(&self) -> Result<()> {
        println!("📊 Processing 60-Day Disclosure Reports");
        println!("{}", "=".repeat(80));
        
        let disclosure_folders = vec![
            ("60-Day_SCED_Disclosure_Reports", "SCED"),
            ("60-Day_DAM_Disclosure_Reports", "DAM"),
            ("60-Day_COP_Adjustment_Period_Snapshot", "COP_Snapshot"),
            ("60-Day_COP_All_Updates", "COP_Updates"),
            ("60-Day_SASM_Disclosure_Reports", "SASM"),
        ];
        
        for (folder_name, report_type) in disclosure_folders {
            let folder_path = self.base_dir.join(folder_name);
            if folder_path.exists() {
                println!("\n📁 Processing {}", folder_name);
                self.process_disclosure_folder(&folder_path, report_type)?;
            } else {
                println!("⚠️  Folder not found: {}", folder_name);
            }
        }
        
        Ok(())
    }

    fn process_disclosure_folder(&self, folder_path: &Path, report_type: &str) -> Result<()> {
        // Extract all zip files
        let extracted_dir = self.output_dir.join(format!("{}_extracted", report_type));
        std::fs::create_dir_all(&extracted_dir)?;
        
        self.extract_all_zips(folder_path, &extracted_dir)?;
        
        // Process by year
        self.process_extracted_files(&extracted_dir, report_type)?;
        
        Ok(())
    }

    fn extract_all_zips(&self, source_dir: &Path, extract_dir: &Path) -> Result<()> {
        let pattern = source_dir.join("*.zip");
        let zip_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("  🗜️  Found {} ZIP files to extract", zip_files.len());
        
        // Check if already extracted
        let csv_pattern = extract_dir.join("*.csv");
        let existing_csv_count = glob(csv_pattern.to_str().unwrap())?.count();
        if existing_csv_count > 0 {
            println!("  ✅ Already extracted {} CSV files, skipping extraction", existing_csv_count);
            return Ok(());
        }
        
        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} - {msg}")
            .unwrap());
        
        // Process in smaller batches to avoid timeout
        let batch_size = 50;
        let mut total_extracted = 0;
        
        for (batch_idx, chunk) in zip_files.chunks(batch_size).enumerate() {
            pb.set_message(format!("Batch {} of {}", batch_idx + 1, (zip_files.len() + batch_size - 1) / batch_size));
            
            let batch_count: usize = chunk
                .par_iter()
                .map(|zip_path| {
                    pb.inc(1);
                    let mut count = 0;
                    
                    if let Ok(file) = std::fs::File::open(zip_path) {
                        if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                            for i in 0..archive.len() {
                                if let Ok(mut zip_file) = archive.by_index(i) {
                                    let outpath = extract_dir.join(zip_file.name());
                                    
                                    if let Some(parent) = outpath.parent() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                    
                                    if zip_file.name().ends_with(".csv") {
                                        if let Ok(mut outfile) = std::fs::File::create(&outpath) {
                                            if std::io::copy(&mut zip_file, &mut outfile).is_ok() {
                                                count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    count
                })
                .sum();
            
            total_extracted += batch_count;
            pb.set_message(format!("Extracted {} files so far", total_extracted));
        }
        
        pb.finish_with_message(format!("Extracted {} CSV files total", total_extracted));
        Ok(())
    }

    fn process_extracted_files(&self, extract_dir: &Path, report_type: &str) -> Result<()> {
        let pattern = extract_dir.join("**/*.csv");
        let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        if csv_files.is_empty() {
            println!("  ❌ No CSV files found");
            return Ok(());
        }
        
        println!("  📊 Found {} CSV files to process", csv_files.len());
        
        // Group by year
        let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
        
        for csv_path in csv_files {
            if let Some(year) = self.extract_year_from_path(&csv_path) {
                files_by_year.entry(year).or_insert_with(Vec::new).push(csv_path);
            }
        }
        
        // Process each year
        for (year, year_files) in files_by_year {
            self.process_year_files(year, &year_files, report_type)?;
        }
        
        Ok(())
    }

    fn extract_year_from_path(&self, path: &Path) -> Option<u16> {
        let path_str = path.to_string_lossy();
        let filename = path.file_name()?.to_str()?;
        
        // Look for YYYYMMDD pattern in filename
        let re = regex::Regex::new(r"(\d{8})").ok()?;
        if let Some(captures) = re.captures(filename) {
            if let Some(date_str) = captures.get(1) {
                let year_str = &date_str.as_str()[0..4];
                if let Ok(year) = year_str.parse::<u16>() {
                    if year >= 2010 && year <= 2025 {
                        return Some(year);
                    }
                }
            }
        }
        
        // Fallback: Look for patterns like 20XX anywhere in path
        for year in (2010..=2025).rev() {
            if path_str.contains(&year.to_string()) {
                return Some(year);
            }
        }
        
        None
    }

    fn process_year_files(&self, year: u16, files: &[PathBuf], report_type: &str) -> Result<()> {
        println!("    📅 Processing {} year {}: {} files", report_type, year, files.len());
        
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        let mut all_dfs = Vec::new();
        let batch_size = 100;
        
        for chunk in files.chunks(batch_size) {
            let chunk_dfs: Vec<DataFrame> = chunk
                .par_iter()
                .filter_map(|file| {
                    pb.inc(1);
                    CsvReader::new(std::fs::File::open(file).ok()?)
                        .has_header(true)
                        .finish()
                        .ok()
                })
                .collect();
            
            all_dfs.extend(chunk_dfs);
        }
        
        pb.finish();
        
        if all_dfs.is_empty() {
            println!("      ❌ No valid data");
            return Ok(());
        }
        
        // Concatenate all dataframes
        let combined = concat(
            all_dfs.iter().map(|df| df.clone().lazy()).collect::<Vec<_>>().as_slice(),
            UnionArgs::default(),
        )?.collect()?;
        
        // Save output files
        let base_name = format!("60_Day_{}_Disclosure_{}", report_type, year);
        
        // CSV
        let csv_path = self.output_dir.join(format!("{}.csv", base_name));
        CsvWriter::new(std::fs::File::create(&csv_path)?)
            .finish(&mut combined.clone())?;
        
        // Parquet
        let parquet_path = self.output_dir.join(format!("{}.parquet", base_name));
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut combined.clone())?;
        
        // Arrow
        let arrow_path = self.output_dir.join(format!("{}.arrow", base_name));
        IpcWriter::new(std::fs::File::create(&arrow_path)?)
            .finish(&mut combined.clone())?;
        
        println!("      ✅ Saved {} records", combined.height());
        Ok(())
    }
}

pub fn process_all_disclosures() -> Result<()> {
    let base_dir = PathBuf::from("/Users/enrico/data/ERCOT_data");
    let output_dir = PathBuf::from("disclosure_data");
    
    let processor = DisclosureProcessor::new(base_dir, output_dir)?;
    processor.process_all_60_day_disclosures()?;
    
    Ok(())
}