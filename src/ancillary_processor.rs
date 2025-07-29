use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct AncillaryProcessor {
    base_dir: PathBuf,
    output_dir: PathBuf,
}

impl AncillaryProcessor {
    pub fn new(base_dir: PathBuf, output_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&output_dir).unwrap();
        
        Self {
            base_dir,
            output_dir,
        }
    }

    pub fn process_all_ancillary_services(&self) -> Result<()> {
        println!("\n⚡ Processing Ancillary Services Data");
        println!("{}", "=".repeat(60));
        
        // List of ancillary services directories to process
        let ancillary_dirs = vec![
            "DAM_Ancillary_Service_Plan",
            "Total_Ancillary_Service_Offers",
            "Total_Ancillary_Service_Procured_in_SASM",
            "QSE_Ancillary_Services_Capacity_Monitor",
            "SASM_MCPC_by_Ancillary_Service_Type",
            "Aggregated_Ancillary_Service_Offer_Curve",
            "SASM_Aggregated_Ancillary_Service_Offer_Curve",
        ];
        
        for dir_name in ancillary_dirs {
            let dir_path = self.base_dir.join(dir_name);
            if dir_path.exists() {
                println!("\n📁 Processing {}", dir_name);
                self.process_ancillary_directory(&dir_path, dir_name)?;
            } else {
                println!("⚠️  Directory not found: {}", dir_name);
            }
        }
        
        Ok(())
    }

    fn process_ancillary_directory(&self, dir_path: &Path, dir_name: &str) -> Result<()> {
        // Check for CSV files
        let csv_pattern = dir_path.join("*.csv");
        let mut csv_files: Vec<PathBuf> = glob(csv_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        // Check for ZIP files if no CSVs found
        if csv_files.is_empty() {
            let zip_pattern = dir_path.join("*.zip");
            let zip_files: Vec<PathBuf> = glob(zip_pattern.to_str().unwrap())?
                .filter_map(Result::ok)
                .collect();
            
            if !zip_files.is_empty() {
                println!("  🗜️  Found {} ZIP files, extracting...", zip_files.len());
                csv_files = self.extract_zips_to_csv(&zip_files, dir_name)?;
            }
        }
        
        if csv_files.is_empty() {
            println!("  ❌ No data files found");
            return Ok(());
        }
        
        println!("  📊 Found {} files to process", csv_files.len());
        
        // Group files by year
        let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
        
        for csv_path in csv_files {
            let filename = csv_path.file_stem().unwrap().to_str().unwrap();
            
            // Try different date patterns
            let year = self.extract_year_from_filename(filename);
            
            if let Some(year) = year {
                files_by_year.entry(year).or_insert_with(Vec::new).push(csv_path);
            }
        }
        
        // Process each year
        for (year, year_files) in files_by_year {
            self.process_year_ancillary_files(year, &year_files, dir_name)?;
        }
        
        Ok(())
    }

    fn extract_zips_to_csv(&self, zip_files: &[PathBuf], dir_name: &str) -> Result<Vec<PathBuf>> {
        let extract_dir = self.output_dir.join(format!("{}_extracted", dir_name));
        std::fs::create_dir_all(&extract_dir)?;
        
        let mut extracted_files = Vec::new();
        
        for zip_path in zip_files {
            let file = std::fs::File::open(zip_path)?;
            if let Ok(mut archive) = ::zip::ZipArchive::new(file) {
                for i in 0..archive.len() {
                    if let Ok(mut file) = archive.by_index(i) {
                        if file.name().ends_with(".csv") {
                            let output_name = format!("{}_{}",
                                zip_path.file_stem().unwrap().to_str().unwrap(),
                                file.name()
                            );
                            let output_path = extract_dir.join(output_name);
                            
                            let mut output_file = std::fs::File::create(&output_path)?;
                            std::io::copy(&mut file, &mut output_file)?;
                            extracted_files.push(output_path);
                        }
                    }
                }
            }
        }
        
        Ok(extracted_files)
    }

    fn extract_year_from_filename(&self, filename: &str) -> Option<u16> {
        // Try patterns like .20240430. or _2024
        if let Some(pos) = filename.find(".202") {
            if let Some(year_str) = filename.get(pos + 1..pos + 5) {
                if let Ok(year) = year_str.parse::<u16>() {
                    return Some(year);
                }
            }
        }
        
        // Try pattern _YYYY
        if let Some(pos) = filename.rfind("_") {
            if let Some(year_str) = filename.get(pos + 1..pos + 5) {
                if let Ok(year) = year_str.parse::<u16>() {
                    if year >= 2000 && year <= 2100 {
                        return Some(year);
                    }
                }
            }
        }
        
        None
    }

    fn process_year_ancillary_files(&self, year: u16, files: &[PathBuf], service_type: &str) -> Result<()> {
        println!("\n  📅 Processing {} year {}: {} files", service_type, year, files.len());
        
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        // Process files in parallel
        let all_dfs: Vec<DataFrame> = files
            .par_iter()
            .filter_map(|file| {
                pb.inc(1);
                
                CsvReader::new(std::fs::File::open(file).ok()?)
                    .has_header(true)
                    .finish()
                    .ok()
            })
            .collect();
        
        pb.finish();
        
        if all_dfs.is_empty() {
            println!("    ❌ No valid data");
            return Ok(());
        }
        
        println!("    📊 Combining {} dataframes...", all_dfs.len());
        
        // Concatenate
        let combined = concat(
            all_dfs.iter().map(|df| df.clone().lazy()).collect::<Vec<_>>().as_slice(),
            UnionArgs::default(),
        )?.collect()?;
        
        // Save files
        let base_name = format!("{}_{}", service_type, year);
        
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
        
        println!("    ✅ Saved: {} records", combined.height());
        Ok(())
    }
}

pub fn process_all_ancillary_data() -> Result<()> {
    let base_dir = PathBuf::from("/Users/enrico/data/ERCOT_data");
    let output_dir = PathBuf::from("ancillary_annual_data");
    
    let processor = AncillaryProcessor::new(base_dir, output_dir);
    processor.process_all_ancillary_services()?;
    
    Ok(())
}