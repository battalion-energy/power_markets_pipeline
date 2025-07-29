use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct DisclosureFastProcessor {
    output_dir: PathBuf,
}

impl DisclosureFastProcessor {
    pub fn new(output_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&output_dir)?;
        Ok(Self { output_dir })
    }

    pub fn process_extracted_disclosures(&self) -> Result<()> {
        println!("📊 Fast Processing 60-Day Disclosure Reports (from extracted CSVs)");
        println!("{}", "=".repeat(80));
        
        let disclosure_dirs = vec![
            ("disclosure_data/SCED_extracted", "SCED"),
            ("disclosure_data/DAM_extracted", "DAM"),
            ("disclosure_data/COP_Snapshot_extracted", "COP_Snapshot"),
            ("disclosure_data/COP_Updates_extracted", "COP_Updates"),
            ("disclosure_data/SASM_extracted", "SASM"),
        ];
        
        for (dir_path, report_type) in disclosure_dirs {
            let extracted_dir = PathBuf::from(dir_path);
            if extracted_dir.exists() {
                println!("\n📁 Processing {} extracted files", report_type);
                self.process_report_type(&extracted_dir, report_type)?;
            } else {
                println!("⚠️  Directory not found: {}", dir_path);
            }
        }
        
        Ok(())
    }

    fn process_report_type(&self, extract_dir: &Path, report_type: &str) -> Result<()> {
        // Find all CSV files
        let pattern = extract_dir.join("*.csv");
        let csv_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        if csv_files.is_empty() {
            println!("  ❌ No CSV files found");
            return Ok(());
        }
        
        println!("  📊 Found {} CSV files to process", csv_files.len());
        
        // Sample first file to understand structure
        if let Some(first_file) = csv_files.first() {
            self.analyze_file_structure(first_file, report_type)?;
        }
        
        // Group by year
        let mut files_by_year: HashMap<u16, Vec<PathBuf>> = HashMap::new();
        
        for csv_path in &csv_files {
            if let Some(year) = self.extract_year_from_filename(csv_path) {
                files_by_year.entry(year).or_insert_with(Vec::new).push(csv_path.clone());
            }
        }
        
        let mut years: Vec<u16> = files_by_year.keys().cloned().collect();
        years.sort();
        println!("  Years found: {:?}", years);
        
        // Process each year
        for (year, year_files) in files_by_year {
            self.process_year_files(year, &year_files, report_type)?;
        }
        
        Ok(())
    }

    fn analyze_file_structure(&self, file_path: &Path, report_type: &str) -> Result<()> {
        println!("  🔍 Analyzing {} file structure...", report_type);
        
        let df = CsvReader::new(std::fs::File::open(file_path)?)
            .has_header(true)
            .finish()?;
        
        println!("    Columns: {:?}", df.get_column_names());
        println!("    Shape: {} rows x {} columns", df.height(), df.width());
        
        // Look for resource-related columns
        let columns = df.get_column_names();
        let resource_columns: Vec<&str> = columns.iter()
            .filter(|col| col.to_lowercase().contains("resource") || 
                         col.to_lowercase().contains("unit") ||
                         col.to_lowercase().contains("qse"))
            .copied()
            .collect();
        
        if !resource_columns.is_empty() {
            println!("    Resource columns found: {:?}", resource_columns);
            
            // Check for BESS resources
            for col in &resource_columns {
                if let Ok(series) = df.column(col) {
                    if let Ok(values) = series.utf8() {
                        let bess_count = values.into_iter()
                            .filter_map(|v| v)
                            .filter(|v| v.to_lowercase().contains("bess") || 
                                       v.to_lowercase().contains("battery") ||
                                       v.to_lowercase().contains("storage"))
                            .take(10)
                            .collect::<Vec<_>>();
                        
                        if !bess_count.is_empty() {
                            println!("    🔋 Found BESS resources in column '{}': {:?}", col, bess_count);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    fn extract_year_from_filename(&self, path: &Path) -> Option<u16> {
        let filename = path.file_name()?.to_str()?;
        
        // Look for YYYYMMDD pattern
        for i in 0..filename.len().saturating_sub(7) {
            if let Some(substr) = filename.get(i..i+8) {
                if substr.chars().all(|c| c.is_ascii_digit()) {
                    if let Ok(year) = substr[0..4].parse::<u16>() {
                        if year >= 2010 && year <= 2025 {
                            return Some(year);
                        }
                    }
                }
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
        let batch_size = 50;
        let mut total_rows = 0;
        
        for chunk in files.chunks(batch_size) {
            let chunk_dfs: Vec<(DataFrame, usize)> = chunk
                .par_iter()
                .filter_map(|file| {
                    pb.inc(1);
                    match CsvReader::new(std::fs::File::open(file).ok()?)
                        .has_header(true)
                        .finish() {
                        Ok(df) => {
                            let rows = df.height();
                            Some((df, rows))
                        },
                        Err(_) => None
                    }
                })
                .collect();
            
            for (df, rows) in chunk_dfs {
                total_rows += rows;
                all_dfs.push(df);
            }
        }
        
        pb.finish();
        
        if all_dfs.is_empty() {
            println!("      ❌ No valid data");
            return Ok(());
        }
        
        println!("      📊 Loaded {} dataframes with {} total rows", all_dfs.len(), total_rows);
        
        // For now, just save a sample of the data to understand structure
        if year == 2024 || year == 2025 {
            // Concatenate all dataframes
            println!("      🔗 Concatenating dataframes...");
            let combined = concat(
                all_dfs.iter().map(|df| df.clone().lazy()).collect::<Vec<_>>().as_slice(),
                UnionArgs::default(),
            )?.collect()?;
            
            // Save output files
            let base_name = format!("60_Day_{}_Disclosure_{}", report_type, year);
            
            // Save Parquet (most efficient)
            let parquet_path = self.output_dir.join(format!("{}.parquet", base_name));
            ParquetWriter::new(std::fs::File::create(&parquet_path)?)
                .finish(&mut combined.clone())?;
            
            println!("      ✅ Saved {} records to {}", combined.height(), parquet_path.display());
            
            // Analyze for BESS resources
            self.find_bess_resources(&combined, report_type, year)?;
        }
        
        Ok(())
    }

    fn find_bess_resources(&self, df: &DataFrame, report_type: &str, year: u16) -> Result<()> {
        let columns = df.get_column_names();
        
        for col in columns {
            if col.to_lowercase().contains("resource") || 
               col.to_lowercase().contains("unit") ||
               col.to_lowercase().contains("qse") {
                
                if let Ok(series) = df.column(col) {
                    if let Ok(values) = series.utf8() {
                        let bess_resources: Vec<String> = values.into_iter()
                            .filter_map(|v| v)
                            .filter(|v| v.to_lowercase().contains("bess") || 
                                       v.to_lowercase().contains("battery") ||
                                       v.to_lowercase().contains("storage") ||
                                       v.to_lowercase().contains("esr")) // Energy Storage Resource
                            .map(|s| s.to_string())
                            .collect::<std::collections::HashSet<_>>()
                            .into_iter()
                            .collect();
                        
                        if !bess_resources.is_empty() {
                            println!("      🔋 Found {} unique BESS resources in {} {} column '{}':", 
                                    bess_resources.len(), report_type, year, col);
                            for (i, resource) in bess_resources.iter().take(10).enumerate() {
                                println!("         {}: {}", i+1, resource);
                            }
                            if bess_resources.len() > 10 {
                                println!("         ... and {} more", bess_resources.len() - 10);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}

pub fn process_disclosure_fast() -> Result<()> {
    let output_dir = PathBuf::from("disclosure_data");
    let processor = DisclosureFastProcessor::new(output_dir)?;
    processor.process_extracted_disclosures()?;
    Ok(())
}