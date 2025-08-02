use std::fs;
use std::path::{Path, PathBuf};
use std::io::{self, BufRead, BufReader};
use rayon::prelude::*;
use zip::ZipArchive;
use anyhow::{Result, Context};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

pub struct CsvExtractor {
    input_dir: PathBuf,
    output_dir: PathBuf,
    processed_count: Arc<AtomicUsize>,
    csv_count: Arc<AtomicUsize>,
}

impl CsvExtractor {
    pub fn new(input_dir: PathBuf) -> Self {
        let output_dir = input_dir.join("csv");
        Self {
            input_dir,
            output_dir,
            processed_count: Arc::new(AtomicUsize::new(0)),
            csv_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn extract_all(&self) -> Result<()> {
        println!("Creating output directory: {:?}", self.output_dir);
        fs::create_dir_all(&self.output_dir)?;

        // Find all ZIP files in the input directory
        let zip_files = self.find_zip_files(&self.input_dir)?;
        println!("Found {} ZIP files to process", zip_files.len());

        // Process each ZIP file in parallel
        zip_files.par_iter().for_each(|zip_path| {
            if let Err(e) = self.process_zip_file(zip_path) {
                eprintln!("Error processing {:?}: {}", zip_path, e);
            }
            
            let count = self.processed_count.fetch_add(1, Ordering::SeqCst) + 1;
            if count % 100 == 0 {
                println!("Processed {} ZIP files, found {} CSV files so far", 
                    count, self.csv_count.load(Ordering::SeqCst));
            }
        });

        println!("\nExtraction complete!");
        println!("Processed {} ZIP files", self.processed_count.load(Ordering::SeqCst));
        println!("Extracted {} CSV files to {:?}", self.csv_count.load(Ordering::SeqCst), self.output_dir);

        Ok(())
    }

    fn find_zip_files(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let mut zip_files = Vec::new();
        
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("zip") {
                // Skip XML zip files - only process CSV zip files
                let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if !filename.contains("_xml.zip") && !filename.contains("_XML.zip") {
                    zip_files.push(path);
                }
            }
        }
        
        Ok(zip_files)
    }

    fn process_zip_file(&self, zip_path: &Path) -> Result<()> {
        // Create a temporary directory for extraction
        let temp_dir = TempDir::new_in(&self.input_dir)?;
        
        // Extract the ZIP file
        self.extract_zip_recursive(zip_path, temp_dir.path())?;
        
        // Move all CSV files to the output directory
        self.collect_csv_files(temp_dir.path())?;
        
        Ok(())
    }

    fn extract_zip_recursive(&self, zip_path: &Path, extract_to: &Path) -> Result<()> {
        let file = fs::File::open(zip_path)
            .with_context(|| format!("Failed to open ZIP file: {:?}", zip_path))?;
        
        let mut archive = ZipArchive::new(file)
            .with_context(|| format!("Failed to read ZIP archive: {:?}", zip_path))?;

        // Extract all files
        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            let outpath = extract_to.join(file.name());

            if file.name().ends_with('/') {
                fs::create_dir_all(&outpath)?;
            } else {
                if let Some(p) = outpath.parent() {
                    fs::create_dir_all(p)?;
                }
                
                let mut outfile = fs::File::create(&outpath)?;
                io::copy(&mut file, &mut outfile)?;
                
                // If it's a ZIP file, recursively extract it (but skip XML zips)
                if outpath.extension().and_then(|s| s.to_str()) == Some("zip") {
                    let filename = outpath.file_name().and_then(|s| s.to_str()).unwrap_or("");
                    if !filename.contains("_xml.zip") && !filename.contains("_XML.zip") {
                        if let Err(e) = self.extract_zip_recursive(&outpath, extract_to) {
                            eprintln!("Failed to extract nested ZIP {:?}: {}", outpath, e);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    fn collect_csv_files(&self, dir: &Path) -> Result<()> {
        // Walk through all directories recursively to find CSV files
        for entry in walkdir::WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
                // Get the filename
                let filename = path.file_name().unwrap();
                let dest_path = self.output_dir.join(filename);
                
                // Move the file, overwriting if it exists
                if dest_path.exists() {
                    fs::remove_file(&dest_path)?;
                }
                
                fs::rename(path, &dest_path)?;
                self.csv_count.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        Ok(())
    }
}

pub fn extract_csv_from_directory(input_dir: PathBuf) -> Result<()> {
    let extractor = CsvExtractor::new(input_dir);
    extractor.extract_all()
}

pub fn extract_all_ercot_directories(base_dir: PathBuf) -> Result<()> {
    // Read directories from CSV file
    let csv_file = "ercot_directories.csv";
    if !Path::new(csv_file).exists() {
        return Err(anyhow::anyhow!("File {} not found in current directory", csv_file));
    }

    let file = fs::File::open(csv_file)?;
    let reader = BufReader::new(file);
    
    let mut directories = Vec::new();
    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        let line = line.trim();
        
        // Skip header line and empty lines
        if line_num == 0 || line.is_empty() || line == "directory_name" {
            continue;
        }
        
        directories.push(line.to_string());
    }

    println!("Found {} directories to process from {}", directories.len(), csv_file);
    
    // Process each directory
    let mut successful = 0;
    let mut failed = 0;
    
    for dir_name in directories {
        let full_path = base_dir.join(&dir_name);
        
        if !full_path.exists() {
            println!("⚠️  Directory not found: {}", full_path.display());
            failed += 1;
            continue;
        }
        
        println!("\n🚀 Processing: {}", dir_name);
        match extract_csv_from_directory(full_path) {
            Ok(()) => {
                println!("✅ Completed: {}", dir_name);
                successful += 1;
            }
            Err(e) => {
                println!("❌ Failed: {} - Error: {}", dir_name, e);
                failed += 1;
            }
        }
    }
    
    println!("\n📊 Summary:");
    println!("✅ Successful: {}", successful);
    println!("❌ Failed: {}", failed);
    println!("📁 Total: {}", successful + failed);
    
    Ok(())
}