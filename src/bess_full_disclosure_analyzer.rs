use anyhow::Result;
use std::path::PathBuf;
use indicatif::{ProgressBar, ProgressStyle};

/// Runs the comprehensive BESS revenue analysis using the complete 60-day disclosure dataset
pub fn analyze_bess_with_full_disclosure() -> Result<()> {
    println!("\n💰 ERCOT BESS Revenue Analysis - Complete 60-Day Disclosure Dataset");
    println!("{}", "=".repeat(80));
    
    // Set up paths
    let _master_list_path = PathBuf::from("bess_analysis/bess_resources_master_list.csv");
    
    // Create symbolic link to the actual disclosure data if it doesn't exist
    let disclosure_link = PathBuf::from("disclosure_data");
    let actual_disclosure = PathBuf::from("/Users/enrico/data/ERCOT_data/60-Day_COP_Adjustment_Period_Snapshot");
    
    if !disclosure_link.exists() && actual_disclosure.exists() {
        println!("📁 Creating link to disclosure data...");
        std::os::unix::fs::symlink(&actual_disclosure, &disclosure_link)?;
    }
    
    // Extract disclosure data if needed
    let csv_dir = disclosure_link.join("csv");
    if !csv_dir.exists() {
        println!("📂 Extracting disclosure ZIP files...");
        extract_disclosure_zips(&actual_disclosure)?;
    }
    
    // Now run the existing comprehensive revenue calculator
    crate::bess_revenue_calculator::calculate_bess_revenues()?;
    
    println!("\n✅ Analysis complete!");
    Ok(())
}

fn extract_disclosure_zips(disclosure_dir: &PathBuf) -> Result<()> {
    use ::zip::ZipArchive;
    use std::fs::File;
    use std::io::copy;
    
    let csv_dir = disclosure_dir.join("csv");
    std::fs::create_dir_all(&csv_dir)?;
    
    // Find all ZIP files
    let pattern = disclosure_dir.join("*.zip");
    let zip_files: Vec<PathBuf> = glob::glob(pattern.to_str().unwrap())?
        .filter_map(Result::ok)
        .collect();
    
    println!("  Found {} ZIP files to extract", zip_files.len());
    
    let pb = ProgressBar::new(zip_files.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} - {msg}")
        .unwrap());
    
    for zip_path in zip_files {
        pb.inc(1);
        pb.set_message(format!("Extracting {}", zip_path.file_name().unwrap().to_str().unwrap()));
        
        let file = File::open(&zip_path)?;
        let mut archive = ZipArchive::new(file)?;
        
        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            
            // Only extract CSV files
            if file.name().ends_with(".csv") {
                let outpath = csv_dir.join(file.name());
                
                if let Some(p) = outpath.parent() {
                    std::fs::create_dir_all(p)?;
                }
                
                let mut outfile = File::create(&outpath)?;
                copy(&mut file, &mut outfile)?;
            }
        }
    }
    
    pb.finish_with_message("Extraction complete");
    
    // Also extract other 60-day disclosure folders if they exist
    let other_folders = vec![
        ("60-Day_SCED_Disclosure_Reports", "SCED_extracted"),
        ("60-Day_DAM_Disclosure_Reports", "DAM_extracted"),
        ("60-Day_SASM_Disclosure_Reports", "SASM_extracted"),
    ];
    
    for (folder_name, extract_name) in other_folders {
        let folder_path = disclosure_dir.parent().unwrap().join(folder_name);
        if folder_path.exists() {
            println!("\n  Extracting {} files...", folder_name);
            let extract_dir = disclosure_dir.join(extract_name);
            std::fs::create_dir_all(&extract_dir)?;
            
            let pattern = folder_path.join("*.zip");
            let zip_files: Vec<PathBuf> = glob::glob(pattern.to_str().unwrap())?
                .filter_map(Result::ok)
                .collect();
            
            for zip_path in zip_files {
                let file = File::open(&zip_path)?;
                if let Ok(mut archive) = ZipArchive::new(file) {
                    for i in 0..archive.len() {
                        if let Ok(mut file) = archive.by_index(i) {
                            if file.name().ends_with(".csv") {
                                let outpath = extract_dir.join(file.name());
                                
                                if let Some(p) = outpath.parent() {
                                    let _ = std::fs::create_dir_all(p);
                                }
                                
                                if let Ok(mut outfile) = File::create(&outpath) {
                                    let _ = copy(&mut file, &mut outfile);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    Ok(())
}