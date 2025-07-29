use anyhow::Result;
use std::path::PathBuf;
use std::io::Write;

pub fn extract_and_process_historical() -> Result<()> {
    println!("📦 Extracting and processing historical ERCOT data...");
    
    let base_dir = PathBuf::from("/Users/enrico/data/ERCOT_data");
    let output_dir = PathBuf::from("ercot_historical_extracted");
    std::fs::create_dir_all(&output_dir)?;
    
    // Process Historical DAM
    let dam_dir = base_dir.join("Historical_DAM_Load_Zone_and_Hub_Prices");
    println!("\n🏛️  Extracting Historical DAM files...");
    
    for entry in std::fs::read_dir(&dam_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("zip") {
            let filename = path.file_stem().unwrap().to_str().unwrap();
            
            // Extract year from filename
            if let Some(year_pos) = filename.rfind("_") {
                if let Ok(year) = filename[year_pos+1..].parse::<u16>() {
                    println!("  Extracting DAM year {}...", year);
                    
                    // Extract CSV from zip
                    let file = std::fs::File::open(&path)?;
                    let mut archive = ::zip::ZipArchive::new(file)?;
                    
                    for i in 0..archive.len() {
                        let mut file = archive.by_index(i)?;
                        let outpath = output_dir.join(format!("DAM_{}.xlsx", year));
                        
                        let mut outfile = std::fs::File::create(&outpath)?;
                        std::io::copy(&mut file, &mut outfile)?;
                        println!("    ✅ Extracted to {}", outpath.display());
                    }
                }
            }
        }
    }
    
    // Process Historical RTM
    let rtm_dir = base_dir.join("Historical_RTM_Load_Zone_and_Hub_Prices");
    println!("\n🏛️  Extracting Historical RTM files...");
    
    for entry in std::fs::read_dir(&rtm_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("zip") {
            let filename = path.file_stem().unwrap().to_str().unwrap();
            
            // Extract year from filename
            if let Some(year_pos) = filename.rfind("_") {
                if let Ok(year) = filename[year_pos+1..].parse::<u16>() {
                    println!("  Extracting RTM year {}...", year);
                    
                    // Extract from zip
                    let file = std::fs::File::open(&path)?;
                    let mut archive = ::zip::ZipArchive::new(file)?;
                    
                    for i in 0..archive.len() {
                        let mut file = archive.by_index(i)?;
                        let outpath = output_dir.join(format!("RTM_{}.xlsx", year));
                        
                        let mut outfile = std::fs::File::create(&outpath)?;
                        std::io::copy(&mut file, &mut outfile)?;
                        println!("    ✅ Extracted to {}", outpath.display());
                    }
                }
            }
        }
    }
    
    println!("\n✅ Extraction complete! Check the {} directory", output_dir.display());
    
    // Now let's process the real-time settlement point prices with all historical data
    println!("\n🚀 Summary of available ERCOT data:");
    println!("- Historical DAM Load Zone/Hub Prices: 2010-2025 (extracted)");
    println!("- Historical RTM Load Zone/Hub Prices: 2010-2025 (extracted)");
    println!("- RT Settlement Point Prices: 2024-2025 (already processed in annual_data/)");
    println!("- DAM Settlement Point Prices: 2024-2025 (daily files available)");
    
    Ok(())
}