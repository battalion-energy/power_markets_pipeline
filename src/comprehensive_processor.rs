use anyhow::Result;
use std::path::PathBuf;
use crate::ercot_processor::ErcotProcessor;

pub fn process_all_ercot_data() -> Result<()> {
    println!("🚀 ERCOT Comprehensive Data Processor");
    println!("Using {} CPU cores", num_cpus::get());
    println!("{}", "=".repeat(80));
    
    let base_dir = PathBuf::from("/Users/enrico/data/ERCOT_data");
    let output_dir = PathBuf::from("ercot_processed_data");
    std::fs::create_dir_all(&output_dir)?;
    
    let processor = ErcotProcessor::new(output_dir.clone());
    let start = std::time::Instant::now();
    
    // Process Historical DAM Load Zone and Hub Prices (2010-2025)
    let historical_dam_dir = base_dir.join("Historical_DAM_Load_Zone_and_Hub_Prices");
    if historical_dam_dir.exists() {
        processor.process_historical_dam(&historical_dam_dir)?;
    }
    
    // Process Historical RTM Load Zone and Hub Prices (2010-2025)
    let historical_rtm_dir = base_dir.join("Historical_RTM_Load_Zone_and_Hub_Prices");
    if historical_rtm_dir.exists() {
        processor.process_historical_rtm(&historical_rtm_dir)?;
    }
    
    // Process Daily DAM Settlement Point Prices
    let dam_settlement_dir = base_dir.join("DAM_Settlement_Point_Prices");
    if dam_settlement_dir.exists() {
        processor.process_daily_dam(&dam_settlement_dir)?;
    }
    
    // Process RT Settlement Point Prices (already done)
    println!("\n✅ RT Settlement Point Prices already processed in annual_data/");
    
    let duration = start.elapsed();
    println!("\n🎉 All processing complete in {:?}!", duration);
    println!("📁 Output directory: {}", output_dir.display());
    
    Ok(())
}