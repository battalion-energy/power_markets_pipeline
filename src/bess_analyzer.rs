use anyhow::Result;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct BessResource {
    pub name: String,
    pub qse: String,
    pub dme: String,
    pub settlement_point: String,
    pub max_capacity: f64,
    pub min_capacity: f64,
}

pub struct BessAnalyzer {
    disclosure_dir: PathBuf,
    output_dir: PathBuf,
}

impl BessAnalyzer {
    pub fn new() -> Result<Self> {
        let disclosure_dir = PathBuf::from("disclosure_data");
        let output_dir = PathBuf::from("bess_analysis");
        std::fs::create_dir_all(&output_dir)?;
        
        Ok(Self {
            disclosure_dir,
            output_dir,
        })
    }

    pub fn find_all_bess_resources(&self) -> Result<()> {
        println!("🔋 BESS Resource Discovery and Analysis");
        println!("{}", "=".repeat(80));
        
        // Find all Gen_Resource_Data files
        let pattern = self.disclosure_dir.join("*/60d_DAM_Gen_Resource_Data*.csv");
        let resource_files: Vec<PathBuf> = glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Found {} Gen Resource Data files", resource_files.len());
        
        // Collect all unique BESS resources
        let mut all_bess_resources: HashMap<String, BessResource> = HashMap::new();
        let mut bess_appearances: HashMap<String, Vec<(String, String)>> = HashMap::new();
        
        let pb = ProgressBar::new(resource_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        for file_path in &resource_files {
            pb.inc(1);
            
            // Read CSV file
            let file = match std::fs::File::open(file_path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            
            let df = match CsvReader::new(file).has_header(true).finish() {
                Ok(d) => d,
                Err(_) => continue,
            };
            
            // Filter for PWRSTR resource type
            if let Ok(resource_type) = df.column("Resource Type") {
                if let Ok(rt_utf8) = resource_type.utf8() {
                    let mask = rt_utf8.equal("PWRSTR");
                    
                    if let Ok(filtered) = df.filter(&mask) {
                        if filtered.height() > 0 {
                            // Extract columns
                            let resource_names = filtered.column("Resource Name").ok().and_then(|c| c.utf8().ok());
                            let qses = filtered.column("QSE").ok().and_then(|c| c.utf8().ok());
                            let dmes = filtered.column("DME").ok().and_then(|c| c.utf8().ok());
                            let settlement_points = filtered.column("Settlement Point Name").ok().and_then(|c| c.utf8().ok());
                            let hsls = filtered.column("HSL").ok();
                            let lsls = filtered.column("LSL").ok();
                            let statuses = filtered.column("Resource Status").ok().and_then(|c| c.utf8().ok());
                            let dates = filtered.column("Delivery Date").ok().and_then(|c| c.utf8().ok());
                            
                            if let (Some(names), Some(qse_col), Some(dme_col), Some(sp_col)) = 
                                (resource_names, qses, dmes, settlement_points) {
                                
                                for i in 0..filtered.height() {
                                    if let (Some(name), Some(qse), Some(dme), Some(sp)) = 
                                        (names.get(i), qse_col.get(i), dme_col.get(i), sp_col.get(i)) {
                                        
                                        let hsl = hsls.as_ref()
                                            .and_then(|h| h.f64().ok())
                                            .and_then(|h| h.get(i))
                                            .unwrap_or(0.0);
                                        let lsl = lsls.as_ref()
                                            .and_then(|l| l.f64().ok())
                                            .and_then(|l| l.get(i))
                                            .unwrap_or(0.0);
                                        let status = statuses.as_ref()
                                            .and_then(|s| s.get(i))
                                            .unwrap_or("UNKNOWN");
                                        let date = dates.as_ref()
                                            .and_then(|d| d.get(i))
                                            .unwrap_or("UNKNOWN");
                                        
                                        // Create resource
                                        let resource = BessResource {
                                            name: name.to_string(),
                                            qse: qse.to_string(),
                                            dme: dme.to_string(),
                                            settlement_point: sp.to_string(),
                                            max_capacity: hsl,
                                            min_capacity: lsl,
                                        };
                                        
                                        all_bess_resources.insert(name.to_string(), resource);
                                        bess_appearances.entry(name.to_string())
                                            .or_insert_with(Vec::new)
                                            .push((date.to_string(), status.to_string()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        pb.finish();
        
        println!("\n📊 BESS Resource Summary:");
        println!("Total unique BESS resources found: {}", all_bess_resources.len());
        
        // Sort by capacity
        let mut sorted_resources: Vec<_> = all_bess_resources.values().collect();
        sorted_resources.sort_by(|a, b| b.max_capacity.partial_cmp(&a.max_capacity).unwrap());
        
        println!("\n🔋 Top 20 BESS Resources by Capacity:");
        println!("{:<40} {:<10} {:<20} {:<15} {:<10}", "Resource Name", "QSE", "DME", "Settlement Point", "Max MW");
        println!("{}", "-".repeat(105));
        
        for (i, resource) in sorted_resources.iter().take(20).enumerate() {
            println!("{:2}. {:<37} {:<10} {:<20} {:<15} {:>8.1}", 
                i + 1,
                resource.name,
                resource.qse,
                resource.dme,
                resource.settlement_point,
                resource.max_capacity
            );
        }
        
        // Save all BESS resources to CSV
        self.save_bess_resources(&all_bess_resources)?;
        
        // Analyze activity patterns
        self.analyze_bess_activity(&all_bess_resources, &bess_appearances)?;
        
        Ok(())
    }

    fn save_bess_resources(&self, resources: &HashMap<String, BessResource>) -> Result<()> {
        let mut names = Vec::new();
        let mut qses = Vec::new();
        let mut dmes = Vec::new();
        let mut settlement_points = Vec::new();
        let mut max_capacities = Vec::new();
        let mut min_capacities = Vec::new();
        
        for resource in resources.values() {
            names.push(resource.name.clone());
            qses.push(resource.qse.clone());
            dmes.push(resource.dme.clone());
            settlement_points.push(resource.settlement_point.clone());
            max_capacities.push(resource.max_capacity);
            min_capacities.push(resource.min_capacity);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", names),
            Series::new("QSE", qses),
            Series::new("DME", dmes),
            Series::new("Settlement_Point", settlement_points),
            Series::new("Max_Capacity_MW", max_capacities),
            Series::new("Min_Capacity_MW", min_capacities),
        ])?;
        
        let output_path = self.output_dir.join("bess_resources_master_list.csv");
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut df.clone())?;
        
        println!("\n✅ Saved BESS resource list to: {}", output_path.display());
        
        // Also save as Parquet
        let parquet_path = self.output_dir.join("bess_resources_master_list.parquet");
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut df.clone())?;
        
        Ok(())
    }

    fn analyze_bess_activity(&self, resources: &HashMap<String, BessResource>, 
                             appearances: &HashMap<String, Vec<(String, String)>>) -> Result<()> {
        println!("\n📈 BESS Activity Analysis:");
        
        // Count status types
        let mut status_counts: HashMap<String, usize> = HashMap::new();
        for (_, statuses) in appearances {
            for (_, status) in statuses {
                *status_counts.entry(status.clone()).or_insert(0) += 1;
            }
        }
        
        println!("\nResource Status Distribution:");
        for (status, count) in status_counts.iter() {
            println!("  {}: {} occurrences", status, count);
        }
        
        // Find most active resources
        let mut activity_counts: Vec<(String, usize)> = appearances.iter()
            .map(|(name, apps)| (name.clone(), apps.len()))
            .collect();
        activity_counts.sort_by(|a, b| b.1.cmp(&a.1));
        
        println!("\nMost Active BESS Resources (by appearances):");
        for (name, count) in activity_counts.iter().take(10) {
            if let Some(resource) = resources.get(name) {
                println!("  {} ({} MW): {} appearances", name, resource.max_capacity, count);
            }
        }
        
        Ok(())
    }
}

pub fn analyze_bess_resources() -> Result<()> {
    let analyzer = BessAnalyzer::new()?;
    analyzer.find_all_bess_resources()?;
    Ok(())
}