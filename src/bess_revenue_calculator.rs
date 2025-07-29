use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct BessRevenue {
    pub resource_name: String,
    pub date: NaiveDate,
    pub energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub rrs_revenue: f64,
    pub ecrs_revenue: f64,
    pub non_spin_revenue: f64,
    pub total_revenue: f64,
    pub energy_cycles: f64,
    pub soc_violations: u32,
    pub as_failures: u32,
}

#[derive(Debug, Clone)]
pub struct AsDispatchEvent {
    pub resource_name: String,
    pub timestamp: NaiveDateTime,
    pub service_type: String,
    pub dispatch_mw: f64,
    pub baseline_mw: f64,
    pub response_mw: f64,
    pub compliance: bool,
}

pub struct BessRevenueCalculator {
    data_dir: PathBuf,
    output_dir: PathBuf,
    bess_resources: HashMap<String, (String, f64)>, // name -> (settlement_point, capacity)
}

impl BessRevenueCalculator {
    pub fn new(bess_master_list_path: &Path) -> Result<Self> {
        let data_dir = PathBuf::from("disclosure_data");
        let output_dir = PathBuf::from("bess_analysis");
        
        // Load BESS resources from master list
        let master_df = CsvReader::new(std::fs::File::open(bess_master_list_path)?)
            .has_header(true)
            .finish()?;
        
        let mut bess_resources = HashMap::new();
        let names = master_df.column("Resource_Name")?.utf8()?;
        let settlement_points = master_df.column("Settlement_Point")?.utf8()?;
        let capacities = master_df.column("Max_Capacity_MW")?.f64()?;
        
        for i in 0..master_df.height() {
            if let (Some(name), Some(sp), Some(cap)) = 
                (names.get(i), settlement_points.get(i), capacities.get(i)) {
                bess_resources.insert(name.to_string(), (sp.to_string(), cap));
            }
        }
        
        println!("Loaded {} BESS resources for revenue calculation", bess_resources.len());
        
        Ok(Self {
            data_dir,
            output_dir,
            bess_resources,
        })
    }

    pub fn calculate_all_revenues(&self) -> Result<()> {
        println!("💰 BESS Revenue Calculation");
        println!("{}", "=".repeat(80));
        
        // Process energy revenues
        let energy_revenues = self.calculate_energy_revenues()?;
        
        // Process ancillary service revenues
        let as_revenues = self.calculate_ancillary_revenues()?;
        
        // Combine and create daily rollups
        let daily_revenues = self.create_daily_rollups(energy_revenues, as_revenues)?;
        
        // Detect SOC violations and AS failures
        self.detect_operational_issues(&daily_revenues)?;
        
        // Generate performance metrics
        self.generate_performance_metrics(&daily_revenues)?;
        
        Ok(())
    }

    fn calculate_energy_revenues(&self) -> Result<HashMap<(String, NaiveDate), f64>> {
        println!("\n📊 Calculating Energy Arbitrage Revenues...");
        
        let mut energy_revenues = HashMap::new();
        
        // Load DAM energy bid awards
        let dam_pattern = self.data_dir.join("DAM_extracted/60d_DAM_EnergyBidAwards*.csv");
        let dam_files: Vec<PathBuf> = glob::glob(dam_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Processing {} DAM energy bid award files", dam_files.len());
        
        let pb = indicatif::ProgressBar::new(dam_files.len() as u64);
        pb.set_style(indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        for file_path in dam_files {
            pb.inc(1);
            
            if let Ok(df) = CsvReader::new(std::fs::File::open(&file_path)?)
                .has_header(true)
                .finish() {
                
                // Filter for BESS settlement points
                if let (Ok(dates), Ok(hours), Ok(sps), Ok(awards), Ok(prices)) = (
                    df.column("Delivery Date"),
                    df.column("Hour Ending"),
                    df.column("Settlement Point"),
                    df.column("Energy Only Bid Award in MW"),
                    df.column("Settlement Point Price")
                ) {
                    let dates_utf8 = dates.utf8()?;
                    let hours_i64 = hours.i64()?;
                    let sps_utf8 = sps.utf8()?;
                    let awards_f64 = awards.f64()?;
                    let prices_f64 = prices.f64()?;
                    
                    // Find BESS resources by settlement point
                    let sp_to_resources = self.get_sp_to_resources_map();
                    
                    for i in 0..df.height() {
                        if let (Some(date_str), Some(_hour), Some(sp), Some(award_mw), Some(price)) = 
                            (dates_utf8.get(i), hours_i64.get(i), sps_utf8.get(i), 
                             awards_f64.get(i), prices_f64.get(i)) {
                            
                            if let Some(resources) = sp_to_resources.get(sp) {
                                // Parse date
                                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                                    // Calculate revenue for each resource at this SP
                                    for resource_name in resources {
                                        let revenue = award_mw * price; // MW * $/MWh = $
                                        
                                        let key = (resource_name.clone(), date);
                                        *energy_revenues.entry(key).or_insert(0.0) += revenue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        pb.finish();
        println!("Calculated energy revenues for {} resource-days", energy_revenues.len());
        
        Ok(energy_revenues)
    }

    fn calculate_ancillary_revenues(&self) -> Result<HashMap<(String, NaiveDate), HashMap<String, f64>>> {
        println!("\n⚡ Calculating Ancillary Service Revenues...");
        
        let mut as_revenues = HashMap::new();
        
        // Load Gen Resource Data with AS awards
        let gen_pattern = self.data_dir.join("DAM_extracted/60d_DAM_Gen_Resource_Data*.csv");
        let gen_files: Vec<PathBuf> = glob::glob(gen_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("Processing {} Gen Resource Data files", gen_files.len());
        
        let pb = indicatif::ProgressBar::new(gen_files.len() as u64);
        pb.set_style(indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        for file_path in gen_files {
            pb.inc(1);
            
            if let Ok(df) = CsvReader::new(std::fs::File::open(&file_path)?)
                .has_header(true)
                .finish() {
                
                // Filter for BESS resources
                if let Ok(resource_types) = df.column("Resource Type") {
                    let mask = resource_types.utf8()?.equal("PWRSTR");
                    
                    if let Ok(filtered) = df.filter(&mask) {
                        self.process_as_awards(&filtered, &mut as_revenues)?;
                    }
                }
            }
        }
        
        pb.finish();
        println!("Calculated AS revenues for {} resource-days", as_revenues.len());
        
        Ok(as_revenues)
    }

    fn process_as_awards(&self, df: &DataFrame, 
                        as_revenues: &mut HashMap<(String, NaiveDate), HashMap<String, f64>>) -> Result<()> {
        // Extract relevant columns
        let dates = df.column("Delivery Date")?.utf8()?;
        let _hours = df.column("Hour Ending")?.i64()?;
        let resources = df.column("Resource Name")?.utf8()?;
        let _prices = df.column("Energy Settlement Point Price")?.f64()?;
        
        // AS awards and prices
        let reg_up_awards = df.column("RegUp Awarded").ok().and_then(|c| c.f64().ok());
        let reg_up_prices = df.column("RegUp MCPC").ok().and_then(|c| c.f64().ok());
        let reg_down_awards = df.column("RegDown Awarded").ok().and_then(|c| c.f64().ok());
        let reg_down_prices = df.column("RegDown MCPC").ok().and_then(|c| c.f64().ok());
        let rrs_awards = df.column("RRSFFR Awarded").ok().and_then(|c| c.f64().ok());
        let rrs_prices = df.column("RRS MCPC").ok().and_then(|c| c.f64().ok());
        let ecrs_awards = df.column("ECRSSD Awarded").ok().and_then(|c| c.f64().ok());
        let ecrs_prices = df.column("ECRS MCPC").ok().and_then(|c| c.f64().ok());
        let non_spin_awards = df.column("NonSpin Awarded").ok().and_then(|c| c.f64().ok());
        let non_spin_prices = df.column("NonSpin MCPC").ok().and_then(|c| c.f64().ok());
        
        for i in 0..df.height() {
            if let (Some(date_str), Some(resource)) = (dates.get(i), resources.get(i)) {
                if self.bess_resources.contains_key(resource) {
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                        let key = (resource.to_string(), date);
                        let revenues = as_revenues.entry(key).or_insert_with(HashMap::new);
                        
                        // Calculate revenues for each AS type
                        if let (Some(awards), Some(prices)) = (&reg_up_awards, &reg_up_prices) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("RegUp".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (&reg_down_awards, &reg_down_prices) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("RegDown".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (&rrs_awards, &rrs_prices) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("RRS".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (&ecrs_awards, &ecrs_prices) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("ECRS".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (&non_spin_awards, &non_spin_prices) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("NonSpin".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    fn create_daily_rollups(&self, 
                           energy_revenues: HashMap<(String, NaiveDate), f64>,
                           as_revenues: HashMap<(String, NaiveDate), HashMap<String, f64>>) 
                           -> Result<Vec<BessRevenue>> {
        println!("\n📅 Creating Daily Revenue Rollups...");
        
        let mut daily_revenues = Vec::new();
        
        // Get all unique resource-date combinations
        let mut all_keys = HashSet::new();
        for key in energy_revenues.keys() {
            all_keys.insert(key.clone());
        }
        for key in as_revenues.keys() {
            all_keys.insert(key.clone());
        }
        
        for (resource_name, date) in all_keys {
            let energy_rev = energy_revenues.get(&(resource_name.clone(), date)).unwrap_or(&0.0);
            let as_rev = as_revenues.get(&(resource_name.clone(), date));
            
            let mut revenue = BessRevenue {
                resource_name: resource_name.clone(),
                date,
                energy_revenue: *energy_rev,
                reg_up_revenue: 0.0,
                reg_down_revenue: 0.0,
                rrs_revenue: 0.0,
                ecrs_revenue: 0.0,
                non_spin_revenue: 0.0,
                total_revenue: *energy_rev,
                energy_cycles: 0.0, // To be calculated
                soc_violations: 0,
                as_failures: 0,
            };
            
            if let Some(as_revs) = as_rev {
                revenue.reg_up_revenue = *as_revs.get("RegUp").unwrap_or(&0.0);
                revenue.reg_down_revenue = *as_revs.get("RegDown").unwrap_or(&0.0);
                revenue.rrs_revenue = *as_revs.get("RRS").unwrap_or(&0.0);
                revenue.ecrs_revenue = *as_revs.get("ECRS").unwrap_or(&0.0);
                revenue.non_spin_revenue = *as_revs.get("NonSpin").unwrap_or(&0.0);
                
                revenue.total_revenue += revenue.reg_up_revenue + revenue.reg_down_revenue + 
                                       revenue.rrs_revenue + revenue.ecrs_revenue + revenue.non_spin_revenue;
            }
            
            daily_revenues.push(revenue);
        }
        
        // Sort by resource and date
        daily_revenues.sort_by(|a, b| {
            a.resource_name.cmp(&b.resource_name)
                .then(a.date.cmp(&b.date))
        });
        
        println!("Created {} daily revenue records", daily_revenues.len());
        
        // Save daily rollups
        self.save_daily_rollups(&daily_revenues)?;
        
        Ok(daily_revenues)
    }

    fn detect_operational_issues(&self, daily_revenues: &[BessRevenue]) -> Result<()> {
        println!("\n🔍 Detecting Operational Issues...");
        
        // Group by resource
        let mut resources: HashMap<String, Vec<&BessRevenue>> = HashMap::new();
        for revenue in daily_revenues {
            resources.entry(revenue.resource_name.clone())
                .or_insert_with(Vec::new)
                .push(revenue);
        }
        
        let mut total_violations = 0;
        let mut total_failures = 0;
        
        for (resource_name, revenues) in resources {
            let mut violations = 0;
            let mut failures = 0;
            
            // Simple heuristics for detecting issues
            for window in revenues.windows(2) {
                let (prev, curr) = (&window[0], &window[1]);
                
                // Check for potential SOC violations (simplified)
                // If energy revenue swings are too large relative to capacity
                if let Some((_, capacity)) = self.bess_resources.get(&resource_name) {
                    let energy_swing = (curr.energy_revenue - prev.energy_revenue).abs();
                    let max_daily_revenue = capacity * 24.0 * 100.0; // Assume $100/MWh max
                    
                    if energy_swing > max_daily_revenue * 2.0 {
                        violations += 1;
                    }
                }
                
                // Check for AS failures (no AS revenue when previously had AS obligations)
                if (prev.reg_up_revenue > 0.0 || prev.reg_down_revenue > 0.0 || 
                    prev.rrs_revenue > 0.0 || prev.ecrs_revenue > 0.0) &&
                   (curr.reg_up_revenue == 0.0 && curr.reg_down_revenue == 0.0 && 
                    curr.rrs_revenue == 0.0 && curr.ecrs_revenue == 0.0) &&
                   curr.total_revenue < prev.total_revenue * 0.5 {
                    failures += 1;
                }
            }
            
            if violations > 0 || failures > 0 {
                println!("  {} - SOC violations: {}, AS failures: {}", 
                        resource_name, violations, failures);
            }
            
            total_violations += violations;
            total_failures += failures;
        }
        
        println!("\nTotal operational issues detected:");
        println!("  SOC violations: {}", total_violations);
        println!("  AS failures: {}", total_failures);
        
        Ok(())
    }

    fn generate_performance_metrics(&self, daily_revenues: &[BessRevenue]) -> Result<()> {
        println!("\n📊 Generating Performance Metrics...");
        
        // Calculate annual totals by resource
        let mut annual_totals: HashMap<String, f64> = HashMap::new();
        let mut resource_days: HashMap<String, u32> = HashMap::new();
        
        for revenue in daily_revenues {
            *annual_totals.entry(revenue.resource_name.clone()).or_insert(0.0) += revenue.total_revenue;
            *resource_days.entry(revenue.resource_name.clone()).or_insert(0) += 1;
        }
        
        // Create leaderboard with $/MW metrics
        let mut leaderboard = Vec::new();
        
        for (resource_name, total_revenue) in annual_totals {
            if let Some((_, capacity)) = self.bess_resources.get(&resource_name) {
                let days = resource_days.get(&resource_name).unwrap_or(&1);
                let annualized_revenue = (total_revenue / *days as f64) * 365.0;
                let revenue_per_mw = if *capacity > 0.0 { 
                    annualized_revenue / capacity 
                } else { 
                    0.0 
                };
                
                leaderboard.push((resource_name, revenue_per_mw, annualized_revenue, *capacity));
            }
        }
        
        // Sort by $/MW
        leaderboard.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        println!("\n🏆 BESS Performance Leaderboard (Top 20):");
        println!("{:<40} {:>15} {:>20} {:>10}", "Resource Name", "$/MW/year", "Total $/year", "MW");
        println!("{}", "-".repeat(95));
        
        for (i, (name, rev_per_mw, total_rev, capacity)) in leaderboard.iter().take(20).enumerate() {
            println!("{:2}. {:<37} ${:>13.0} ${:>18.0} {:>9.1}", 
                    i + 1, name, rev_per_mw, total_rev, capacity);
        }
        
        // Calculate market statistics
        let total_market_revenue: f64 = leaderboard.iter().map(|(_, _, rev, _)| rev).sum();
        let total_market_capacity: f64 = leaderboard.iter().map(|(_, _, _, cap)| cap).sum();
        let market_average = total_market_revenue / total_market_capacity;
        
        println!("\n📈 Market Statistics:");
        println!("  Total BESS capacity: {:.1} MW", total_market_capacity);
        println!("  Total market revenue: ${:.0}/year", total_market_revenue);
        println!("  Market average: ${:.0}/MW/year", market_average);
        
        // Compare to Modo benchmark
        println!("\n📊 Benchmark Comparison:");
        println!("  Modo Energy 2023 average: $196,000/MW/year");
        println!("  This analysis average: ${:.0}/MW/year", market_average);
        
        // Save leaderboard
        self.save_leaderboard(&leaderboard)?;
        
        Ok(())
    }

    fn save_daily_rollups(&self, revenues: &[BessRevenue]) -> Result<()> {
        let mut resource_names = Vec::new();
        let mut dates = Vec::new();
        let mut energy_revs = Vec::new();
        let mut reg_up_revs = Vec::new();
        let mut reg_down_revs = Vec::new();
        let mut rrs_revs = Vec::new();
        let mut ecrs_revs = Vec::new();
        let mut non_spin_revs = Vec::new();
        let mut total_revs = Vec::new();
        
        for rev in revenues {
            resource_names.push(rev.resource_name.clone());
            dates.push(rev.date.format("%Y-%m-%d").to_string());
            energy_revs.push(rev.energy_revenue);
            reg_up_revs.push(rev.reg_up_revenue);
            reg_down_revs.push(rev.reg_down_revenue);
            rrs_revs.push(rev.rrs_revenue);
            ecrs_revs.push(rev.ecrs_revenue);
            non_spin_revs.push(rev.non_spin_revenue);
            total_revs.push(rev.total_revenue);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", resource_names),
            Series::new("Date", dates),
            Series::new("Energy_Revenue", energy_revs),
            Series::new("RegUp_Revenue", reg_up_revs),
            Series::new("RegDown_Revenue", reg_down_revs),
            Series::new("RRS_Revenue", rrs_revs),
            Series::new("ECRS_Revenue", ecrs_revs),
            Series::new("NonSpin_Revenue", non_spin_revs),
            Series::new("Total_Revenue", total_revs),
        ])?;
        
        let output_path = self.output_dir.join("bess_daily_revenues.csv");
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut df.clone())?;
        
        // Also save as Parquet
        let parquet_path = self.output_dir.join("bess_daily_revenues.parquet");
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut df.clone())?;
        
        println!("\n✅ Saved daily revenue rollups to: {}", output_path.display());
        
        Ok(())
    }

    fn save_leaderboard(&self, leaderboard: &[(String, f64, f64, f64)]) -> Result<()> {
        let mut names = Vec::new();
        let mut rev_per_mw = Vec::new();
        let mut total_revs = Vec::new();
        let mut capacities = Vec::new();
        
        for (name, rpm, total, cap) in leaderboard {
            names.push(name.clone());
            rev_per_mw.push(*rpm);
            total_revs.push(*total);
            capacities.push(*cap);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", names),
            Series::new("Revenue_Per_MW_Year", rev_per_mw),
            Series::new("Total_Revenue_Year", total_revs),
            Series::new("Capacity_MW", capacities),
        ])?;
        
        let output_path = self.output_dir.join("bess_performance_leaderboard.csv");
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut df.clone())?;
        
        println!("✅ Saved performance leaderboard to: {}", output_path.display());
        
        Ok(())
    }

    fn get_sp_to_resources_map(&self) -> HashMap<String, Vec<String>> {
        let mut sp_map = HashMap::new();
        
        for (resource_name, (sp, _)) in &self.bess_resources {
            sp_map.entry(sp.clone())
                .or_insert_with(Vec::new)
                .push(resource_name.clone());
        }
        
        sp_map
    }
}

pub fn calculate_bess_revenues() -> Result<()> {
    let master_list_path = PathBuf::from("bess_analysis/bess_resources_master_list.csv");
    let calculator = BessRevenueCalculator::new(&master_list_path)?;
    calculator.calculate_all_revenues()?;
    Ok(())
}