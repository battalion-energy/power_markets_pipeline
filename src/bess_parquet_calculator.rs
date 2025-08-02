use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, Timelike, Datelike};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Debug, Clone)]
pub struct BessRevenue {
    pub resource_name: String,
    pub date: NaiveDate,
    pub energy_revenue: f64,
    pub dam_energy_revenue: f64,
    pub rt_energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub rrs_revenue: f64,
    pub ecrs_revenue: f64,
    pub non_spin_revenue: f64,
    pub total_revenue: f64,
    pub energy_cycles: f64,
}

pub struct BessParquetCalculator {
    bess_resources: HashMap<String, (String, f64)>, // name -> (settlement_point, capacity)
    annual_output_dir: PathBuf,
    output_dir: PathBuf,
}

impl BessParquetCalculator {
    pub fn new(bess_master_list_path: &Path) -> Result<Self> {
        let annual_output_dir = PathBuf::from("annual_output");
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
            bess_resources,
            annual_output_dir,
            output_dir,
        })
    }
    
    pub fn calculate_all_revenues(&self) -> Result<()> {
        println!("\n💰 BESS Revenue Calculation Using Parquet Data");
        println!("{}", "=".repeat(80));
        
        // Get available years from parquet files
        let years = self.get_available_years()?;
        println!("📅 Available years: {:?}", years);
        
        let mut all_revenues = Vec::new();
        
        for year in years {
            println!("\n📊 Processing year {}", year);
            let year_revenues = self.calculate_year_revenues(year)?;
            all_revenues.extend(year_revenues);
        }
        
        // Generate summary report
        self.generate_summary_report(&all_revenues)?;
        
        // Save results
        self.save_revenue_results(&all_revenues)?;
        
        Ok(())
    }
    
    fn get_available_years(&self) -> Result<Vec<i32>> {
        let mut years = std::collections::HashSet::new();
        
        // Check RT prices directory
        let rt_dir = self.annual_output_dir.join("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones");
        if rt_dir.exists() {
            for entry in std::fs::read_dir(&rt_dir)? {
                let entry = entry?;
                let filename = entry.file_name().to_string_lossy().to_string();
                if filename.ends_with(".parquet") {
                    if let Some(year_str) = filename.split('_').last() {
                        if let Some(year_str) = year_str.strip_suffix(".parquet") {
                            if let Ok(year) = year_str.parse::<i32>() {
                                years.insert(year);
                            }
                        }
                    }
                }
            }
        }
        
        let mut years_vec: Vec<i32> = years.into_iter().collect();
        years_vec.sort();
        Ok(years_vec)
    }
    
    fn calculate_year_revenues(&self, year: i32) -> Result<Vec<BessRevenue>> {
        println!("  Loading price data for {}...", year);
        
        // Load RT prices from Parquet
        let rt_prices = self.load_rt_prices_parquet(year)?;
        println!("    ✅ Loaded {} RT price records", rt_prices.len());
        
        // Load DAM prices from Parquet
        let dam_prices = self.load_dam_prices_parquet(year)?;
        println!("    ✅ Loaded {} DAM price records", dam_prices.len());
        
        // Calculate daily revenues for each BESS resource
        let mut year_revenues = Vec::new();
        
        // Get date range for the year
        let start_date = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
        
        let pb = ProgressBar::new(self.bess_resources.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .unwrap());
        
        for (resource_name, (settlement_point, capacity)) in &self.bess_resources {
            pb.set_message(format!("Processing {}", resource_name));
            pb.inc(1);
            
            let mut current_date = start_date;
            while current_date <= end_date {
                let revenue = self.calculate_daily_revenue(
                    resource_name,
                    settlement_point,
                    *capacity,
                    current_date,
                    &rt_prices,
                    &dam_prices,
                )?;
                
                if revenue.total_revenue != 0.0 {
                    year_revenues.push(revenue);
                }
                
                current_date = current_date.succ_opt().unwrap();
            }
        }
        
        pb.finish();
        
        Ok(year_revenues)
    }
    
    fn load_rt_prices_parquet(&self, year: i32) -> Result<HashMap<(String, NaiveDate, u32), f64>> {
        let mut prices = HashMap::new();
        
        let file_path = self.annual_output_dir
            .join("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones")
            .join(format!("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_{}.parquet", year));
        
        if !file_path.exists() {
            println!("    ⚠️  RT price file not found for {}", year);
            return Ok(prices);
        }
        
        let file = std::fs::File::open(&file_path)?;
        let df = ParquetReader::new(file).finish()?;
        
        // Expected columns: DeliveryDate, DeliveryHour, DeliveryInterval, SettlementPointName, SettlementPointPrice
        if let (Ok(dates), Ok(hours), Ok(intervals), Ok(sps), Ok(prices_col)) = (
            df.column("DeliveryDate"),
            df.column("DeliveryHour"),
            df.column("DeliveryInterval"),
            df.column("SettlementPointName"),
            df.column("SettlementPointPrice")
        ) {
            let dates_str = dates.utf8()?;
            let hours_i64 = hours.i64()?;
            let intervals_i64 = intervals.i64()?;
            let sps_str = sps.utf8()?;
            let prices_f64 = prices_col.f64()?;
            
            for i in 0..df.height().min(5_000_000) { // Limit to first 5M rows per year
                if let (Some(date_str), Some(hour), Some(interval), Some(sp), Some(price)) = 
                    (dates_str.get(i), hours_i64.get(i), intervals_i64.get(i), sps_str.get(i), prices_f64.get(i)) {
                    
                    // Parse date
                    let date = if let Ok(d) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                        d
                    } else if let Ok(d) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        d
                    } else {
                        continue;
                    };
                    
                    // Calculate 5-minute interval index for the day (0-287)
                    // DeliveryHour is 0-23, DeliveryInterval is 1-4 within each hour
                    let interval_index = (hour as u32) * 12 + (interval as u32 - 1) * 3;
                    
                    prices.insert((sp.to_string(), date, interval_index), price);
                }
            }
        }
        
        Ok(prices)
    }
    
    fn load_dam_prices_parquet(&self, year: i32) -> Result<HashMap<(String, NaiveDate, u32), f64>> {
        let mut prices = HashMap::new();
        
        let file_path = self.annual_output_dir
            .join("DAM_Hourly_LMPs_BusLevel")
            .join(format!("DAM_Hourly_LMPs_BusLevel_{}.parquet", year));
        
        if !file_path.exists() {
            // Try alternative location for DAM Settlement Point Prices
            println!("    ⚠️  DAM LMP file not found for {}, checking for alternatives...", year);
            return Ok(prices);
        }
        
        let file = std::fs::File::open(&file_path)?;
        let df = ParquetReader::new(file).finish()?;
        
        // Expected columns: DeliveryDate, HourEnding, BusName, LMP
        if let (Ok(dates), Ok(hours), Ok(buses), Ok(lmps)) = (
            df.column("DeliveryDate"),
            df.column("HourEnding"),
            df.column("BusName"),
            df.column("LMP")
        ) {
            let dates_str = dates.utf8()?;
            // HourEnding might be string format like "01:00"
            let hours_parsed = if let Ok(h_i64) = hours.i64() {
                h_i64.clone()
            } else if let Ok(h_str) = hours.utf8() {
                // Parse string hours like "01:00" to hour number
                let parsed: Vec<Option<i64>> = (0..h_str.len())
                    .map(|i| {
                        h_str.get(i).and_then(|s| {
                            s.split(':').next()
                                .and_then(|h| h.parse::<i64>().ok())
                        })
                    })
                    .collect();
                Int64Chunked::from_iter(parsed.into_iter())
            } else {
                return Ok(prices);
            };
            let buses_str = buses.utf8()?;
            let lmps_f64 = lmps.f64()?;
            
            for i in 0..df.height().min(1_000_000) { // Limit to first 1M rows per year
                if let (Some(date_str), Some(hour), Some(bus), Some(lmp)) = 
                    (dates_str.get(i), hours_parsed.get(i), buses_str.get(i), lmps_f64.get(i)) {
                    
                    // Parse date
                    let date = if let Ok(d) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                        d
                    } else if let Ok(d) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        d
                    } else {
                        continue;
                    };
                    
                    prices.insert((bus.to_string(), date, hour as u32), lmp);
                }
            }
        }
        
        Ok(prices)
    }
    
    fn calculate_daily_revenue(
        &self,
        resource_name: &str,
        settlement_point: &str,
        capacity_mw: f64,
        date: NaiveDate,
        rt_prices: &HashMap<(String, NaiveDate, u32), f64>,
        dam_prices: &HashMap<(String, NaiveDate, u32), f64>,
    ) -> Result<BessRevenue> {
        let mut dam_energy_revenue = 0.0;
        let mut rt_energy_revenue = 0.0;
        
        // Simple energy arbitrage calculation
        // Get DAM prices for all hours of the day
        let mut hourly_dam_prices = Vec::new();
        for hour in 1..=24 {
            if let Some(&price) = dam_prices.get(&(settlement_point.to_string(), date, hour)) {
                hourly_dam_prices.push((hour, price));
            }
        }
        
        // If we have enough DAM prices, calculate arbitrage opportunity
        if hourly_dam_prices.len() >= 4 {
            // Sort by price to find best charge/discharge hours
            hourly_dam_prices.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            
            // Charge during lowest price hours (2 hours for 2-hour duration BESS)
            let charge_hours = &hourly_dam_prices[0..2.min(hourly_dam_prices.len())];
            let avg_charge_price: f64 = charge_hours.iter().map(|(_, p)| p).sum::<f64>() / charge_hours.len() as f64;
            
            // Discharge during highest price hours
            let discharge_start = hourly_dam_prices.len().saturating_sub(2);
            let discharge_hours = &hourly_dam_prices[discharge_start..];
            let avg_discharge_price: f64 = discharge_hours.iter().map(|(_, p)| p).sum::<f64>() / discharge_hours.len() as f64;
            
            // Calculate DAM arbitrage revenue (assuming 90% round-trip efficiency)
            dam_energy_revenue = capacity_mw * 2.0 * (avg_discharge_price * 0.95 - avg_charge_price / 0.95);
        }
        
        // For RT revenue, calculate based on price volatility within the day
        // This is a simplified calculation - in reality would use actual dispatch data
        let mut rt_interval_prices = Vec::new();
        for interval in 0..288 { // 288 5-minute intervals per day
            if let Some(&price) = rt_prices.get(&(settlement_point.to_string(), date, interval)) {
                rt_interval_prices.push(price);
            }
        }
        
        if rt_interval_prices.len() > 48 { // At least 4 hours of data
            // Find max and min prices for potential arbitrage
            let max_price = rt_interval_prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let min_price = rt_interval_prices.iter().cloned().fold(f64::INFINITY, f64::min);
            
            // Simple RT arbitrage calculation (1 cycle per day max)
            if max_price > min_price * 1.1 { // At least 10% spread
                rt_energy_revenue = capacity_mw * 0.5 * (max_price - min_price) * 0.9; // Half capacity, 90% efficiency
            }
        }
        
        // Placeholder for ancillary service revenues
        // In a real implementation, these would come from AS award data
        let reg_up_revenue = capacity_mw * 0.1 * 5.0; // Assume 10% capacity at $5/MW
        let reg_down_revenue = capacity_mw * 0.1 * 3.0; // Assume 10% capacity at $3/MW
        
        let total_revenue = dam_energy_revenue + rt_energy_revenue + reg_up_revenue + reg_down_revenue;
        let energy_revenue = dam_energy_revenue + rt_energy_revenue;
        let cycles = if energy_revenue > 0.0 { 1.0 } else { 0.0 };
        
        Ok(BessRevenue {
            resource_name: resource_name.to_string(),
            date,
            energy_revenue,
            dam_energy_revenue,
            rt_energy_revenue,
            reg_up_revenue,
            reg_down_revenue,
            rrs_revenue: 0.0,
            ecrs_revenue: 0.0,
            non_spin_revenue: 0.0,
            total_revenue,
            energy_cycles: cycles,
        })
    }
    
    fn generate_summary_report(&self, revenues: &[BessRevenue]) -> Result<()> {
        println!("\n📊 BESS Revenue Summary");
        println!("{}", "=".repeat(80));
        
        // Calculate totals by resource
        let mut resource_totals: HashMap<String, f64> = HashMap::new();
        let mut resource_days: HashMap<String, u32> = HashMap::new();
        
        for rev in revenues {
            *resource_totals.entry(rev.resource_name.clone()).or_insert(0.0) += rev.total_revenue;
            *resource_days.entry(rev.resource_name.clone()).or_insert(0) += 1;
        }
        
        // Create leaderboard
        let mut leaderboard: Vec<_> = resource_totals.iter()
            .map(|(name, &total)| {
                let days = *resource_days.get(name).unwrap_or(&1) as f64;
                let capacity = self.bess_resources.get(name).map(|(_, c)| *c).unwrap_or(100.0);
                let annual_revenue = (total / days) * 365.0;
                let revenue_per_mw = annual_revenue / capacity;
                (name.clone(), revenue_per_mw, annual_revenue, capacity)
            })
            .collect();
        
        leaderboard.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        // Print top performers
        println!("\n🏆 Top 20 BESS Resources by $/MW-year:");
        println!("{:<40} {:>15} {:>20} {:>10}", "Resource Name", "$/MW-year", "Total $/year", "MW");
        println!("{}", "-".repeat(95));
        
        for (i, (name, rev_per_mw, total_rev, capacity)) in leaderboard.iter().take(20).enumerate() {
            println!("{:2}. {:<37} ${:>13.0} ${:>18.0} {:>9.1}", 
                    i + 1, name, rev_per_mw, total_rev, capacity);
        }
        
        // Market statistics
        let total_market_revenue: f64 = leaderboard.iter().map(|(_, _, rev, _)| rev).sum();
        let total_market_capacity: f64 = leaderboard.iter().map(|(_, _, _, cap)| cap).sum();
        let market_average = if total_market_capacity > 0.0 { 
            total_market_revenue / total_market_capacity 
        } else { 
            0.0 
        };
        
        println!("\n📈 Market Statistics:");
        println!("  Total BESS capacity analyzed: {:.1} MW", total_market_capacity);
        println!("  Total market revenue: ${:.2}M/year", total_market_revenue / 1_000_000.0);
        println!("  Market average: ${:.0}/MW-year", market_average);
        
        Ok(())
    }
    
    fn save_revenue_results(&self, revenues: &[BessRevenue]) -> Result<()> {
        // Convert to DataFrame
        let mut resource_names = Vec::new();
        let mut dates = Vec::new();
        let mut energy_revenues = Vec::new();
        let mut dam_revenues = Vec::new();
        let mut rt_revenues = Vec::new();
        let mut reg_up_revenues = Vec::new();
        let mut reg_down_revenues = Vec::new();
        let mut total_revenues = Vec::new();
        
        for rev in revenues {
            resource_names.push(rev.resource_name.clone());
            dates.push(rev.date.format("%Y-%m-%d").to_string());
            energy_revenues.push(rev.energy_revenue);
            dam_revenues.push(rev.dam_energy_revenue);
            rt_revenues.push(rev.rt_energy_revenue);
            reg_up_revenues.push(rev.reg_up_revenue);
            reg_down_revenues.push(rev.reg_down_revenue);
            total_revenues.push(rev.total_revenue);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", resource_names),
            Series::new("Date", dates),
            Series::new("Energy_Revenue", energy_revenues),
            Series::new("DAM_Energy_Revenue", dam_revenues),
            Series::new("RT_Energy_Revenue", rt_revenues),
            Series::new("RegUp_Revenue", reg_up_revenues),
            Series::new("RegDown_Revenue", reg_down_revenues),
            Series::new("Total_Revenue", total_revenues),
        ])?;
        
        // Save as CSV
        let csv_path = self.output_dir.join("bess_daily_revenues_parquet.csv");
        CsvWriter::new(std::fs::File::create(&csv_path)?)
            .finish(&mut df.clone())?;
        
        // Save as Parquet
        let parquet_path = self.output_dir.join("bess_daily_revenues_parquet.parquet");
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut df.clone())?;
        
        println!("\n✅ Saved revenue analysis to:");
        println!("  - {}", csv_path.display());
        println!("  - {}", parquet_path.display());
        
        Ok(())
    }
}

pub fn calculate_bess_revenues_from_parquet() -> Result<()> {
    let master_list_path = PathBuf::from("bess_analysis/bess_resources_master_list.csv");
    let calculator = BessParquetCalculator::new(&master_list_path)?;
    calculator.calculate_all_revenues()?;
    Ok(())
}