use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, Timelike};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct BessResource {
    pub name: String,
    pub settlement_point: String,
    pub capacity_mw: f64,
    pub duration_hours: f64,  // Assumed 2 hours if not specified
}

#[derive(Debug, Clone)]
pub struct DailyRevenue {
    pub resource_name: String,
    pub date: NaiveDate,
    pub dam_energy_revenue: f64,
    pub rt_energy_revenue: f64,
    pub net_energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub rrs_revenue: f64,
    pub ecrs_revenue: f64,
    pub non_spin_revenue: f64,
    pub total_as_revenue: f64,
    pub total_revenue: f64,
    pub capacity_factor: f64,
    pub cycles: f64,
}

#[derive(Debug, Clone)]
pub struct AnnualSummary {
    pub resource_name: String,
    pub year: i32,
    pub capacity_mw: f64,
    pub total_revenue: f64,
    pub energy_revenue: f64,
    pub as_revenue: f64,
    pub revenue_per_mw: f64,
    pub revenue_per_mwh: f64,
    pub capacity_factor: f64,
    pub cycles: f64,
}

pub struct BessComprehensiveCalculator {
    bess_resources: HashMap<String, BessResource>,
    annual_output_dir: PathBuf,
    disclosure_data_dir: PathBuf,
    output_dir: PathBuf,
}

impl BessComprehensiveCalculator {
    pub fn new(
        bess_master_list_path: &Path,
        annual_output_dir: PathBuf,
        disclosure_data_dir: PathBuf,
    ) -> Result<Self> {
        // Create output directory
        let output_dir = PathBuf::from("bess_comprehensive_analysis");
        std::fs::create_dir_all(&output_dir)?;

        // Load BESS resources
        let bess_resources = Self::load_bess_resources(bess_master_list_path)?;
        
        println!("✅ Loaded {} BESS resources", bess_resources.len());
        
        Ok(Self {
            bess_resources,
            annual_output_dir,
            disclosure_data_dir,
            output_dir,
        })
    }

    fn load_bess_resources(path: &Path) -> Result<HashMap<String, BessResource>> {
        let file = std::fs::File::open(path)?;
        let df = CsvReader::new(file)
            .has_header(true)
            .finish()?;
        
        let mut resources = HashMap::new();
        
        let names = df.column("Resource_Name")?.utf8()?;
        let settlement_points = df.column("Settlement_Point")?.utf8()?;
        let capacities = df.column("Max_Capacity_MW")?.f64()?;
        
        for i in 0..df.height() {
            if let (Some(name), Some(sp), Some(capacity)) = 
                (names.get(i), settlement_points.get(i), capacities.get(i)) {
                
                resources.insert(name.to_string(), BessResource {
                    name: name.to_string(),
                    settlement_point: sp.to_string(),
                    capacity_mw: capacity,
                    duration_hours: 2.0,  // Default assumption
                });
            }
        }
        
        Ok(resources)
    }

    pub fn calculate_all_revenues(&self) -> Result<()> {
        println!("\n💰 ERCOT BESS Comprehensive Revenue Analysis");
        println!("{}", "=".repeat(80));
        
        // Get available years from parquet files
        let years = self.get_available_years()?;
        println!("📅 Available years: {:?}", years);
        
        let mut all_annual_summaries = Vec::new();
        
        for year in years {
            println!("\n📊 Processing year {}", year);
            
            // Calculate revenues for this year
            let annual_summaries = self.calculate_year_revenues(year)?;
            all_annual_summaries.extend(annual_summaries);
        }
        
        // Generate reports
        self.generate_comprehensive_report(&all_annual_summaries)?;
        self.save_annual_summaries(&all_annual_summaries)?;
        
        Ok(())
    }

    fn get_available_years(&self) -> Result<Vec<i32>> {
        let mut years = std::collections::HashSet::new();
        
        // Check Settlement Point Prices (RT data)
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

    fn calculate_year_revenues(&self, year: i32) -> Result<Vec<AnnualSummary>> {
        println!("  Loading price data for {}...", year);
        
        // Load RT prices
        let rt_prices = self.load_rt_prices(year)?;
        println!("    ✅ Loaded {} RT price records", rt_prices.len());
        
        // Load DAM prices
        let dam_prices = self.load_dam_prices(year)?;
        println!("    ✅ Loaded {} DAM price records", dam_prices.len());
        
        // Load ancillary service clearing prices if available
        let as_prices = self.load_ancillary_prices(year)?;
        println!("    ✅ Loaded {} AS price records", as_prices.len());
        
        // Load BESS dispatch data from 60-day disclosures
        let bess_dispatch = self.load_bess_dispatch_data(year)?;
        println!("    ✅ Loaded dispatch data for {} resources", bess_dispatch.len());
        
        // Calculate revenues for each BESS resource
        let mut annual_summaries = Vec::new();
        
        for (resource_name, resource) in &self.bess_resources {
            // Check if we have dispatch data for this resource
            if let Some(dispatch_data) = bess_dispatch.get(resource_name) {
                let summary = self.calculate_resource_annual_revenue(
                    resource,
                    year,
                    &rt_prices,
                    &dam_prices,
                    &as_prices,
                    dispatch_data,
                )?;
                
                annual_summaries.push(summary);
            }
        }
        
        Ok(annual_summaries)
    }

    fn load_rt_prices(&self, year: i32) -> Result<HashMap<(String, NaiveDate, u32), f64>> {
        let mut prices = HashMap::new();
        
        let file_path = self.annual_output_dir
            .join("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones")
            .join(format!("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_{}.parquet", year));
        
        if !file_path.exists() {
            return Ok(prices);
        }
        
        let file = std::fs::File::open(&file_path)?;
        let df = ParquetReader::new(file).finish()?;
        
        // Expected columns: SCEDTimestamp, SettlementPoint, SettlementPointPrice
        if let (Ok(timestamps), Ok(sps), Ok(prices_col)) = (
            df.column("SCEDTimestamp"),
            df.column("SettlementPoint"),
            df.column("SettlementPointPrice")
        ) {
            let timestamps_str = timestamps.utf8()?;
            let sps_str = sps.utf8()?;
            let prices_f64 = prices_col.f64()?;
            
            for i in 0..df.height() {
                if let (Some(timestamp_str), Some(sp), Some(price)) = 
                    (timestamps_str.get(i), sps_str.get(i), prices_f64.get(i)) {
                    
                    // Parse timestamp
                    if let Ok(timestamp) = NaiveDateTime::parse_from_str(timestamp_str, "%m/%d/%Y %H:%M:%S") {
                        let date = timestamp.date();
                        let interval = (timestamp.hour() * 60 + timestamp.minute()) / 5; // 5-minute intervals
                        
                        prices.insert((sp.to_string(), date, interval), price);
                    }
                }
            }
        }
        
        Ok(prices)
    }

    fn load_dam_prices(&self, year: i32) -> Result<HashMap<(String, NaiveDate, u32), f64>> {
        let mut prices = HashMap::new();
        
        // Try DAM LMP file
        let file_path = self.annual_output_dir
            .join("DAM_Hourly_LMPs_BusLevel")
            .join(format!("DAM_Hourly_LMPs_BusLevel_{}.parquet", year));
        
        if !file_path.exists() {
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
            let hours_i64 = hours.i64()?;
            let buses_str = buses.utf8()?;
            let lmps_f64 = lmps.f64()?;
            
            for i in 0..df.height() {
                if let (Some(date_str), Some(hour), Some(bus), Some(lmp)) = 
                    (dates_str.get(i), hours_i64.get(i), buses_str.get(i), lmps_f64.get(i)) {
                    
                    // Parse date
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                        prices.insert((bus.to_string(), date, hour as u32), lmp);
                    }
                }
            }
        }
        
        Ok(prices)
    }

    fn load_ancillary_prices(&self, _year: i32) -> Result<HashMap<(NaiveDate, u32, String), f64>> {
        let prices = HashMap::new();
        
        // TODO: Load ancillary service clearing prices when available
        // For now, we'll use placeholder values from the disclosure data
        
        Ok(prices)
    }

    fn load_bess_dispatch_data(&self, _year: i32) -> Result<HashMap<String, Vec<BessDispatch>>> {
        let mut dispatch_map = HashMap::new();
        
        // Load from 60-day disclosure data
        // This would include:
        // - DAM Gen Resource Data (for DAM awards)
        // - SCED Gen Resource Data (for RT dispatch)
        // - AS awards from DAM files
        
        // For now, create simplified dispatch data
        // In production, this would read actual disclosure files
        for (resource_name, _resource) in &self.bess_resources {
            dispatch_map.insert(resource_name.clone(), Vec::new());
        }
        
        Ok(dispatch_map)
    }

    fn calculate_resource_annual_revenue(
        &self,
        resource: &BessResource,
        year: i32,
        _rt_prices: &HashMap<(String, NaiveDate, u32), f64>,
        dam_prices: &HashMap<(String, NaiveDate, u32), f64>,
        _as_prices: &HashMap<(NaiveDate, u32, String), f64>,
        _dispatch_data: &[BessDispatch],
    ) -> Result<AnnualSummary> {
        let mut total_energy_revenue = 0.0;
        let total_as_revenue;
        let mut total_cycles = 0.0;
        
        // Simple energy arbitrage calculation
        // Assume charging during low-price hours and discharging during high-price hours
        let start_date = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
        
        let mut current_date = start_date;
        while current_date <= end_date {
            // Get DAM prices for this day
            let mut daily_dam_prices = Vec::new();
            for hour in 1..=24 {
                if let Some(&price) = dam_prices.get(&(resource.settlement_point.clone(), current_date, hour)) {
                    daily_dam_prices.push((hour, price));
                }
            }
            
            if daily_dam_prices.len() >= 20 {
                // Sort by price
                daily_dam_prices.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                
                // Charge during lowest price hours (assuming 2-hour duration)
                let charge_hours = &daily_dam_prices[0..2];
                let discharge_hours = &daily_dam_prices[daily_dam_prices.len()-2..];
                
                let avg_charge_price = charge_hours.iter().map(|(_, p)| p).sum::<f64>() / 2.0;
                let avg_discharge_price = discharge_hours.iter().map(|(_, p)| p).sum::<f64>() / 2.0;
                
                // Simple arbitrage calculation (90% round-trip efficiency)
                let daily_revenue = resource.capacity_mw * 2.0 * (avg_discharge_price * 0.95 - avg_charge_price / 0.95);
                
                if daily_revenue > 0.0 {
                    total_energy_revenue += daily_revenue;
                    total_cycles += 1.0;
                }
            }
            
            current_date = current_date.succ_opt().unwrap();
        }
        
        // Add ancillary service revenue (placeholder - would use actual AS awards)
        // Assume 20% of capacity participates in AS markets at $10/MW average
        total_as_revenue = resource.capacity_mw * 0.2 * 10.0 * 365.0 * 24.0;
        
        let total_revenue = total_energy_revenue + total_as_revenue;
        let revenue_per_mw = if resource.capacity_mw > 0.0 { total_revenue / resource.capacity_mw } else { 0.0 };
        let revenue_per_mwh = if resource.capacity_mw > 0.0 && resource.duration_hours > 0.0 {
            total_revenue / (resource.capacity_mw * resource.duration_hours)
        } else {
            0.0
        };
        
        Ok(AnnualSummary {
            resource_name: resource.name.clone(),
            year,
            capacity_mw: resource.capacity_mw,
            total_revenue,
            energy_revenue: total_energy_revenue,
            as_revenue: total_as_revenue,
            revenue_per_mw,
            revenue_per_mwh,
            capacity_factor: total_cycles / 365.0,
            cycles: total_cycles,
        })
    }

    fn generate_comprehensive_report(&self, summaries: &[AnnualSummary]) -> Result<()> {
        println!("\n📊 Generating Comprehensive BESS Revenue Report");
        
        // Group by year
        let mut by_year: HashMap<i32, Vec<&AnnualSummary>> = HashMap::new();
        for summary in summaries {
            by_year.entry(summary.year).or_insert_with(Vec::new).push(summary);
        }
        
        // Generate year-by-year analysis
        for (year, year_summaries) in by_year.iter() {
            println!("\n🗓️  Year {} Summary:", year);
            
            let total_capacity: f64 = year_summaries.iter().map(|s| s.capacity_mw).sum();
            let total_revenue: f64 = year_summaries.iter().map(|s| s.total_revenue).sum();
            let total_energy: f64 = year_summaries.iter().map(|s| s.energy_revenue).sum();
            let total_as: f64 = year_summaries.iter().map(|s| s.as_revenue).sum();
            
            println!("  Total Capacity: {:.1} MW", total_capacity);
            println!("  Total Revenue: ${:.2}M", total_revenue / 1_000_000.0);
            println!("  Energy Revenue: ${:.2}M ({:.1}%)", 
                total_energy / 1_000_000.0, 
                (total_energy / total_revenue) * 100.0);
            println!("  AS Revenue: ${:.2}M ({:.1}%)", 
                total_as / 1_000_000.0,
                (total_as / total_revenue) * 100.0);
            println!("  Average $/MW-year: ${:.0}", total_revenue / total_capacity);
            
            // Top performers
            let mut sorted = year_summaries.clone();
            sorted.sort_by(|a, b| b.revenue_per_mw.partial_cmp(&a.revenue_per_mw).unwrap());
            
            println!("\n  Top 5 Performers:");
            for (i, summary) in sorted.iter().take(5).enumerate() {
                println!("    {}. {} - ${:.0}/MW-year", 
                    i + 1, 
                    summary.resource_name,
                    summary.revenue_per_mw);
            }
        }
        
        Ok(())
    }

    fn save_annual_summaries(&self, summaries: &[AnnualSummary]) -> Result<()> {
        // Convert to DataFrame
        let mut resource_names = Vec::new();
        let mut years = Vec::new();
        let mut capacities = Vec::new();
        let mut total_revenues = Vec::new();
        let mut energy_revenues = Vec::new();
        let mut as_revenues = Vec::new();
        let mut revenues_per_mw = Vec::new();
        let mut revenues_per_mwh = Vec::new();
        let mut capacity_factors = Vec::new();
        let mut cycles_vec = Vec::new();
        
        for summary in summaries {
            resource_names.push(summary.resource_name.clone());
            years.push(summary.year);
            capacities.push(summary.capacity_mw);
            total_revenues.push(summary.total_revenue);
            energy_revenues.push(summary.energy_revenue);
            as_revenues.push(summary.as_revenue);
            revenues_per_mw.push(summary.revenue_per_mw);
            revenues_per_mwh.push(summary.revenue_per_mwh);
            capacity_factors.push(summary.capacity_factor);
            cycles_vec.push(summary.cycles);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", resource_names),
            Series::new("Year", years),
            Series::new("Capacity_MW", capacities),
            Series::new("Total_Revenue", total_revenues),
            Series::new("Energy_Revenue", energy_revenues),
            Series::new("AS_Revenue", as_revenues),
            Series::new("Revenue_Per_MW", revenues_per_mw),
            Series::new("Revenue_Per_MWh", revenues_per_mwh),
            Series::new("Capacity_Factor", capacity_factors),
            Series::new("Cycles", cycles_vec),
        ])?;
        
        // Save as CSV
        let csv_path = self.output_dir.join("bess_annual_revenue_summary.csv");
        CsvWriter::new(std::fs::File::create(&csv_path)?)
            .finish(&mut df.clone())?;
        
        // Save as Parquet
        let parquet_path = self.output_dir.join("bess_annual_revenue_summary.parquet");
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut df.clone())?;
        
        println!("\n✅ Saved comprehensive analysis to:");
        println!("  - {}", csv_path.display());
        println!("  - {}", parquet_path.display());
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct BessDispatch {
    timestamp: NaiveDateTime,
    dispatch_mw: f64,
    service_type: String,
}

pub fn run_comprehensive_bess_analysis() -> Result<()> {
    let master_list_path = PathBuf::from("bess_analysis/bess_resources_master_list.csv");
    let annual_output_dir = PathBuf::from("annual_output");
    let disclosure_data_dir = PathBuf::from("disclosure_data");
    
    let calculator = BessComprehensiveCalculator::new(
        &master_list_path,
        annual_output_dir,
        disclosure_data_dir,
    )?;
    
    calculator.calculate_all_revenues()?;
    
    Ok(())
}