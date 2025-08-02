use anyhow::{Result, Context};
use chrono::{NaiveDate, NaiveDateTime, Datelike, Timelike};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;

#[derive(Debug, Clone)]
pub struct BessResource {
    pub name: String,
    pub settlement_point: String,
    pub capacity_mw: f64,
    pub qse: String,
}

#[derive(Debug, Clone)]
pub struct BessAnnualRevenue {
    pub resource_name: String,
    pub year: i32,
    pub rt_energy_revenue: f64,
    pub dam_energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub spin_revenue: f64,      // RRS (all types)
    pub non_spin_revenue: f64,
    pub ecrs_revenue: f64,
    pub total_revenue: f64,
}

pub struct BessCompleteAnalyzer {
    dam_disclosure_dir: PathBuf,
    sced_disclosure_dir: PathBuf,
    price_data_dir: PathBuf,
    output_dir: PathBuf,
    bess_resources: HashMap<String, BessResource>,
}

impl BessCompleteAnalyzer {
    pub fn new() -> Result<Self> {
        // Set up paths
        let dam_disclosure_dir = PathBuf::from("/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv");
        let sced_disclosure_dir = PathBuf::from("/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv");
        let price_data_dir = PathBuf::from("annual_output");
        let output_dir = PathBuf::from("bess_complete_analysis");
        
        std::fs::create_dir_all(&output_dir)?;
        
        // Load BESS resources
        let bess_resources = Self::load_bess_resources()?;
        println!("📋 Loaded {} BESS resources", bess_resources.len());
        
        Ok(Self {
            dam_disclosure_dir,
            sced_disclosure_dir,
            price_data_dir,
            output_dir,
            bess_resources,
        })
    }
    
    fn load_bess_resources() -> Result<HashMap<String, BessResource>> {
        let mut resources = HashMap::new();
        
        let master_list_path = PathBuf::from("bess_analysis/bess_resources_master_list.csv");
        if master_list_path.exists() {
            let file = std::fs::File::open(&master_list_path)?;
            let df = CsvReader::new(file).has_header(true).finish()?;
            
            let names = df.column("Resource_Name")?.utf8()?;
            let settlement_points = df.column("Settlement_Point")?.utf8()?;
            let capacities = df.column("Max_Capacity_MW")?.f64()?;
            let qses = df.column("QSE").ok().and_then(|c| c.utf8().ok());
            
            for i in 0..df.height() {
                if let (Some(name), Some(sp), Some(capacity)) = 
                    (names.get(i), settlement_points.get(i), capacities.get(i)) {
                    
                    let qse = qses.as_ref().and_then(|q| q.get(i)).unwrap_or("UNKNOWN");
                    
                    resources.insert(name.to_string(), BessResource {
                        name: name.to_string(),
                        settlement_point: sp.to_string(),
                        capacity_mw: capacity,
                        qse: qse.to_string(),
                    });
                }
            }
        }
        
        Ok(resources)
    }
    
    pub fn analyze_all_years(&self) -> Result<()> {
        println!("\n💰 ERCOT BESS Complete Revenue Analysis");
        println!("{}", "=".repeat(80));
        
        // Get available years from disclosure files
        let years = self.get_available_years()?;
        println!("\n📅 Processing years: {:?}", years);
        
        let mut all_revenues = Vec::new();
        
        for year in years {
            println!("\n📊 Processing year {}", year);
            let year_revenues = self.process_year(year)?;
            all_revenues.extend(year_revenues);
        }
        
        // Save results
        self.save_results(&all_revenues)?;
        self.generate_summary_report(&all_revenues)?;
        
        Ok(())
    }
    
    fn get_available_years(&self) -> Result<Vec<i32>> {
        let mut years = std::collections::HashSet::new();
        
        // Check DAM files
        let dam_pattern = self.dam_disclosure_dir.join("*Gen_Resource_Data*.csv");
        let dam_files: Vec<PathBuf> = glob::glob(dam_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        for file in dam_files {
            if let Some(year) = Self::extract_year_from_filename(&file) {
                years.insert(year);
            }
        }
        
        let mut years_vec: Vec<i32> = years.into_iter().collect();
        years_vec.sort();
        Ok(years_vec)
    }
    
    fn extract_year_from_filename(path: &Path) -> Option<i32> {
        let filename = path.file_name()?.to_str()?;
        
        // Try to find year in format DD-MMM-YY
        let parts: Vec<&str> = filename.split('-').collect();
        if parts.len() >= 3 {
            if let Some(year_part) = parts.last() {
                let year_str = year_part.trim_end_matches(".csv");
                if let Ok(year) = year_str.parse::<i32>() {
                    // Convert 2-digit year to 4-digit
                    if year < 100 {
                        return Some(if year < 50 { 2000 + year } else { 1900 + year });
                    }
                    return Some(year);
                }
            }
        }
        
        None
    }
    
    fn process_year(&self, year: i32) -> Result<Vec<BessAnnualRevenue>> {
        let mut annual_revenues = HashMap::new();
        
        // Initialize revenues for all BESS resources
        for (name, _resource) in &self.bess_resources {
            annual_revenues.insert(name.clone(), BessAnnualRevenue {
                resource_name: name.clone(),
                year,
                rt_energy_revenue: 0.0,
                dam_energy_revenue: 0.0,
                reg_up_revenue: 0.0,
                reg_down_revenue: 0.0,
                spin_revenue: 0.0,
                non_spin_revenue: 0.0,
                ecrs_revenue: 0.0,
                total_revenue: 0.0,
            });
        }
        
        // Process DAM data
        self.process_dam_data(year, &mut annual_revenues)?;
        
        // Process RT (SCED) data
        self.process_rt_data(year, &mut annual_revenues)?;
        
        // Calculate totals
        let mut results = Vec::new();
        for (_, mut revenue) in annual_revenues {
            revenue.total_revenue = revenue.rt_energy_revenue + revenue.dam_energy_revenue +
                revenue.reg_up_revenue + revenue.reg_down_revenue + revenue.spin_revenue +
                revenue.non_spin_revenue + revenue.ecrs_revenue;
            results.push(revenue);
        }
        
        Ok(results)
    }
    
    fn process_dam_data(&self, year: i32, annual_revenues: &mut HashMap<String, BessAnnualRevenue>) -> Result<()> {
        let pattern = format!("*DAM_Gen_Resource_Data*{:02}.csv", year % 100);
        let dam_files: Vec<PathBuf> = glob::glob(self.dam_disclosure_dir.join(&pattern).to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("  Processing {} DAM files", dam_files.len());
        
        let pb = ProgressBar::new(dam_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        for file in dam_files {
            pb.inc(1);
            self.process_dam_file(&file, annual_revenues)?;
        }
        
        pb.finish();
        Ok(())
    }
    
    fn process_dam_file(&self, file: &Path, annual_revenues: &mut HashMap<String, BessAnnualRevenue>) -> Result<()> {
        let df = CsvReader::new(std::fs::File::open(file)?).has_header(true).finish()?;
        
        // Filter for BESS resources
        if let Ok(resource_types) = df.column("Resource Type") {
            let mask = resource_types.utf8()?.equal("PWRSTR");
            
            if let Ok(filtered) = df.filter(&mask) {
                // Process energy awards
                if let (Ok(resources), Ok(awards), Ok(prices)) = (
                    filtered.column("Resource Name"),
                    filtered.column("Awarded Quantity"),
                    filtered.column("Energy Settlement Point Price")
                ) {
                    let resources_str = resources.utf8()?;
                    let awards_f64 = Self::parse_numeric_column(awards)?;
                    let prices_f64 = Self::parse_numeric_column(prices)?;
                    
                    for i in 0..filtered.height() {
                        if let (Some(resource), Some(award), Some(price)) = 
                            (resources_str.get(i), awards_f64.get(i), prices_f64.get(i)) {
                            
                            if let Some(revenue) = annual_revenues.get_mut(resource) {
                                revenue.dam_energy_revenue += award * price;
                            }
                        }
                    }
                }
                
                // Process AS awards
                self.process_dam_as_awards(&filtered, annual_revenues)?;
            }
        }
        
        Ok(())
    }
    
    fn process_dam_as_awards(&self, df: &DataFrame, annual_revenues: &mut HashMap<String, BessAnnualRevenue>) -> Result<()> {
        let resources = df.column("Resource Name")?.utf8()?;
        
        // RegUp
        if let (Ok(awards), Ok(prices)) = (
            df.column("RegUp Awarded"),
            df.column("RegUp MCPC")
        ) {
            let awards_f64 = Self::parse_numeric_column(awards)?;
            let prices_f64 = Self::parse_numeric_column(prices)?;
            
            for i in 0..df.height() {
                if let (Some(resource), Some(award), Some(price)) = 
                    (resources.get(i), awards_f64.get(i), prices_f64.get(i)) {
                    
                    if let Some(revenue) = annual_revenues.get_mut(resource) {
                        revenue.reg_up_revenue += award * price;
                    }
                }
            }
        }
        
        // RegDown
        if let (Ok(awards), Ok(prices)) = (
            df.column("RegDown Awarded"),
            df.column("RegDown MCPC")
        ) {
            let awards_f64 = Self::parse_numeric_column(awards)?;
            let prices_f64 = Self::parse_numeric_column(prices)?;
            
            for i in 0..df.height() {
                if let (Some(resource), Some(award), Some(price)) = 
                    (resources.get(i), awards_f64.get(i), prices_f64.get(i)) {
                    
                    if let Some(revenue) = annual_revenues.get_mut(resource) {
                        revenue.reg_down_revenue += award * price;
                    }
                }
            }
        }
        
        // RRS (combines RRSPFR, RRSFFR, RRSUFR)
        let mut rrs_total_awards = vec![0.0; df.height()];
        for rrs_type in ["RRSPFR Awarded", "RRSFFR Awarded", "RRSUFR Awarded"] {
            if let Ok(awards) = df.column(rrs_type) {
                let awards_f64 = Self::parse_numeric_column(awards)?;
                for i in 0..df.height() {
                    if let Some(award) = awards_f64.get(i) {
                        rrs_total_awards[i] += award;
                    }
                }
            }
        }
        
        if let Ok(prices) = df.column("RRS MCPC") {
            let prices_f64 = Self::parse_numeric_column(prices)?;
            
            for i in 0..df.height() {
                if let (Some(resource), Some(price)) = (resources.get(i), prices_f64.get(i)) {
                    if let Some(revenue) = annual_revenues.get_mut(resource) {
                        revenue.spin_revenue += rrs_total_awards[i] * price;
                    }
                }
            }
        }
        
        // ECRS
        if let (Ok(awards), Ok(prices)) = (
            df.column("ECRSSD Awarded"),
            df.column("ECRS MCPC")
        ) {
            let awards_f64 = Self::parse_numeric_column(awards)?;
            let prices_f64 = Self::parse_numeric_column(prices)?;
            
            for i in 0..df.height() {
                if let (Some(resource), Some(award), Some(price)) = 
                    (resources.get(i), awards_f64.get(i), prices_f64.get(i)) {
                    
                    if let Some(revenue) = annual_revenues.get_mut(resource) {
                        revenue.ecrs_revenue += award * price;
                    }
                }
            }
        }
        
        // NonSpin
        if let (Ok(awards), Ok(prices)) = (
            df.column("NonSpin Awarded"),
            df.column("NonSpin MCPC")
        ) {
            let awards_f64 = Self::parse_numeric_column(awards)?;
            let prices_f64 = Self::parse_numeric_column(prices)?;
            
            for i in 0..df.height() {
                if let (Some(resource), Some(award), Some(price)) = 
                    (resources.get(i), awards_f64.get(i), prices_f64.get(i)) {
                    
                    if let Some(revenue) = annual_revenues.get_mut(resource) {
                        revenue.non_spin_revenue += award * price;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn process_rt_data(&self, year: i32, annual_revenues: &mut HashMap<String, BessAnnualRevenue>) -> Result<()> {
        let pattern = format!("*SCED_Gen_Resource_Data*{:02}.csv", year % 100);
        let sced_files: Vec<PathBuf> = glob::glob(self.sced_disclosure_dir.join(&pattern).to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("  Processing {} SCED files", sced_files.len());
        
        // Load RT prices from Parquet files
        let rt_prices = self.load_rt_prices(year)?;
        println!("    Loaded {} RT price points", rt_prices.len());
        
        let pb = ProgressBar::new(sced_files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        for file in sced_files {
            pb.inc(1);
            self.process_sced_file(&file, &rt_prices, annual_revenues)?;
        }
        
        pb.finish();
        Ok(())
    }
    
    fn load_rt_prices(&self, year: i32) -> Result<HashMap<(String, NaiveDateTime), f64>> {
        let mut prices = HashMap::new();
        
        let file_path = self.price_data_dir
            .join("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones")
            .join(format!("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_{}.parquet", year));
        
        if !file_path.exists() {
            return Ok(prices);
        }
        
        let file = std::fs::File::open(&file_path)?;
        let df = ParquetReader::new(file).finish()?;
        
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
            
            for i in 0..df.height().min(50_000_000) {  // Limit for memory
                if let (Some(date_str), Some(hour), Some(interval), Some(sp), Some(price)) = 
                    (dates_str.get(i), hours_i64.get(i), intervals_i64.get(i), sps_str.get(i), prices_f64.get(i)) {
                    
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                        // Create timestamp for 15-minute interval
                        // RT prices are 15-minute intervals, not 5-minute
                        let minutes = (interval - 1) * 15;  // intervals are 1-4 within each hour
                        if let Some(timestamp) = date.and_hms_opt(hour as u32, minutes as u32, 0) {
                            prices.insert((sp.to_string(), timestamp), price);
                        }
                    }
                }
            }
        }
        
        Ok(prices)
    }
    
    fn process_sced_file(&self, file: &Path, rt_prices: &HashMap<(String, NaiveDateTime), f64>, 
                         annual_revenues: &mut HashMap<String, BessAnnualRevenue>) -> Result<()> {
        let df = CsvReader::new(std::fs::File::open(file)?).has_header(true).finish()?;
        
        // Filter for BESS resources
        if let Ok(resource_types) = df.column("Resource Type") {
            let mask = resource_types.utf8()?.equal("PWRSTR");
            
            if let Ok(filtered) = df.filter(&mask) {
                // Get base point (dispatch) data
                if let (Ok(timestamps), Ok(resources), Ok(base_points)) = (
                    filtered.column("SCED Time Stamp"),
                    filtered.column("Resource Name"),
                    filtered.column("Base Point")
                ) {
                    let timestamps_str = timestamps.utf8()?;
                    let resources_str = resources.utf8()?;
                    let base_points_f64 = Self::parse_numeric_column(base_points)?;
                    
                    for i in 0..filtered.height() {
                        if let (Some(timestamp_str), Some(resource_name), Some(base_point)) = 
                            (timestamps_str.get(i), resources_str.get(i), base_points_f64.get(i)) {
                            
                            // Parse timestamp
                            if let Ok(timestamp) = NaiveDateTime::parse_from_str(timestamp_str, "%m/%d/%Y %H:%M:%S") {
                                // Get price for this interval
                                if let Some(resource) = self.bess_resources.get(resource_name) {
                                    let price_key = (resource.settlement_point.clone(), timestamp);
                                    
                                    if let Some(&price) = rt_prices.get(&price_key) {
                                        if let Some(revenue) = annual_revenues.get_mut(resource_name) {
                                            // RT revenue = MW * $/MWh * hours
                                            // SCED data is 5-minute, but RT prices are 15-minute
                                            // Use 5-minute duration for SCED dispatch
                                            revenue.rt_energy_revenue += base_point * price * (5.0 / 60.0);
                                        }
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
    
    fn parse_numeric_column(series: &Series) -> Result<Float64Chunked> {
        if let Ok(f64_col) = series.f64() {
            Ok(f64_col.clone())
        } else if let Ok(utf8_col) = series.utf8() {
            // Convert string to float, handling empty strings and NaN
            let values: Vec<Option<f64>> = utf8_col.into_iter()
                .map(|v| v.and_then(|s| {
                    if s.is_empty() || s == "NaN" { 
                        Some(0.0) 
                    } else { 
                        s.parse().ok() 
                    }
                }))
                .collect();
            Ok(Float64Chunked::from_iter(values))
        } else {
            // Return zeros if can't parse
            Ok(Float64Chunked::from_iter(vec![Some(0.0); series.len()]))
        }
    }
    
    fn save_results(&self, revenues: &[BessAnnualRevenue]) -> Result<()> {
        // Convert to DataFrame
        let mut resource_names = Vec::new();
        let mut years = Vec::new();
        let mut rt_energy = Vec::new();
        let mut dam_energy = Vec::new();
        let mut reg_up = Vec::new();
        let mut reg_down = Vec::new();
        let mut spin = Vec::new();
        let mut non_spin = Vec::new();
        let mut ecrs = Vec::new();
        let mut total = Vec::new();
        
        for rev in revenues {
            resource_names.push(rev.resource_name.clone());
            years.push(rev.year);
            rt_energy.push(rev.rt_energy_revenue);
            dam_energy.push(rev.dam_energy_revenue);
            reg_up.push(rev.reg_up_revenue);
            reg_down.push(rev.reg_down_revenue);
            spin.push(rev.spin_revenue);
            non_spin.push(rev.non_spin_revenue);
            ecrs.push(rev.ecrs_revenue);
            total.push(rev.total_revenue);
        }
        
        let df = DataFrame::new(vec![
            Series::new("BESS_Asset_Name", resource_names),
            Series::new("Year", years),
            Series::new("RT_Revenue", rt_energy),
            Series::new("DA_Revenue", dam_energy),
            Series::new("RegUp_Revenue", reg_up),
            Series::new("RegDown_Revenue", reg_down),
            Series::new("Spin_Revenue", spin),
            Series::new("NonSpin_Revenue", non_spin),
            Series::new("ECRS_Revenue", ecrs),
            Series::new("Total_Revenue", total),
        ])?;
        
        // Save as CSV
        let csv_path = self.output_dir.join("bess_annual_revenues_complete.csv");
        CsvWriter::new(std::fs::File::create(&csv_path)?)
            .finish(&mut df.clone())?;
        
        // Save as Parquet
        let parquet_path = self.output_dir.join("bess_annual_revenues_complete.parquet");
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .finish(&mut df.clone())?;
        
        println!("\n✅ Saved results to:");
        println!("  - {}", csv_path.display());
        println!("  - {}", parquet_path.display());
        
        Ok(())
    }
    
    fn generate_summary_report(&self, revenues: &[BessAnnualRevenue]) -> Result<()> {
        println!("\n📊 BESS Revenue Summary by Year");
        println!("{}", "=".repeat(80));
        
        // Group by year
        let mut by_year: HashMap<i32, Vec<&BessAnnualRevenue>> = HashMap::new();
        for rev in revenues {
            by_year.entry(rev.year).or_insert_with(Vec::new).push(rev);
        }
        
        // Create summary table
        println!("\n{:<6} {:>15} {:>15} {:>15} {:>15} {:>15}", 
                 "Year", "Total($M)", "RT($M)", "DAM($M)", "AS($M)", "Resources");
        println!("{}", "-".repeat(90));
        
        for year in by_year.keys().cloned().collect::<Vec<_>>().into_iter().rev() {
            let year_revs = &by_year[&year];
            
            let total: f64 = year_revs.iter().map(|r| r.total_revenue).sum();
            let rt: f64 = year_revs.iter().map(|r| r.rt_energy_revenue).sum();
            let dam: f64 = year_revs.iter().map(|r| r.dam_energy_revenue).sum();
            let as_total: f64 = year_revs.iter()
                .map(|r| r.reg_up_revenue + r.reg_down_revenue + r.spin_revenue + r.non_spin_revenue + r.ecrs_revenue)
                .sum();
            
            let active_resources = year_revs.iter()
                .filter(|r| r.total_revenue > 0.0)
                .count();
            
            println!("{:<6} {:>15.2} {:>15.2} {:>15.2} {:>15.2} {:>15}", 
                     year, 
                     total / 1_000_000.0,
                     rt / 1_000_000.0,
                     dam / 1_000_000.0,
                     as_total / 1_000_000.0,
                     active_resources);
        }
        
        Ok(())
    }
}

pub fn run_complete_bess_analysis() -> Result<()> {
    let analyzer = BessCompleteAnalyzer::new()?;
    analyzer.analyze_all_years()?;
    Ok(())
}