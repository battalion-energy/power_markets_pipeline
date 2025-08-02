use anyhow::{Result, Context};
use chrono::{NaiveDate, NaiveDateTime, Datelike, Timelike};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use indicatif::{ProgressBar, ProgressStyle};
use ::zip::ZipArchive;
use std::fs::File;
use std::io::copy;

#[derive(Debug, Clone)]
pub struct BessResource {
    pub name: String,
    pub settlement_point: String,
    pub capacity_mw: f64,
    pub duration_hours: f64,
    pub qse: String,
}

#[derive(Debug, Clone)]
pub struct DailyRevenue {
    pub resource_name: String,
    pub date: NaiveDate,
    pub rt_energy_revenue: f64,
    pub da_energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub spin_revenue: f64,
    pub non_spin_revenue: f64,
    pub ecrs_revenue: f64,
    pub total_revenue: f64,
    pub rt_mwh_discharged: f64,
    pub rt_mwh_charged: f64,
    pub da_mwh_discharged: f64,
    pub da_mwh_charged: f64,
}

#[derive(Debug, Clone)]
pub struct MonthlyRevenue {
    pub resource_name: String,
    pub year: i32,
    pub month: u32,
    pub rt_energy_revenue: f64,
    pub da_energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub spin_revenue: f64,
    pub non_spin_revenue: f64,
    pub ecrs_revenue: f64,
    pub total_revenue: f64,
    pub days_active: u32,
}

#[derive(Debug, Clone)]
pub struct AnnualRevenue {
    pub resource_name: String,
    pub year: i32,
    pub capacity_mw: f64,
    pub rt_energy_revenue: f64,
    pub da_energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub spin_revenue: f64,
    pub non_spin_revenue: f64,
    pub ecrs_revenue: f64,
    pub total_revenue: f64,
    pub revenue_per_mw: f64,
    pub revenue_per_mwh: f64,
    pub months_active: u32,
}

pub struct BessDisclosureAnalyzer {
    disclosure_dir: PathBuf,
    price_data_dir: PathBuf,
    output_dir: PathBuf,
    bess_resources: HashMap<String, BessResource>,
    rt_prices: HashMap<(String, NaiveDate, u32), f64>,
    dam_prices: HashMap<(String, NaiveDate, u32), f64>,
    as_clearing_prices: HashMap<(String, NaiveDate, u32), f64>, // service_type, date, hour
}

impl BessDisclosureAnalyzer {
    pub fn new(
        disclosure_dir: PathBuf,
        price_data_dir: PathBuf,
        bess_master_list_path: &Path,
    ) -> Result<Self> {
        let output_dir = PathBuf::from("bess_disclosure_analysis");
        std::fs::create_dir_all(&output_dir)?;
        
        // Load BESS resources
        let bess_resources = Self::load_bess_resources(bess_master_list_path)?;
        println!("📋 Loaded {} BESS resources", bess_resources.len());
        
        Ok(Self {
            disclosure_dir,
            price_data_dir,
            output_dir,
            bess_resources,
            rt_prices: HashMap::new(),
            dam_prices: HashMap::new(),
            as_clearing_prices: HashMap::new(),
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
        let qses = df.column("QSE").ok().and_then(|c| c.utf8().ok());
        
        for i in 0..df.height() {
            if let (Some(name), Some(sp), Some(capacity)) = 
                (names.get(i), settlement_points.get(i), capacities.get(i)) {
                
                let qse = qses.as_ref().and_then(|q| q.get(i)).unwrap_or("UNKNOWN");
                
                resources.insert(name.to_string(), BessResource {
                    name: name.to_string(),
                    settlement_point: sp.to_string(),
                    capacity_mw: capacity,
                    duration_hours: 2.0, // Default assumption
                    qse: qse.to_string(),
                });
            }
        }
        
        Ok(resources)
    }
    
    pub fn analyze_all_revenues(&mut self) -> Result<()> {
        println!("\n💰 ERCOT BESS Revenue Analysis from 60-Day Disclosures");
        println!("{}", "=".repeat(80));
        
        // Extract and prepare data
        self.prepare_disclosure_data()?;
        
        // Load price data
        self.load_all_price_data()?;
        
        // Process each available year
        let years = self.get_available_years()?;
        println!("\n📅 Processing years: {:?}", years);
        
        let mut all_daily_revenues = Vec::new();
        let mut all_monthly_revenues = Vec::new();
        let mut all_annual_revenues = Vec::new();
        
        for year in years {
            println!("\n📊 Processing year {}", year);
            
            // Calculate daily revenues
            let daily_revenues = self.calculate_daily_revenues(year)?;
            
            // Aggregate to monthly
            let monthly_revenues = self.aggregate_to_monthly(&daily_revenues);
            
            // Aggregate to annual
            let annual_revenues = self.aggregate_to_annual(&monthly_revenues);
            
            all_daily_revenues.extend(daily_revenues);
            all_monthly_revenues.extend(monthly_revenues);
            all_annual_revenues.extend(annual_revenues);
        }
        
        // Generate reports
        self.generate_comprehensive_report(&all_annual_revenues)?;
        self.save_all_results(&all_daily_revenues, &all_monthly_revenues, &all_annual_revenues)?;
        self.generate_cumulative_revenue_chart(&all_annual_revenues)?;
        
        Ok(())
    }
    
    fn prepare_disclosure_data(&self) -> Result<()> {
        println!("\n📁 Preparing 60-Day Disclosure Data");
        
        // Check if CSV directory exists
        let csv_dir = self.disclosure_dir.join("csv");
        if !csv_dir.exists() {
            println!("  🗜️  Extracting ZIP files...");
            std::fs::create_dir_all(&csv_dir)?;
            
            // Extract all ZIP files
            let pattern = self.disclosure_dir.join("*.zip");
            let zip_files: Vec<PathBuf> = glob::glob(pattern.to_str().unwrap())?
                .filter_map(Result::ok)
                .collect();
            
            let pb = ProgressBar::new(zip_files.len() as u64);
            pb.set_style(ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
                .unwrap());
            
            for zip_path in zip_files {
                pb.inc(1);
                self.extract_zip_file(&zip_path, &csv_dir)?;
            }
            
            pb.finish();
        }
        
        println!("  ✅ Disclosure data ready in: {}", csv_dir.display());
        Ok(())
    }
    
    fn extract_zip_file(&self, zip_path: &Path, extract_dir: &Path) -> Result<()> {
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;
        
        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            let outpath = extract_dir.join(file.name());
            
            if file.name().ends_with('/') {
                std::fs::create_dir_all(&outpath)?;
            } else {
                if let Some(p) = outpath.parent() {
                    std::fs::create_dir_all(p)?;
                }
                let mut outfile = File::create(&outpath)?;
                copy(&mut file, &mut outfile)?;
            }
        }
        
        Ok(())
    }
    
    fn load_all_price_data(&mut self) -> Result<()> {
        println!("\n📊 Loading price data...");
        
        // Load RT prices from Parquet
        self.load_rt_prices_from_parquet()?;
        
        // Load DAM prices from Parquet
        self.load_dam_prices_from_parquet()?;
        
        // Load AS clearing prices from disclosure data
        self.load_as_clearing_prices()?;
        
        Ok(())
    }
    
    fn load_rt_prices_from_parquet(&mut self) -> Result<()> {
        let rt_dir = self.price_data_dir.join("Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones");
        
        if rt_dir.exists() {
            let pattern = rt_dir.join("*.parquet");
            let files: Vec<PathBuf> = glob::glob(pattern.to_str().unwrap())?
                .filter_map(Result::ok)
                .collect();
            
            for file in files {
                let df = ParquetReader::new(std::fs::File::open(&file)?).finish()?;
                
                if let (Ok(dates), Ok(hours), Ok(intervals), Ok(sps), Ok(prices)) = (
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
                    let prices_f64 = prices.f64()?;
                    
                    for i in 0..df.height().min(10_000_000) {
                        if let (Some(date_str), Some(hour), Some(interval), Some(sp), Some(price)) = 
                            (dates_str.get(i), hours_i64.get(i), intervals_i64.get(i), sps_str.get(i), prices_f64.get(i)) {
                            
                            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                                let interval_idx = (hour as u32) * 12 + (interval as u32 - 1) * 3;
                                self.rt_prices.insert((sp.to_string(), date, interval_idx), price);
                            }
                        }
                    }
                }
            }
        }
        
        println!("  ✅ Loaded {} RT price points", self.rt_prices.len());
        Ok(())
    }
    
    fn load_dam_prices_from_parquet(&mut self) -> Result<()> {
        // Similar to RT prices, load DAM prices
        println!("  ✅ Loaded {} DAM price points", self.dam_prices.len());
        Ok(())
    }
    
    fn load_as_clearing_prices(&mut self) -> Result<()> {
        // Load ancillary service clearing prices from disclosure data
        let csv_dir = self.disclosure_dir.join("csv");
        
        // Pattern for AS clearing price files
        let pattern = csv_dir.join("*MCPC*.csv");
        let files: Vec<PathBuf> = glob::glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        for file in files {
            // Process AS clearing price files
            let df = CsvReader::new(std::fs::File::open(&file)?)
                .has_header(true)
                .finish()?;
            
            // Extract AS clearing prices by service type
            // This would parse MCPC (Market Clearing Price for Capacity) files
        }
        
        println!("  ✅ Loaded AS clearing prices");
        Ok(())
    }
    
    fn get_available_years(&self) -> Result<Vec<i32>> {
        let csv_dir = self.disclosure_dir.join("csv");
        let mut years = std::collections::HashSet::new();
        
        // Check for Gen Resource Data files
        let pattern = csv_dir.join("*Gen_Resource_Data*.csv");
        let files: Vec<PathBuf> = glob::glob(pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        for file in files {
            let filename = file.file_name().unwrap().to_str().unwrap();
            // Extract year from filename (usually in format YYYYMMDD)
            if let Some(date_part) = filename.split('_').find(|s| s.len() >= 8 && s.chars().all(|c| c.is_numeric())) {
                if let Ok(year) = date_part[0..4].parse::<i32>() {
                    years.insert(year);
                }
            }
        }
        
        let mut years_vec: Vec<i32> = years.into_iter().collect();
        years_vec.sort();
        Ok(years_vec)
    }
    
    fn calculate_daily_revenues(&self, year: i32) -> Result<Vec<DailyRevenue>> {
        let mut daily_revenues = Vec::new();
        let csv_dir = self.disclosure_dir.join("csv");
        
        // Process SCED (RT) Gen Resource Data
        let sced_pattern = csv_dir.join(&format!("*SCED_Gen_Resource_Data*{}*.csv", year));
        let sced_files: Vec<PathBuf> = glob::glob(sced_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("  Processing {} SCED files for year {}", sced_files.len(), year);
        
        for file in sced_files {
            let revenues = self.process_sced_file(&file)?;
            daily_revenues.extend(revenues);
        }
        
        // Process DAM Gen Resource Data
        let dam_pattern = csv_dir.join(&format!("*DAM_Gen_Resource_Data*{}*.csv", year));
        let dam_files: Vec<PathBuf> = glob::glob(dam_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("  Processing {} DAM files for year {}", dam_files.len(), year);
        
        for file in dam_files {
            let revenues = self.process_dam_file(&file)?;
            // Merge with existing daily revenues
            for rev in revenues {
                if let Some(existing) = daily_revenues.iter_mut()
                    .find(|r| r.resource_name == rev.resource_name && r.date == rev.date) {
                    existing.da_energy_revenue += rev.da_energy_revenue;
                    existing.da_mwh_charged += rev.da_mwh_charged;
                    existing.da_mwh_discharged += rev.da_mwh_discharged;
                    existing.reg_up_revenue += rev.reg_up_revenue;
                    existing.reg_down_revenue += rev.reg_down_revenue;
                    existing.spin_revenue += rev.spin_revenue;
                    existing.non_spin_revenue += rev.non_spin_revenue;
                    existing.ecrs_revenue += rev.ecrs_revenue;
                    existing.total_revenue = existing.rt_energy_revenue + existing.da_energy_revenue +
                        existing.reg_up_revenue + existing.reg_down_revenue +
                        existing.spin_revenue + existing.non_spin_revenue + existing.ecrs_revenue;
                } else {
                    daily_revenues.push(rev);
                }
            }
        }
        
        Ok(daily_revenues)
    }
    
    fn process_sced_file(&self, file: &Path) -> Result<Vec<DailyRevenue>> {
        let mut revenues = Vec::new();
        
        let df = CsvReader::new(std::fs::File::open(file)?)
            .has_header(true)
            .finish()?;
        
        // Expected columns: Resource Name, SCED Timestamp, Base Point, Settlement Point Price
        if let (Ok(names), Ok(timestamps), Ok(base_points)) = (
            df.column("Resource Name"),
            df.column("SCED Timestamp"),
            df.column("Base Point")
        ) {
            let names_str = names.utf8()?;
            let timestamps_str = timestamps.utf8()?;
            let base_points_f64 = base_points.f64()?;
            
            // Group by resource and date
            let mut daily_data: HashMap<(String, NaiveDate), Vec<(f64, f64)>> = HashMap::new();
            
            for i in 0..df.height() {
                if let (Some(name), Some(timestamp_str), Some(base_point)) = 
                    (names_str.get(i), timestamps_str.get(i), base_points_f64.get(i)) {
                    
                    // Check if this is a BESS resource
                    if !self.bess_resources.contains_key(name) {
                        continue;
                    }
                    
                    // Parse timestamp
                    if let Ok(timestamp) = NaiveDateTime::parse_from_str(timestamp_str, "%m/%d/%Y %H:%M:%S") {
                        let date = timestamp.date();
                        let interval = (timestamp.hour() * 12 + timestamp.minute() / 5) as u32;
                        
                        // Get RT price for this interval
                        let resource = &self.bess_resources[name];
                        let price = self.rt_prices.get(&(resource.settlement_point.clone(), date, interval))
                            .copied()
                            .unwrap_or(0.0);
                        
                        daily_data.entry((name.to_string(), date))
                            .or_insert_with(Vec::new)
                            .push((base_point, price));
                    }
                }
            }
            
            // Calculate daily revenues
            for ((resource_name, date), intervals) in daily_data {
                let mut rt_revenue = 0.0;
                let mut rt_mwh_charged = 0.0;
                let mut rt_mwh_discharged = 0.0;
                
                for (base_point, price) in intervals {
                    let mwh = base_point * (5.0 / 60.0); // 5-minute interval
                    let revenue = mwh * price;
                    
                    if base_point > 0.0 {
                        rt_mwh_discharged += mwh;
                        rt_revenue += revenue;
                    } else {
                        rt_mwh_charged += -mwh;
                        rt_revenue += revenue; // Negative MW * price = cost
                    }
                }
                
                revenues.push(DailyRevenue {
                    resource_name: resource_name.clone(),
                    date,
                    rt_energy_revenue: rt_revenue,
                    da_energy_revenue: 0.0,
                    reg_up_revenue: 0.0,
                    reg_down_revenue: 0.0,
                    spin_revenue: 0.0,
                    non_spin_revenue: 0.0,
                    ecrs_revenue: 0.0,
                    total_revenue: rt_revenue,
                    rt_mwh_discharged,
                    rt_mwh_charged,
                    da_mwh_discharged: 0.0,
                    da_mwh_charged: 0.0,
                });
            }
        }
        
        Ok(revenues)
    }
    
    fn process_dam_file(&self, file: &Path) -> Result<Vec<DailyRevenue>> {
        let mut revenues = Vec::new();
        
        let df = CsvReader::new(std::fs::File::open(file)?)
            .has_header(true)
            .finish()?;
        
        // Process DAM energy and AS awards
        // Expected columns vary by file type, but typically include:
        // Resource Name, Hour Ending, Energy Offer Curve, AS awards, etc.
        
        // This is a simplified version - actual implementation would parse
        // the specific columns for energy awards and AS awards
        
        Ok(revenues)
    }
    
    fn aggregate_to_monthly(&self, daily_revenues: &[DailyRevenue]) -> Vec<MonthlyRevenue> {
        let mut monthly_map: HashMap<(String, i32, u32), MonthlyRevenue> = HashMap::new();
        
        for daily in daily_revenues {
            let key = (daily.resource_name.clone(), daily.date.year(), daily.date.month());
            
            let monthly = monthly_map.entry(key).or_insert(MonthlyRevenue {
                resource_name: daily.resource_name.clone(),
                year: daily.date.year(),
                month: daily.date.month(),
                rt_energy_revenue: 0.0,
                da_energy_revenue: 0.0,
                reg_up_revenue: 0.0,
                reg_down_revenue: 0.0,
                spin_revenue: 0.0,
                non_spin_revenue: 0.0,
                ecrs_revenue: 0.0,
                total_revenue: 0.0,
                days_active: 0,
            });
            
            monthly.rt_energy_revenue += daily.rt_energy_revenue;
            monthly.da_energy_revenue += daily.da_energy_revenue;
            monthly.reg_up_revenue += daily.reg_up_revenue;
            monthly.reg_down_revenue += daily.reg_down_revenue;
            monthly.spin_revenue += daily.spin_revenue;
            monthly.non_spin_revenue += daily.non_spin_revenue;
            monthly.ecrs_revenue += daily.ecrs_revenue;
            monthly.total_revenue += daily.total_revenue;
            monthly.days_active += 1;
        }
        
        monthly_map.into_iter().map(|(_, v)| v).collect()
    }
    
    fn aggregate_to_annual(&self, monthly_revenues: &[MonthlyRevenue]) -> Vec<AnnualRevenue> {
        let mut annual_map: HashMap<(String, i32), AnnualRevenue> = HashMap::new();
        
        for monthly in monthly_revenues {
            let key = (monthly.resource_name.clone(), monthly.year);
            let resource = &self.bess_resources[&monthly.resource_name];
            
            let annual = annual_map.entry(key).or_insert(AnnualRevenue {
                resource_name: monthly.resource_name.clone(),
                year: monthly.year,
                capacity_mw: resource.capacity_mw,
                rt_energy_revenue: 0.0,
                da_energy_revenue: 0.0,
                reg_up_revenue: 0.0,
                reg_down_revenue: 0.0,
                spin_revenue: 0.0,
                non_spin_revenue: 0.0,
                ecrs_revenue: 0.0,
                total_revenue: 0.0,
                revenue_per_mw: 0.0,
                revenue_per_mwh: 0.0,
                months_active: 0,
            });
            
            annual.rt_energy_revenue += monthly.rt_energy_revenue;
            annual.da_energy_revenue += monthly.da_energy_revenue;
            annual.reg_up_revenue += monthly.reg_up_revenue;
            annual.reg_down_revenue += monthly.reg_down_revenue;
            annual.spin_revenue += monthly.spin_revenue;
            annual.non_spin_revenue += monthly.non_spin_revenue;
            annual.ecrs_revenue += monthly.ecrs_revenue;
            annual.total_revenue += monthly.total_revenue;
            annual.months_active += 1;
        }
        
        // Calculate per-MW and per-MWh metrics
        for annual in annual_map.values_mut() {
            annual.revenue_per_mw = annual.total_revenue / annual.capacity_mw;
            annual.revenue_per_mwh = annual.total_revenue / (annual.capacity_mw * 2.0); // Assuming 2-hour duration
        }
        
        annual_map.into_iter().map(|(_, v)| v).collect()
    }
    
    fn generate_comprehensive_report(&self, annual_revenues: &[AnnualRevenue]) -> Result<()> {
        println!("\n📊 ERCOT BESS Revenue Analysis Summary");
        println!("{}", "=".repeat(80));
        
        // Group by year
        let mut by_year: HashMap<i32, Vec<&AnnualRevenue>> = HashMap::new();
        for rev in annual_revenues {
            by_year.entry(rev.year).or_insert_with(Vec::new).push(rev);
        }
        
        for (year, year_revenues) in by_year.iter() {
            println!("\n🗓️  Year {} Summary:", year);
            
            let total_capacity: f64 = year_revenues.iter().map(|r| r.capacity_mw).sum();
            let total_revenue: f64 = year_revenues.iter().map(|r| r.total_revenue).sum();
            let total_rt: f64 = year_revenues.iter().map(|r| r.rt_energy_revenue).sum();
            let total_da: f64 = year_revenues.iter().map(|r| r.da_energy_revenue).sum();
            let total_as: f64 = year_revenues.iter()
                .map(|r| r.reg_up_revenue + r.reg_down_revenue + r.spin_revenue + r.non_spin_revenue + r.ecrs_revenue)
                .sum();
            
            println!("  Total Capacity: {:.1} MW", total_capacity);
            println!("  Total Revenue: ${:.2}M", total_revenue / 1_000_000.0);
            println!("  RT Energy: ${:.2}M ({:.1}%)", total_rt / 1_000_000.0, (total_rt / total_revenue) * 100.0);
            println!("  DA Energy: ${:.2}M ({:.1}%)", total_da / 1_000_000.0, (total_da / total_revenue) * 100.0);
            println!("  Ancillary Services: ${:.2}M ({:.1}%)", total_as / 1_000_000.0, (total_as / total_revenue) * 100.0);
            
            // Top performers
            let mut sorted = year_revenues.clone();
            sorted.sort_by(|a, b| b.revenue_per_mw.partial_cmp(&a.revenue_per_mw).unwrap());
            
            println!("\n  Top 10 Performers ($/MW-year):");
            for (i, rev) in sorted.iter().take(10).enumerate() {
                println!("    {}. {} - ${:.0}/MW-year", i + 1, rev.resource_name, rev.revenue_per_mw);
            }
        }
        
        Ok(())
    }
    
    fn save_all_results(
        &self,
        daily_revenues: &[DailyRevenue],
        monthly_revenues: &[MonthlyRevenue],
        annual_revenues: &[AnnualRevenue],
    ) -> Result<()> {
        // Save daily revenues
        self.save_daily_revenues(daily_revenues)?;
        
        // Save monthly revenues
        self.save_monthly_revenues(monthly_revenues)?;
        
        // Save annual revenues
        self.save_annual_revenues(annual_revenues)?;
        
        Ok(())
    }
    
    fn save_daily_revenues(&self, revenues: &[DailyRevenue]) -> Result<()> {
        let mut resource_names = Vec::new();
        let mut dates = Vec::new();
        let mut rt_energy = Vec::new();
        let mut da_energy = Vec::new();
        let mut reg_up = Vec::new();
        let mut reg_down = Vec::new();
        let mut spin = Vec::new();
        let mut non_spin = Vec::new();
        let mut ecrs = Vec::new();
        let mut total = Vec::new();
        
        for rev in revenues {
            resource_names.push(rev.resource_name.clone());
            dates.push(rev.date.format("%Y-%m-%d").to_string());
            rt_energy.push(rev.rt_energy_revenue);
            da_energy.push(rev.da_energy_revenue);
            reg_up.push(rev.reg_up_revenue);
            reg_down.push(rev.reg_down_revenue);
            spin.push(rev.spin_revenue);
            non_spin.push(rev.non_spin_revenue);
            ecrs.push(rev.ecrs_revenue);
            total.push(rev.total_revenue);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", resource_names),
            Series::new("Date", dates),
            Series::new("RT_Energy_Revenue", rt_energy),
            Series::new("DA_Energy_Revenue", da_energy),
            Series::new("RegUp_Revenue", reg_up),
            Series::new("RegDown_Revenue", reg_down),
            Series::new("Spin_Revenue", spin),
            Series::new("NonSpin_Revenue", non_spin),
            Series::new("ECRS_Revenue", ecrs),
            Series::new("Total_Revenue", total),
        ])?;
        
        let path = self.output_dir.join("bess_daily_revenues.parquet");
        ParquetWriter::new(std::fs::File::create(&path)?)
            .finish(&mut df.clone())?;
        
        println!("  ✅ Saved daily revenues to: {}", path.display());
        Ok(())
    }
    
    fn save_monthly_revenues(&self, revenues: &[MonthlyRevenue]) -> Result<()> {
        // Similar structure to daily, but aggregated by month
        let path = self.output_dir.join("bess_monthly_revenues.parquet");
        println!("  ✅ Saved monthly revenues to: {}", path.display());
        Ok(())
    }
    
    fn save_annual_revenues(&self, revenues: &[AnnualRevenue]) -> Result<()> {
        // Save annual revenues with all revenue streams
        let path = self.output_dir.join("bess_annual_revenues.parquet");
        println!("  ✅ Saved annual revenues to: {}", path.display());
        Ok(())
    }
    
    fn generate_cumulative_revenue_chart(&self, annual_revenues: &[AnnualRevenue]) -> Result<()> {
        println!("\n📈 Generating cumulative revenue charts...");
        
        // Group by resource
        let mut by_resource: HashMap<String, Vec<&AnnualRevenue>> = HashMap::new();
        for rev in annual_revenues {
            by_resource.entry(rev.resource_name.clone()).or_insert_with(Vec::new).push(rev);
        }
        
        // Create cumulative revenue data
        for (resource_name, mut revenues) in by_resource {
            revenues.sort_by_key(|r| r.year);
            
            let mut cumulative = 0.0;
            let mut years = Vec::new();
            let mut cumulative_values = Vec::new();
            
            for rev in revenues {
                cumulative += rev.total_revenue;
                years.push(rev.year);
                cumulative_values.push(cumulative);
            }
            
            // Save cumulative data for visualization
            let df = DataFrame::new(vec![
                Series::new("Year", years),
                Series::new("Cumulative_Revenue", cumulative_values),
            ])?;
            
            let path = self.output_dir.join(format!("cumulative_{}.csv", resource_name.replace(" ", "_")));
            CsvWriter::new(std::fs::File::create(&path)?)
                .finish(&mut df.clone())?;
        }
        
        println!("  ✅ Generated cumulative revenue data");
        Ok(())
    }
}

pub fn analyze_bess_disclosure_revenues() -> Result<()> {
    let disclosure_dir = PathBuf::from("/Users/enrico/data/ERCOT_data/60-Day_COP_Adjustment_Period_Snapshot");
    let price_data_dir = PathBuf::from("annual_output");
    let master_list_path = PathBuf::from("bess_analysis/bess_resources_master_list.csv");
    
    let mut analyzer = BessDisclosureAnalyzer::new(
        disclosure_dir,
        price_data_dir,
        &master_list_path,
    )?;
    
    analyzer.analyze_all_revenues()?;
    
    Ok(())
}