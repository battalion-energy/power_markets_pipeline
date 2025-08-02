use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, Timelike, DateTime};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BessRevenue {
    pub resource_name: String,
    pub date: NaiveDate,
    pub energy_revenue: f64,
    pub dam_energy_revenue: f64,  // New: DAM energy revenue
    pub rt_energy_revenue: f64,   // New: RT energy revenue
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
#[allow(dead_code)]
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
    settlement_point_map: HashMap<String, String>, // resource_name -> RT settlement point
    rt_prices: HashMap<(String, NaiveDate, i64), f64>, // Cached RT prices
    dam_prices: HashMap<(String, NaiveDate, i32), f64>, // Cached DAM prices
    ancillary_prices: HashMap<(String, NaiveDate, i32), HashMap<String, f64>>, // Cached AS prices
}

impl BessRevenueCalculator {
    fn load_settlement_point_mapping(output_dir: &Path) -> HashMap<String, String> {
        let mut map = HashMap::new();
        
        // Try to load the updated mapping file first
        let updated_path = output_dir.join("settlement_point_mapping_updated.csv");
        let path = if updated_path.exists() {
            updated_path
        } else {
            output_dir.join("settlement_point_mapping.csv")
        };
        
        if let Ok(file) = std::fs::File::open(&path) {
            if let Ok(df) = CsvReader::new(file)
                .has_header(true)
                .finish() {
                
                if let (Ok(resources), Ok(settlement_points)) = (
                    df.column("Resource_Name"),
                    df.column("Settlement_Point")
                ) {
                    let resources_utf8 = resources.utf8().unwrap();
                    let sps_utf8 = settlement_points.utf8().unwrap();
                    
                    for i in 0..df.height() {
                        if let (Some(resource), Some(sp)) = 
                            (resources_utf8.get(i), sps_utf8.get(i)) {
                            map.insert(resource.to_string(), sp.to_string());
                        }
                    }
                    
                    println!("    Loaded {} settlement point mappings from {}", 
                             map.len(), path.file_name().unwrap().to_str().unwrap());
                }
            }
        }
        
        map
    }
    
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
        
        // Load updated settlement point mapping if available
        let settlement_point_map = Self::load_settlement_point_mapping(&output_dir);
        
        // Load all price data at initialization
        let mut calculator = Self {
            data_dir,
            output_dir,
            bess_resources,
            settlement_point_map,
            rt_prices: HashMap::new(),
            dam_prices: HashMap::new(),
            ancillary_prices: HashMap::new(),
        };
        
        // Load all available price data
        calculator.load_all_price_data()?;
        
        Ok(calculator)
    }
    
    fn load_all_price_data(&mut self) -> Result<()> {
        println!("📊 Loading all available price data...");
        
        // Load RT prices
        self.load_all_rt_prices()?;
        
        // Load DAM prices
        self.load_all_dam_prices()?;
        
        // Load Ancillary Service prices
        self.load_all_ancillary_prices()?;
        
        println!("✅ Price data loading complete");
        Ok(())
    }
    
    fn load_all_rt_prices(&mut self) -> Result<()> {
        // Check both unified_processed_data and annual_data directories
        let patterns = vec![
            "unified_processed_data/RT_Settlement_Point_Prices_*/RT_Settlement_Point_Prices_*.csv",
            "unified_processed_data/RT_LMPs_*/RT_LMPs_*.csv",
            "annual_data/RT_Settlement_Point_Prices_*.csv",
            "annual_data/RT_LMPs_*.csv",
        ];
        
        for pattern in patterns {
            let files: Vec<PathBuf> = glob::glob(pattern)?
                .filter_map(Result::ok)
                .collect();
            
            for file in files {
                println!("    Loading RT prices from: {}", file.display());
                let prices = self.load_rt_prices(&file)?;
                self.rt_prices.extend(prices);
            }
        }
        
        println!("    Loaded {} total RT price points", self.rt_prices.len());
        Ok(())
    }
    
    fn load_all_dam_prices(&mut self) -> Result<()> {
        // Load DAM prices from processed files
        let patterns = vec![
            "unified_processed_data/DAM_Settlement_Point_Prices_*/DAM_Settlement_Point_Prices_*.csv",
            "unified_processed_data/DAM_Hourly_LMPs_*/DAM_Hourly_LMPs_*.csv",
            "dam_annual_data/DAM_Settlement_Point_Prices_*.csv",
            "dam_annual_data/DAM_Hourly_LMPs_*.csv",
        ];
        
        for pattern in patterns {
            let files: Vec<PathBuf> = glob::glob(pattern)?
                .filter_map(Result::ok)
                .collect();
            
            for file in files {
                println!("    Loading DAM prices from: {}", file.display());
                let prices = self.load_dam_prices(&file)?;
                self.dam_prices.extend(prices);
            }
        }
        
        println!("    Loaded {} total DAM price points", self.dam_prices.len());
        Ok(())
    }
    
    fn load_all_ancillary_prices(&mut self) -> Result<()> {
        // Load ancillary service clearing prices
        let patterns = vec![
            "unified_processed_data/DAM_Clearing_Prices_Capacity_*/DAM_Clearing_Prices_Capacity_*.csv",
            "ancillary_annual_data/DAM_Clearing_Prices_Capacity_*.csv",
        ];
        
        for pattern in patterns {
            let files: Vec<PathBuf> = glob::glob(pattern)?
                .filter_map(Result::ok)
                .collect();
            
            for file in files {
                println!("    Loading AS prices from: {}", file.display());
                let prices = self.load_ancillary_service_prices(&file)?;
                
                // Merge AS prices into the map
                for ((date, hour), service_prices) in prices {
                    self.ancillary_prices.entry(("ERCOT".to_string(), date, hour))
                        .or_insert_with(HashMap::new)
                        .extend(service_prices);
                }
            }
        }
        
        let total_as_entries: usize = self.ancillary_prices.values()
            .map(|m| m.len())
            .sum();
        println!("    Loaded {} total AS price entries", total_as_entries);
        Ok(())
    }

    pub fn calculate_all_revenues(&self) -> Result<()> {
        println!("💰 BESS Revenue Calculation");
        println!("{}", "=".repeat(80));
        
        // Process energy revenues (now returns separate DAM and RT)
        let (dam_revenues, rt_revenues) = self.calculate_energy_revenues_split()?;
        
        // Process ancillary service revenues
        let as_revenues = self.calculate_ancillary_revenues()?;
        
        // Combine and create daily rollups
        let daily_revenues = self.create_daily_rollups_split(dam_revenues, rt_revenues, as_revenues)?;
        
        // Detect SOC violations and AS failures
        self.detect_operational_issues(&daily_revenues)?;
        
        // Generate performance metrics
        self.generate_performance_metrics(&daily_revenues)?;
        
        // Generate detailed revenue breakdown
        self.generate_detailed_revenue_breakdown(&daily_revenues)?;
        
        Ok(())
    }

    fn calculate_energy_revenues_split(&self) -> Result<(HashMap<(String, NaiveDate), f64>, HashMap<(String, NaiveDate), f64>)> {
        println!("\n📊 Calculating Energy Arbitrage Revenues...");
        
        let mut energy_revenues = HashMap::new();
        
        // First, calculate DAM costs (charging)
        println!("  📥 Calculating DAM energy costs (charging)...");
        let dam_costs = self.calculate_dam_energy_costs()?;
        
        // Then, calculate RT revenues (discharging)
        println!("  📤 Calculating RT energy revenues (discharging)...");
        let rt_revenues = self.calculate_rt_energy_revenues()?;
        
        // Combine DAM costs and RT revenues
        for (key, dam_cost) in &dam_costs {
            *energy_revenues.entry(key.clone()).or_insert(0.0) += dam_cost;
        }
        
        for (key, rt_revenue) in &rt_revenues {
            *energy_revenues.entry(key.clone()).or_insert(0.0) += rt_revenue;
        }
        
        // Calculate total
        let total_dam: f64 = dam_costs.values().sum();
        let total_rt: f64 = rt_revenues.values().sum();
        let total_energy: f64 = energy_revenues.values().sum();
        
        println!("\n  Energy Revenue Summary:");
        println!("    DAM energy: ${:.2}", total_dam);
        println!("    RT energy: ${:.2}", total_rt);
        println!("    Net energy arbitrage: ${:.2}", total_energy);
        println!("\n  Calculated energy revenues for {} resource-days", energy_revenues.len());
        
        Ok((dam_costs, rt_revenues))
    }
    
    fn calculate_dam_energy_costs(&self) -> Result<HashMap<(String, NaiveDate), f64>> {
        let mut dam_costs = HashMap::new();
        let mut dam_revenues = HashMap::new();
        let mut dam_net = HashMap::new();
        
        // Use DAM Gen Resource Data instead of Energy Bid Awards
        let dam_pattern = self.data_dir.join("DAM_extracted/60d_DAM_Gen_Resource_Data*.csv");
        let dam_files: Vec<PathBuf> = glob::glob(dam_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("    Processing {} DAM Gen Resource Data files (separating charging costs and discharging revenues)", dam_files.len());
        
        let pb = indicatif::ProgressBar::new(dam_files.len() as u64);
        pb.set_style(indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        for file_path in dam_files {
            pb.inc(1);
            
            if let Ok(df) = CsvReader::new(std::fs::File::open(&file_path)?)
                .has_header(true)
                .finish() {
                
                // Filter for BESS resources (PWRSTR type)
                if let Ok(resource_types) = df.column("Resource Type") {
                    let mask = resource_types.utf8()?.equal("PWRSTR");
                    
                    if let Ok(filtered) = df.filter(&mask) {
                        // Process PWRSTR resources
                        if let (Ok(dates), Ok(hours), Ok(resources), Ok(awards), Ok(prices)) = (
                            filtered.column("Delivery Date"),
                            filtered.column("Hour Ending"),
                            filtered.column("Resource Name"),
                            filtered.column("Awarded Quantity"),
                            filtered.column("Energy Settlement Point Price")
                        ) {
                            let dates_utf8 = dates.utf8()?;
                            let hours_i64 = hours.i64()?;
                            let resources_utf8 = resources.utf8()?;
                            
                            // Handle awarded quantity - might be string or float
                            let awards_f64 = if let Ok(f64_col) = awards.f64() {
                                f64_col.clone()
                            } else if let Ok(utf8_col) = awards.utf8() {
                                // Convert string to float
                                let values: Vec<Option<f64>> = utf8_col.into_iter()
                                    .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                                    .collect();
                                Float64Chunked::from_iter(values)
                            } else {
                                continue;
                            };
                            
                            let prices_f64 = prices.f64()?;
                            
                            for i in 0..filtered.height() {
                                if let (Some(date_str), Some(_hour), Some(resource), Some(award_mw), Some(price)) = 
                                    (dates_utf8.get(i), hours_i64.get(i), resources_utf8.get(i), 
                                     awards_f64.get(i), prices_f64.get(i)) {
                                    
                                    // Check if this is one of our BESS resources
                                    if self.bess_resources.contains_key(resource) {
                                        // Parse date
                                        if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                                            let key = (resource.to_string(), date);
                                            
                                            // Separate charging costs from discharging revenues
                                            if award_mw < 0.0 {
                                                // Charging (negative MW) = cost
                                                let cost = award_mw * price; // Negative MW * $/MWh = negative $
                                                *dam_costs.entry(key.clone()).or_insert(0.0) += cost;
                                            } else if award_mw > 0.0 {
                                                // Discharging (positive MW) = revenue
                                                let revenue = award_mw * price; // Positive MW * $/MWh = positive $
                                                *dam_revenues.entry(key.clone()).or_insert(0.0) += revenue;
                                            }
                                            
                                            // Net revenue
                                            let net = award_mw * price;
                                            *dam_net.entry(key).or_insert(0.0) += net;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        pb.finish();
        
        // Report DAM breakdown
        let total_charging: f64 = dam_costs.values().sum();
        let total_discharging: f64 = dam_revenues.values().sum();
        let total_net: f64 = dam_net.values().sum();
        
        println!("      DAM Energy Breakdown:");
        println!("        Charging costs: ${:.2}", total_charging);
        println!("        Discharging revenues: ${:.2}", total_discharging);
        println!("        Net DAM energy: ${:.2}", total_net);
        
        Ok(dam_net)
    }
    
    fn calculate_rt_energy_revenues(&self) -> Result<HashMap<(String, NaiveDate), f64>> {
        let mut rt_revenues = HashMap::new();
        
        // Load RT SCED Gen Resource Data
        let sced_pattern = self.data_dir.join("SCED_extracted/60d_SCED_Gen_Resource_Data*.csv");
        let sced_files: Vec<PathBuf> = glob::glob(sced_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
        
        println!("    Processing {} SCED Gen Resource Data files (both charging and discharging)", sced_files.len());
        
        // Use cached RT prices
        if self.rt_prices.is_empty() {
            println!("    ⚠️  No RT prices loaded!");
        } else {
            println!("    Using {} cached RT price points", self.rt_prices.len());
        }
        
        let pb = indicatif::ProgressBar::new(sced_files.len() as u64);
        pb.set_style(indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap());
        
        for file_path in sced_files {
            pb.inc(1);
            
            if let Ok(df) = CsvReader::new(std::fs::File::open(&file_path)?)
                .has_header(true)
                .finish() {
                
                // Filter for BESS resources (PWRSTR type)
                if let Ok(resource_types) = df.column("Resource Type") {
                    let mask = resource_types.utf8()?.equal("PWRSTR");
                    
                    if let Ok(filtered) = df.filter(&mask) {
                        self.process_rt_output(&filtered, &self.rt_prices, &mut rt_revenues)?;
                    }
                }
            }
        }
        
        pb.finish();
        
        // Also process SMNE (Settlement Metered Net Energy) files
        println!("    Processing SCED SMNE files for additional RT data...");
        let smne_pattern = self.data_dir.join("SCED_extracted/60d_SCED_SMNE_GEN_RES*.csv");
        let smne_files: Vec<PathBuf> = glob::glob(smne_pattern.to_str().unwrap())?
            .filter_map(Result::ok)
            .collect();
            
        if !smne_files.is_empty() {
            println!("    Found {} SMNE files to process", smne_files.len());
            let pb2 = indicatif::ProgressBar::new(smne_files.len() as u64);
            pb2.set_style(indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}")
                .unwrap());
                
            for file_path in smne_files {
                pb2.inc(1);
                self.process_smne_file(&file_path, &self.rt_prices, &mut rt_revenues)?;
            }
            pb2.finish();
        }
        
        Ok(rt_revenues)
    }
    
    fn load_rt_prices(&self, file_path: &Path) -> Result<HashMap<(String, NaiveDate, i64), f64>> {
        let mut prices = HashMap::new();
        
        if let Ok(df) = CsvReader::new(std::fs::File::open(file_path)?)
            .has_header(true)
            .finish() {
            
            if let (Ok(datetimes), Ok(sps), Ok(prices_col)) = (
                df.column("datetime"),
                df.column("SettlementPoint"),
                df.column("SettlementPointPrice")
            ) {
                let datetimes_i64 = datetimes.i64()?;
                let sps_utf8 = sps.utf8()?;
                let prices_f64 = prices_col.f64()?;
                
                println!("    Loading {} RT price records", df.height());
                
                for i in 0..df.height() {
                    if let (Some(timestamp_ms), Some(sp), Some(price)) = 
                        (datetimes_i64.get(i), sps_utf8.get(i), prices_f64.get(i)) {
                        
                        // Convert milliseconds to datetime
                        let datetime = DateTime::from_timestamp_millis(timestamp_ms)
                            .map(|dt| dt.naive_utc());
                        if let Some(dt) = datetime {
                            let date = dt.date();
                            let interval = (dt.hour() * 60 + dt.minute()) / 15; // 15-min interval
                            
                            let key = (sp.to_string(), date, interval as i64);
                            prices.insert(key, price);
                        }
                    }
                }
                
                println!("    Loaded {} unique RT price points", prices.len());
            }
        }
        
        Ok(prices)
    }
    
    fn load_dam_prices(&self, file_path: &Path) -> Result<HashMap<(String, NaiveDate, i32), f64>> {
        let mut prices = HashMap::new();
        
        if let Ok(df) = CsvReader::new(std::fs::File::open(file_path)?)
            .has_header(true)
            .finish() {
            
            // Try different column names for DAM data
            let datetime_col = if df.get_column_names().contains(&"datetime") {
                "datetime"
            } else if df.get_column_names().contains(&"DeliveryDate") {
                "DeliveryDate" 
            } else {
                return Ok(prices);
            };
            
            let sp_col = if df.get_column_names().contains(&"SettlementPoint") {
                "SettlementPoint"
            } else if df.get_column_names().contains(&"BusName") {
                "BusName"
            } else {
                return Ok(prices);
            };
            
            let price_col = if df.get_column_names().contains(&"SettlementPointPrice") {
                "SettlementPointPrice"
            } else if df.get_column_names().contains(&"LMP") {
                "LMP"
            } else {
                return Ok(prices);
            };
            
            if datetime_col == "datetime" {
                // Datetime column exists
                if let (Ok(datetimes), Ok(sps), Ok(prices_col)) = (
                    df.column(datetime_col),
                    df.column(sp_col),
                    df.column(price_col)
                ) {
                    let datetimes_i64 = datetimes.i64()?;
                    let sps_utf8 = sps.utf8()?;
                    let prices_f64 = prices_col.f64()?;
                    
                    for i in 0..df.height() {
                        if let (Some(timestamp_ms), Some(sp), Some(price)) = 
                            (datetimes_i64.get(i), sps_utf8.get(i), prices_f64.get(i)) {
                            
                            let datetime = DateTime::from_timestamp_millis(timestamp_ms)
                            .map(|dt| dt.naive_utc());
                            if let Some(dt) = datetime {
                                let date = dt.date();
                                let hour = dt.hour() as i32 + 1; // DAM uses hour ending (1-24)
                                
                                let key = (sp.to_string(), date, hour);
                                prices.insert(key, price);
                            }
                        }
                    }
                }
            } else {
                // Use DeliveryDate and HourEnding columns
                if let (Ok(dates), Ok(hours), Ok(sps), Ok(prices_col)) = (
                    df.column("DeliveryDate"),
                    df.column("HourEnding"),
                    df.column(sp_col),
                    df.column(price_col)
                ) {
                    let dates_utf8 = dates.utf8()?;
                    let hours_cast = hours.cast(&DataType::Int32)?;
                    let hours_i32 = hours_cast.i32()?;
                    let sps_utf8 = sps.utf8()?;
                    let prices_f64 = prices_col.f64()?;
                    
                    for i in 0..df.height() {
                        if let (Some(date_str), Some(hour), Some(sp), Some(price)) = 
                            (dates_utf8.get(i), hours_i32.get(i), sps_utf8.get(i), prices_f64.get(i)) {
                            
                            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                                let key = (sp.to_string(), date, hour);
                                prices.insert(key, price);
                            }
                        }
                    }
                }
            }
            
            println!("      Loaded {} DAM price points from {}", prices.len(), file_path.file_name().unwrap().to_str().unwrap());
        }
        
        Ok(prices)
    }
    
    fn load_ancillary_service_prices(&self, file_path: &Path) -> Result<HashMap<(NaiveDate, i32), HashMap<String, f64>>> {
        let mut prices = HashMap::new();
        
        if let Ok(df) = CsvReader::new(std::fs::File::open(file_path)?)
            .has_header(true)
            .finish() {
            
            // Check if we have datetime column
            let has_datetime = df.get_column_names().contains(&"datetime");
            
            if has_datetime {
                if let Ok(datetimes) = df.column("datetime") {
                    let datetimes_i64 = datetimes.i64()?;
                    
                    // Get all AS service columns
                    let service_columns = vec!["REGUP", "REGDN", "RRSPFR", "RRSUFR", "RRSFFR", "NSPIN", "ECRS", "ECRSM", "ECRSS"];
                    
                    for i in 0..df.height() {
                        if let Some(timestamp_ms) = datetimes_i64.get(i) {
                            let datetime = DateTime::from_timestamp_millis(timestamp_ms)
                            .map(|dt| dt.naive_utc());
                            if let Some(dt) = datetime {
                                let date = dt.date();
                                let hour = dt.hour() as i32 + 1; // Hour ending
                                
                                let mut service_prices = HashMap::new();
                                
                                // Extract price for each service
                                for service in &service_columns {
                                    if let Ok(price_col) = df.column(service) {
                                        if let Ok(prices_f64) = price_col.f64() {
                                            if let Some(price) = prices_f64.get(i) {
                                                service_prices.insert(service.to_string(), price);
                                            }
                                        }
                                    }
                                }
                                
                                if !service_prices.is_empty() {
                                    prices.insert((date, hour), service_prices);
                                }
                            }
                        }
                    }
                }
            } else {
                // Use DeliveryDate and HourEnding
                if let (Ok(dates), Ok(hours)) = (
                    df.column("DeliveryDate"),
                    df.column("HourEnding")
                ) {
                    let dates_utf8 = dates.utf8()?;
                    let hours_cast = hours.cast(&DataType::Int32)?;
                    let hours_i32 = hours_cast.i32()?;
                    
                    // Get all AS service columns
                    let service_columns = vec!["REGUP", "REGDN", "RRSPFR", "RRSUFR", "RRSFFR", "NSPIN", "ECRS", "ECRSM", "ECRSS"];
                    
                    for i in 0..df.height() {
                        if let (Some(date_str), Some(hour)) = (dates_utf8.get(i), hours_i32.get(i)) {
                            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                                let mut service_prices = HashMap::new();
                                
                                // Extract price for each service
                                for service in &service_columns {
                                    if let Ok(price_col) = df.column(service) {
                                        if let Ok(prices_f64) = price_col.f64() {
                                            if let Some(price) = prices_f64.get(i) {
                                                service_prices.insert(service.to_string(), price);
                                            }
                                        }
                                    }
                                }
                                
                                if !service_prices.is_empty() {
                                    prices.insert((date, hour), service_prices);
                                }
                            }
                        }
                    }
                }
            }
            
            println!("      Loaded {} AS price points from {}", prices.len(), file_path.file_name().unwrap().to_str().unwrap());
        }
        
        Ok(prices)
    }
    
    fn process_rt_output(&self, df: &DataFrame, rt_prices: &HashMap<(String, NaiveDate, i64), f64>,
                        rt_revenues: &mut HashMap<(String, NaiveDate), f64>) -> Result<()> {
        // Debug: print columns once
        static mut PRINTED_SCED: bool = false;
        unsafe {
            if !PRINTED_SCED {
                println!("    SCED columns: {:?}", df.get_column_names());
                PRINTED_SCED = true;
            }
        }
        
        // Extract relevant columns - try Output Schedule first, then Telemetered Net Output
        let output_col = if df.column("Output Schedule").is_ok() {
            "Output Schedule"
        } else {
            "Telemetered Net Output"
        };
        
        if let (Ok(timestamps), Ok(resources), Ok(outputs)) = (
            df.column("SCED Time Stamp"),
            df.column("Resource Name"),
            df.column(output_col)
        ) {
            let timestamps_utf8 = timestamps.utf8()?;
            let resources_utf8 = resources.utf8()?;
            
            // Handle output column - might be string or float
            let outputs_f64 = if let Ok(f64_col) = outputs.f64() {
                f64_col.clone()
            } else if let Ok(utf8_col) = outputs.utf8() {
                // Convert string to float
                let values: Vec<Option<f64>> = utf8_col.into_iter()
                    .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                    .collect();
                Float64Chunked::from_iter(values)
            } else {
                return Ok(());
            };
            
            for i in 0..df.height() {
                if let (Some(timestamp_str), Some(resource), Some(output_mw)) = 
                    (timestamps_utf8.get(i), resources_utf8.get(i), outputs_f64.get(i)) {
                    
                    // Parse timestamp
                    if let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%m/%d/%Y %H:%M:%S") {
                        let date = timestamp.date();
                        let interval = (timestamp.hour() * 60 + timestamp.minute()) / 15; // 15-min interval
                        
                        // Both charging (negative) and discharging (positive)
                        if output_mw != 0.0 {
                            // Get settlement point for this resource
                            if let Some((master_sp, _)) = self.bess_resources.get(resource) {
                                // Use mapped settlement point if available, otherwise use master list SP
                                let sp = self.settlement_point_map.get(resource)
                                    .unwrap_or(master_sp);
                                
                                // Look up RT price
                                let price_key = (sp.clone(), date, interval as i64);
                                let price = if let Some(p) = rt_prices.get(&price_key) {
                                    *p
                                } else {
                                    // Try Houston Hub as fallback
                                    let houston_key = ("HB_HOUSTON".to_string(), date, interval as i64);
                                    if let Some(p) = rt_prices.get(&houston_key) {
                                        static mut DEBUG_HOUSTON: u32 = 0;
                                        unsafe {
                                            if DEBUG_HOUSTON < 3 {
                                                println!("      Using Houston Hub price for {} @ {} interval {}", sp, date, interval);
                                                DEBUG_HOUSTON += 1;
                                            }
                                        }
                                        *p
                                    } else {
                                        // No price available - skip this interval
                                        static mut DEBUG_NO_PRICE: u32 = 0;
                                        unsafe {
                                            if DEBUG_NO_PRICE < 3 {
                                                println!("      No RT price found for {} @ {} interval {} - skipping", sp, date, interval);
                                                DEBUG_NO_PRICE += 1;
                                            }
                                        }
                                        continue; // Skip this interval entirely
                                    }
                                };
                                
                                let revenue = output_mw * price / 4.0; // MW * $/MWh / 4 = $ for 15-min interval
                                
                                // Debug first few RT revenues
                                static mut DEBUG_COUNT: u32 = 0;
                                unsafe {
                                    if DEBUG_COUNT < 5 {
                                        println!("      RT revenue: {} @ {} - {} MW × ${}/MWh = ${:.2}", 
                                                 resource, timestamp_str, output_mw, price, revenue);
                                        DEBUG_COUNT += 1;
                                    }
                                }
                                
                                let key = (resource.to_string(), date);
                                *rt_revenues.entry(key).or_insert(0.0) += revenue;
                            } else {
                                // Debug: resource not found in BESS list
                                static mut DEBUG_NOT_FOUND: u32 = 0;
                                unsafe {
                                    if DEBUG_NOT_FOUND < 3 {
                                        println!("      BESS resource not found: {}", resource);
                                        DEBUG_NOT_FOUND += 1;
                                    }
                                }
                            }
                        }  // <-- This closes the if output_mw != 0.0 block
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn process_smne_file(&self, file_path: &Path, rt_prices: &HashMap<(String, NaiveDate, i64), f64>,
                         rt_revenues: &mut HashMap<(String, NaiveDate), f64>) -> Result<()> {
        if let Ok(df) = CsvReader::new(std::fs::File::open(file_path)?)
            .has_header(true)
            .finish() {
            
            // SMNE file format: "Interval Time","Interval Number","Resource Code","Interval Value"
            if let (Ok(timestamps), Ok(resources), Ok(values)) = (
                df.column("Interval Time"),
                df.column("Resource Code"),
                df.column("Interval Value")
            ) {
                let timestamps_utf8 = timestamps.utf8()?;
                let resources_utf8 = resources.utf8()?;
                
                // Handle values - might be string or float
                let values_f64 = if let Ok(f64_col) = values.f64() {
                    f64_col.clone()
                } else if let Ok(utf8_col) = values.utf8() {
                    // Convert string to float
                    let values: Vec<Option<f64>> = utf8_col.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Float64Chunked::from_iter(values)
                } else {
                    return Ok(());
                };
                
                for i in 0..df.height() {
                    if let (Some(timestamp_str), Some(resource), Some(output_mw)) = 
                        (timestamps_utf8.get(i), resources_utf8.get(i), values_f64.get(i)) {
                        
                        // Check if this is a BESS resource
                        if !self.bess_resources.contains_key(resource) {
                            continue;
                        }
                        
                        // Parse timestamp
                        if let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%m/%d/%Y %H:%M:%S") {
                            let date = timestamp.date();
                            let interval = (timestamp.hour() * 60 + timestamp.minute()) / 15; // 15-min interval
                            
                            // Both charging (negative) and discharging (positive)
                            if output_mw != 0.0 {
                                // Get settlement point for this resource
                                if let Some((master_sp, _)) = self.bess_resources.get(resource) {
                                    // Use mapped settlement point if available, otherwise use master list SP
                                    let sp = self.settlement_point_map.get(resource)
                                        .unwrap_or(master_sp);
                                    
                                    // Look up RT price
                                    let price_key = (sp.clone(), date, interval as i64);
                                    let price = if let Some(p) = rt_prices.get(&price_key) {
                                        *p
                                    } else {
                                        // Try Houston Hub as fallback
                                        let houston_key = ("HB_HOUSTON".to_string(), date, interval as i64);
                                        if let Some(p) = rt_prices.get(&houston_key) {
                                            *p
                                        } else {
                                            continue; // Skip this interval entirely
                                        }
                                    };
                                    
                                    let revenue = output_mw * price / 4.0; // MW * $/MWh / 4 = $ for 15-min interval
                                    
                                    // Debug first few SMNE revenues
                                    static mut DEBUG_SMNE: u32 = 0;
                                    unsafe {
                                        if DEBUG_SMNE < 5 && output_mw.abs() > 0.01 {
                                            println!("      SMNE revenue: {} @ {} - {} MW × ${}/MWh = ${:.2}", 
                                                     resource, timestamp_str, output_mw, price, revenue);
                                            DEBUG_SMNE += 1;
                                        }
                                    }
                                    
                                    let key = (resource.to_string(), date);
                                    *rt_revenues.entry(key).or_insert(0.0) += revenue;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
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
        // Debug: Print column names once
        static mut PRINTED: bool = false;
        unsafe {
            if !PRINTED {
                println!("  Gen Resource Data columns: {:?}", df.get_column_names());
                PRINTED = true;
            }
        }
        
        // Extract relevant columns
        let dates = df.column("Delivery Date")?.utf8()?;
        let _hours = df.column("Hour Ending")?.i64()?;
        let resources = df.column("Resource Name")?.utf8()?;
        
        // Try to get energy price column (may not exist in older formats)
        let _prices = df.column("Energy Settlement Point Price").ok().and_then(|c| c.f64().ok());
        
        // AS awards and prices - handle both old and new formats
        // Try to convert string columns to float, handling empty strings
        let reg_up_awards = df.column("RegUp Awarded").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    // Convert empty strings to 0.0
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        let reg_up_prices = df.column("RegUp MCPC").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        let reg_down_awards = df.column("RegDown Awarded").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        let reg_down_prices = df.column("RegDown MCPC").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        // For RRS, try both "RRS Awarded" and combined RRS types
        let rrs_awards = df.column("RRS Awarded").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        let rrs_prices = df.column("RRS MCPC").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        let non_spin_awards = df.column("NonSpin Awarded").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        let non_spin_prices = df.column("NonSpin MCPC").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
        
        // Try ECRS columns (newer format)
        let ecrs_awards = df.column("ECRSSD Awarded").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
            
        let ecrs_prices = df.column("ECRS MCPC").ok()
            .and_then(|c| {
                if let Ok(utf8) = c.utf8() {
                    let values: Vec<Option<f64>> = utf8.into_iter()
                        .map(|v| v.and_then(|s| if s.is_empty() { Some(0.0) } else { s.parse().ok() }))
                        .collect();
                    Some(Float64Chunked::from_iter(values))
                } else {
                    c.f64().ok().cloned()
                }
            });
        
        // Debug: Print if we found AS columns
        if reg_up_awards.is_some() && reg_up_prices.is_some() {
            println!("  Found RegUp columns in Gen Resource Data");
        }
        
        for i in 0..df.height() {
            if let (Some(date_str), Some(resource)) = (dates.get(i), resources.get(i)) {
                if self.bess_resources.contains_key(resource) {
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
                        let key = (resource.to_string(), date);
                        let revenues = as_revenues.entry(key).or_insert_with(HashMap::new);
                        
                        // Calculate revenues for each AS type
                        if let (Some(awards), Some(prices)) = (reg_up_awards.as_ref(), reg_up_prices.as_ref()) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("RegUp".to_string()).or_insert(0.0) += award * price;
                                    // Debug first AS revenue calculation
                                    static mut PRINTED_AS: bool = false;
                                    unsafe {
                                        if !PRINTED_AS && resource == "BLSUMMIT_BATTERY" {
                                            println!("  BLSUMMIT_BATTERY RegUp: {} MW @ ${}/MW = ${}", award, price, award * price);
                                            PRINTED_AS = true;
                                        }
                                    }
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (reg_down_awards.as_ref(), reg_down_prices.as_ref()) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("RegDown".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (rrs_awards.as_ref(), rrs_prices.as_ref()) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("RRS".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (ecrs_awards.as_ref(), ecrs_prices.as_ref()) {
                            if let (Some(award), Some(price)) = (awards.get(i), prices.get(i)) {
                                if award > 0.0 && price > 0.0 {
                                    *revenues.entry("ECRS".to_string()).or_insert(0.0) += award * price;
                                }
                            }
                        }
                        
                        if let (Some(awards), Some(prices)) = (non_spin_awards.as_ref(), non_spin_prices.as_ref()) {
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

    fn create_daily_rollups_split(&self, 
                           dam_revenues: HashMap<(String, NaiveDate), f64>,
                           rt_revenues: HashMap<(String, NaiveDate), f64>,
                           as_revenues: HashMap<(String, NaiveDate), HashMap<String, f64>>) 
                           -> Result<Vec<BessRevenue>> {
        println!("\n📅 Creating Daily Revenue Rollups...");
        
        let mut daily_revenues = Vec::new();
        
        // Get all unique resource-date combinations
        let mut all_keys = HashSet::new();
        for key in dam_revenues.keys() {
            all_keys.insert(key.clone());
        }
        for key in rt_revenues.keys() {
            all_keys.insert(key.clone());
        }
        for key in as_revenues.keys() {
            all_keys.insert(key.clone());
        }
        
        for (resource_name, date) in all_keys {
            let dam_rev = dam_revenues.get(&(resource_name.clone(), date)).unwrap_or(&0.0);
            let rt_rev = rt_revenues.get(&(resource_name.clone(), date)).unwrap_or(&0.0);
            let energy_rev = dam_rev + rt_rev;
            let as_rev = as_revenues.get(&(resource_name.clone(), date));
            
            let mut revenue = BessRevenue {
                resource_name: resource_name.clone(),
                date,
                energy_revenue: energy_rev,
                dam_energy_revenue: *dam_rev,
                rt_energy_revenue: *rt_rev,
                reg_up_revenue: 0.0,
                reg_down_revenue: 0.0,
                rrs_revenue: 0.0,
                ecrs_revenue: 0.0,
                non_spin_revenue: 0.0,
                total_revenue: energy_rev,
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
        let mut dam_energy_revs = Vec::new();
        let mut rt_energy_revs = Vec::new();
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
            dam_energy_revs.push(rev.dam_energy_revenue);
            rt_energy_revs.push(rev.rt_energy_revenue);
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
            Series::new("DAM_Energy_Revenue", dam_energy_revs),
            Series::new("RT_Energy_Revenue", rt_energy_revs),
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

    #[allow(dead_code)]
    fn get_sp_to_resources_map(&self) -> HashMap<String, Vec<String>> {
        let mut sp_map = HashMap::new();
        
        for (resource_name, (sp, _)) in &self.bess_resources {
            sp_map.entry(sp.clone())
                .or_insert_with(Vec::new)
                .push(resource_name.clone());
        }
        
        sp_map
    }
    
    fn generate_detailed_revenue_breakdown(&self, daily_revenues: &[BessRevenue]) -> Result<()> {
        println!("\n📊 Generating Detailed Revenue Breakdown...");
        
        // Calculate annual totals by resource and revenue stream
        let mut resource_totals: HashMap<String, HashMap<&str, f64>> = HashMap::new();
        let mut resource_days: HashMap<String, u32> = HashMap::new();
        
        for revenue in daily_revenues {
            let totals = resource_totals.entry(revenue.resource_name.clone())
                .or_insert_with(HashMap::new);
            
            *totals.entry("DAM_Energy").or_insert(0.0) += revenue.dam_energy_revenue;
            *totals.entry("RT_Energy").or_insert(0.0) += revenue.rt_energy_revenue;
            *totals.entry("Total_Energy").or_insert(0.0) += revenue.energy_revenue;
            *totals.entry("RegUp").or_insert(0.0) += revenue.reg_up_revenue;
            *totals.entry("RegDown").or_insert(0.0) += revenue.reg_down_revenue;
            *totals.entry("RRS").or_insert(0.0) += revenue.rrs_revenue;
            *totals.entry("ECRS").or_insert(0.0) += revenue.ecrs_revenue;
            *totals.entry("NonSpin").or_insert(0.0) += revenue.non_spin_revenue;
            *totals.entry("Total").or_insert(0.0) += revenue.total_revenue;
            
            *resource_days.entry(revenue.resource_name.clone()).or_insert(0) += 1;
        }
        
        // Create DataFrame with detailed breakdown
        let mut resource_names = Vec::new();
        let mut capacities = Vec::new();
        let mut dam_energy_totals = Vec::new();
        let mut rt_energy_totals = Vec::new();
        let mut total_energy_totals = Vec::new();
        let mut reg_up_totals = Vec::new();
        let mut reg_down_totals = Vec::new();
        let mut rrs_totals = Vec::new();
        let mut ecrs_totals = Vec::new();
        let mut non_spin_totals = Vec::new();
        let mut total_as_revenues = Vec::new();
        let mut grand_totals = Vec::new();
        let mut revenue_per_mw_year = Vec::new();
        
        // Sort resources by total revenue descending
        let mut sorted_resources: Vec<_> = resource_totals.iter().collect();
        sorted_resources.sort_by(|a, b| {
            let total_a = a.1.get("Total").unwrap_or(&0.0);
            let total_b = b.1.get("Total").unwrap_or(&0.0);
            total_b.partial_cmp(total_a).unwrap()
        });
        
        for (resource_name, totals) in sorted_resources {
            let days = *resource_days.get(resource_name).unwrap_or(&1) as f64;
            let annualization_factor = 365.0 / days;
            
            let capacity = self.bess_resources.get(resource_name)
                .map(|(_, cap)| *cap)
                .unwrap_or(0.0);
            
            resource_names.push(resource_name.clone());
            capacities.push(capacity);
            
            // Annualize all revenues
            let dam_annual = totals.get("DAM_Energy").unwrap_or(&0.0) * annualization_factor;
            let rt_annual = totals.get("RT_Energy").unwrap_or(&0.0) * annualization_factor;
            let total_energy_annual = totals.get("Total_Energy").unwrap_or(&0.0) * annualization_factor;
            let reg_up_annual = totals.get("RegUp").unwrap_or(&0.0) * annualization_factor;
            let reg_down_annual = totals.get("RegDown").unwrap_or(&0.0) * annualization_factor;
            let rrs_annual = totals.get("RRS").unwrap_or(&0.0) * annualization_factor;
            let ecrs_annual = totals.get("ECRS").unwrap_or(&0.0) * annualization_factor;
            let non_spin_annual = totals.get("NonSpin").unwrap_or(&0.0) * annualization_factor;
            let total_annual = totals.get("Total").unwrap_or(&0.0) * annualization_factor;
            
            dam_energy_totals.push(dam_annual);
            rt_energy_totals.push(rt_annual);
            total_energy_totals.push(total_energy_annual);
            reg_up_totals.push(reg_up_annual);
            reg_down_totals.push(reg_down_annual);
            rrs_totals.push(rrs_annual);
            ecrs_totals.push(ecrs_annual);
            non_spin_totals.push(non_spin_annual);
            
            let total_as = reg_up_annual + reg_down_annual + rrs_annual + ecrs_annual + non_spin_annual;
            total_as_revenues.push(total_as);
            grand_totals.push(total_annual);
            
            let per_mw = if capacity > 0.0 { total_annual / capacity } else { 0.0 };
            revenue_per_mw_year.push(per_mw);
        }
        
        // Calculate summary statistics before moving vectors
        let total_dam: f64 = dam_energy_totals.iter().sum();
        let total_rt: f64 = rt_energy_totals.iter().sum();
        let total_energy: f64 = total_energy_totals.iter().sum();
        let total_as: f64 = total_as_revenues.iter().sum();
        let grand_total: f64 = grand_totals.iter().sum();
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", resource_names),
            Series::new("Capacity_MW", capacities),
            Series::new("DAM_Energy_Revenue_Annual", dam_energy_totals),
            Series::new("RT_Energy_Revenue_Annual", rt_energy_totals),
            Series::new("Total_Energy_Revenue_Annual", total_energy_totals),
            Series::new("RegUp_Revenue_Annual", reg_up_totals),
            Series::new("RegDown_Revenue_Annual", reg_down_totals),
            Series::new("RRS_Revenue_Annual", rrs_totals),
            Series::new("ECRS_Revenue_Annual", ecrs_totals),
            Series::new("NonSpin_Revenue_Annual", non_spin_totals),
            Series::new("Total_AS_Revenue_Annual", total_as_revenues),
            Series::new("Total_Revenue_Annual", grand_totals),
            Series::new("Revenue_Per_MW_Year", revenue_per_mw_year),
        ])?;
        
        let output_path = self.output_dir.join("bess_revenue_breakdown_detailed.csv");
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut df.clone())?;
        
        println!("✅ Saved detailed revenue breakdown to: {}", output_path.display());
        
        println!("\n📊 Portfolio Revenue Summary (Annualized):");
        println!("  DAM Energy Revenue: ${:.2}M", total_dam / 1_000_000.0);
        println!("  RT Energy Revenue: ${:.2}M", total_rt / 1_000_000.0);
        println!("  Total Energy Revenue: ${:.2}M", total_energy / 1_000_000.0);
        println!("  Total AS Revenue: ${:.2}M", total_as / 1_000_000.0);
        println!("  Grand Total Revenue: ${:.2}M", grand_total / 1_000_000.0);
        
        // Calculate percentage breakdown
        if grand_total > 0.0 {
            println!("\n  Revenue Mix:");
            println!("    Energy: {:.1}%", (total_energy / grand_total) * 100.0);
            println!("    Ancillary Services: {:.1}%", (total_as / grand_total) * 100.0);
        }
        
        Ok(())
    }
}

pub fn calculate_bess_revenues() -> Result<()> {
    let master_list_path = PathBuf::from("bess_analysis/bess_resources_master_list.csv");
    let calculator = BessRevenueCalculator::new(&master_list_path)?;
    calculator.calculate_all_revenues()?;
    Ok(())
}