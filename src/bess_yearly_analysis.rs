use anyhow::Result;
use chrono::{NaiveDate, Datelike};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct BessYearlyAnalysis {
    output_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct YearlyRevenue {
    pub year: i32,
    pub resource_name: String,
    pub capacity_mw: f64,
    pub energy_revenue: f64,
    pub dam_energy_revenue: f64,
    pub rt_energy_revenue: f64,
    pub reg_up_revenue: f64,
    pub reg_down_revenue: f64,
    pub rrs_revenue: f64,
    pub ecrs_revenue: f64,
    pub nonspin_revenue: f64,
    pub total_as_revenue: f64,
    pub total_revenue: f64,
    pub days_operating: u32,
}

impl BessYearlyAnalysis {
    pub fn new(output_dir: PathBuf) -> Self {
        Self { output_dir }
    }
    
    pub fn generate_yearly_analysis(&self) -> Result<()> {
        println!("\n📅 Generating Year-by-Year BESS Revenue Analysis");
        println!("{}", "=".repeat(80));
        
        // Load daily revenues
        let daily_revenues = self.load_daily_revenues()?;
        let resource_info = self.load_resource_info()?;
        
        // Process by year
        let yearly_data = self.calculate_yearly_revenues(&daily_revenues, &resource_info)?;
        
        // Generate reports
        self.generate_yearly_summary(&yearly_data)?;
        self.generate_revenue_stream_trends(&yearly_data)?;
        self.generate_energy_transition_analysis(&yearly_data)?;
        
        println!("\n✅ Yearly analysis complete!");
        Ok(())
    }
    
    fn load_daily_revenues(&self) -> Result<DataFrame> {
        let path = self.output_dir.join("bess_daily_revenues.csv");
        CsvReader::new(std::fs::File::open(&path)?)
            .has_header(true)
            .finish()
            .map_err(Into::into)
    }
    
    fn load_resource_info(&self) -> Result<HashMap<String, f64>> {
        let path = self.output_dir.join("bess_resources_master_list.csv");
        let df = CsvReader::new(std::fs::File::open(&path)?)
            .has_header(true)
            .finish()?;
            
        let mut capacities = HashMap::new();
        if let (Ok(names), Ok(caps)) = (
            df.column("Resource_Name")?.utf8(),
            df.column("Max_Capacity_MW")?.f64()
        ) {
            for i in 0..df.height() {
                if let (Some(name), Some(cap)) = (names.get(i), caps.get(i)) {
                    capacities.insert(name.to_string(), cap);
                }
            }
        }
        
        Ok(capacities)
    }
    
    fn calculate_yearly_revenues(&self, daily_revenues: &DataFrame, resource_info: &HashMap<String, f64>) 
        -> Result<Vec<YearlyRevenue>> {
        
        let mut yearly_data = Vec::new();
        
        // Extract year from date
        let dates = daily_revenues.column("Date")?.utf8()?;
        let mut years = Vec::new();
        
        for i in 0..dates.len() {
            if let Some(date_str) = dates.get(i) {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    years.push(date.year());
                } else {
                    years.push(0);
                }
            }
        }
        
        // Add year column
        let year_series = Series::new("Year", years);
        let mut df_with_year = daily_revenues.clone();
        df_with_year = df_with_year.with_column(year_series)?.clone();
        
        // Group by resource and year
        let resource_names = daily_revenues.column("Resource_Name")?.utf8()?;
        let unique_resources: Vec<String> = resource_names.into_iter()
            .filter_map(|x| x.map(|s| s.to_string()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
            
        let unique_years: Vec<i32> = df_with_year.column("Year")?.i32()?
            .into_iter()
            .filter_map(|x| x)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .filter(|&y| y > 2010 && y < 2030)
            .collect();
        
        // Process each resource-year combination
        for resource in &unique_resources {
            for &year in &unique_years {
                // Filter for this resource and year
                let resource_mask = df_with_year.column("Resource_Name")?.utf8()?
                    .into_iter()
                    .map(|v| v == Some(resource.as_str()))
                    .collect::<BooleanChunked>();
                let year_mask = df_with_year.column("Year")?.i32()?
                    .into_iter()
                    .map(|v| v == Some(year))
                    .collect::<BooleanChunked>();
                let mask = resource_mask & year_mask;
                    
                if let Ok(filtered) = df_with_year.filter(&mask) {
                    if filtered.height() > 0 {
                        let yearly_rev = self.aggregate_yearly_revenue(&filtered, resource, year, resource_info)?;
                        yearly_data.push(yearly_rev);
                    }
                }
            }
        }
        
        Ok(yearly_data)
    }
    
    fn aggregate_yearly_revenue(&self, df: &DataFrame, resource: &str, year: i32, 
                               resource_info: &HashMap<String, f64>) -> Result<YearlyRevenue> {
        
        // Sum all revenue columns
        let total_revenue: f64 = df.column("Total_Revenue")?.f64()?.sum().unwrap_or(0.0);
        let energy_revenue: f64 = df.column("Energy_Revenue")?.f64()?.sum().unwrap_or(0.0);
        // For now, assume all energy revenue is from DAM since RT is showing $0
        let dam_energy: f64 = energy_revenue;
        let rt_energy: f64 = 0.0;
        let reg_up: f64 = df.column("RegUp_Revenue")?.f64()?.sum().unwrap_or(0.0);
        let reg_down: f64 = df.column("RegDown_Revenue")?.f64()?.sum().unwrap_or(0.0);
        let rrs: f64 = df.column("RRS_Revenue")?.f64()?.sum().unwrap_or(0.0);
        let ecrs: f64 = df.column("ECRS_Revenue")?.f64()?.sum().unwrap_or(0.0);
        let nonspin: f64 = df.column("NonSpin_Revenue")?.f64()?.sum().unwrap_or(0.0);
        
        let total_as = reg_up + reg_down + rrs + ecrs + nonspin;
        let capacity = resource_info.get(resource).cloned().unwrap_or(1.0);
        
        Ok(YearlyRevenue {
            year,
            resource_name: resource.to_string(),
            capacity_mw: capacity,
            energy_revenue,
            dam_energy_revenue: dam_energy,
            rt_energy_revenue: rt_energy,
            reg_up_revenue: reg_up,
            reg_down_revenue: reg_down,
            rrs_revenue: rrs,
            ecrs_revenue: ecrs,
            nonspin_revenue: nonspin,
            total_as_revenue: total_as,
            total_revenue,
            days_operating: df.height() as u32,
        })
    }
    
    fn generate_yearly_summary(&self, yearly_data: &[YearlyRevenue]) -> Result<()> {
        let output_path = self.output_dir.join("bess_yearly_summary.csv");
        
        // Group by year
        let mut year_summaries: HashMap<i32, (f64, f64, f64, f64, u32)> = HashMap::new();
        
        for data in yearly_data {
            let entry = year_summaries.entry(data.year)
                .or_insert((0.0, 0.0, 0.0, 0.0, 0));
            
            entry.0 += data.total_revenue;
            entry.1 += data.energy_revenue;
            entry.2 += data.total_as_revenue;
            entry.3 += data.capacity_mw;
            entry.4 += 1;
        }
        
        // Create output
        let mut years = Vec::new();
        let mut total_revenues = Vec::new();
        let mut energy_revenues = Vec::new();
        let mut as_revenues = Vec::new();
        let mut energy_percentages = Vec::new();
        let mut total_capacity = Vec::new();
        let mut resource_count = Vec::new();
        let mut avg_revenue_per_mw = Vec::new();
        
        let mut sorted_years: Vec<_> = year_summaries.keys().cloned().collect();
        sorted_years.sort();
        
        println!("\n📊 Yearly Revenue Summary:");
        println!("Year | Resources | Capacity | Total Rev | Energy Rev | AS Rev | Energy %");
        println!("{}", "-".repeat(80));
        
        for year in &sorted_years {
            if let Some((total, energy, as_rev, capacity, count)) = year_summaries.get(&year) {
                years.push(year);
                total_revenues.push(*total);
                energy_revenues.push(*energy);
                as_revenues.push(*as_rev);
                let energy_pct = if *total != 0.0 { 100.0 * energy / total } else { 0.0 };
                energy_percentages.push(energy_pct);
                total_capacity.push(*capacity);
                resource_count.push(*count);
                let avg_per_mw = if *capacity > 0.0 { total / capacity } else { 0.0 };
                avg_revenue_per_mw.push(avg_per_mw);
                
                println!("{} | {:>9} | {:>8.1} MW | ${:>8.2}M | ${:>9.2}M | ${:>6.2}M | {:>6.1}%",
                         year, count, capacity, 
                         total / 1_000_000.0, 
                         energy / 1_000_000.0,
                         as_rev / 1_000_000.0,
                         energy_pct);
            }
        }
        
        // Convert Vec<&i32> to Vec<i32>
        let years_owned: Vec<i32> = years.into_iter().map(|&y| y).collect();
        
        let df = DataFrame::new(vec![
            Series::new("Year", years_owned),
            Series::new("Resource_Count", resource_count),
            Series::new("Total_Capacity_MW", total_capacity),
            Series::new("Total_Revenue", total_revenues),
            Series::new("Energy_Revenue", energy_revenues),
            Series::new("AS_Revenue", as_revenues),
            Series::new("Energy_Revenue_Pct", energy_percentages),
            Series::new("Avg_Revenue_Per_MW", avg_revenue_per_mw),
        ])?;
        
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut df.clone())?;
            
        println!("\n✅ Yearly summary saved to: {}", output_path.display());
        Ok(())
    }
    
    fn generate_revenue_stream_trends(&self, yearly_data: &[YearlyRevenue]) -> Result<()> {
        let output_path = self.output_dir.join("bess_revenue_stream_trends.csv");
        
        // Create detailed breakdown by resource and year
        let mut entries = Vec::new();
        
        for data in yearly_data {
            entries.push((
                data.resource_name.clone(),
                data.year,
                data.capacity_mw,
                data.energy_revenue,
                data.dam_energy_revenue,
                data.rt_energy_revenue,
                data.reg_up_revenue,
                data.reg_down_revenue,
                data.rrs_revenue,
                data.ecrs_revenue,
                data.nonspin_revenue,
                data.total_revenue,
                data.days_operating,
            ));
        }
        
        // Sort by resource name and year
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        
        // Create vectors for DataFrame
        let mut resources = Vec::new();
        let mut years = Vec::new();
        let mut capacities = Vec::new();
        let mut energy_revs = Vec::new();
        let mut dam_revs = Vec::new();
        let mut rt_revs = Vec::new();
        let mut reg_up_revs = Vec::new();
        let mut reg_down_revs = Vec::new();
        let mut rrs_revs = Vec::new();
        let mut ecrs_revs = Vec::new();
        let mut nonspin_revs = Vec::new();
        let mut total_revs = Vec::new();
        let mut days_ops = Vec::new();
        
        for entry in entries {
            resources.push(entry.0);
            years.push(entry.1);
            capacities.push(entry.2);
            energy_revs.push(entry.3);
            dam_revs.push(entry.4);
            rt_revs.push(entry.5);
            reg_up_revs.push(entry.6);
            reg_down_revs.push(entry.7);
            rrs_revs.push(entry.8);
            ecrs_revs.push(entry.9);
            nonspin_revs.push(entry.10);
            total_revs.push(entry.11);
            days_ops.push(entry.12);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", resources),
            Series::new("Year", years),
            Series::new("Capacity_MW", capacities),
            Series::new("Energy_Revenue", energy_revs),
            Series::new("DAM_Energy_Revenue", dam_revs),
            Series::new("RT_Energy_Revenue", rt_revs),
            Series::new("RegUp_Revenue", reg_up_revs),
            Series::new("RegDown_Revenue", reg_down_revs),
            Series::new("RRS_Revenue", rrs_revs),
            Series::new("ECRS_Revenue", ecrs_revs),
            Series::new("NonSpin_Revenue", nonspin_revs),
            Series::new("Total_Revenue", total_revs),
            Series::new("Days_Operating", days_ops),
        ])?;
        
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut df.clone())?;
            
        println!("✅ Revenue stream trends saved to: {}", output_path.display());
        Ok(())
    }
    
    fn generate_energy_transition_analysis(&self, yearly_data: &[YearlyRevenue]) -> Result<()> {
        println!("\n📈 Energy Revenue Transition Analysis:");
        println!("{}", "=".repeat(80));
        
        // Group by year and calculate statistics
        let mut year_stats: HashMap<i32, Vec<f64>> = HashMap::new();
        
        for data in yearly_data {
            if data.total_revenue > 0.0 {
                let energy_pct = 100.0 * data.energy_revenue / data.total_revenue;
                year_stats.entry(data.year)
                    .or_insert_with(Vec::new)
                    .push(energy_pct);
            }
        }
        
        println!("\nYear | Avg Energy % | Median Energy % | # Positive Energy | # Negative Energy");
        println!("{}", "-".repeat(80));
        
        let mut sorted_years: Vec<_> = year_stats.keys().cloned().collect();
        sorted_years.sort();
        
        for year in &sorted_years {
            if let Some(percentages) = year_stats.get(&year) {
                let avg = percentages.iter().sum::<f64>() / percentages.len() as f64;
                
                let mut sorted_pcts = percentages.clone();
                sorted_pcts.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let median = if sorted_pcts.len() % 2 == 0 {
                    (sorted_pcts[sorted_pcts.len()/2 - 1] + sorted_pcts[sorted_pcts.len()/2]) / 2.0
                } else {
                    sorted_pcts[sorted_pcts.len()/2]
                };
                
                let positive_count = yearly_data.iter()
                    .filter(|d| d.year == *year && d.energy_revenue > 0.0)
                    .count();
                    
                let negative_count = yearly_data.iter()
                    .filter(|d| d.year == *year && d.energy_revenue < 0.0)
                    .count();
                
                println!("{} | {:>12.1}% | {:>15.1}% | {:>17} | {:>17}",
                         year, avg, median, positive_count, negative_count);
            }
        }
        
        // Identify trends
        println!("\n🔍 Key Trends:");
        
        // Check if energy revenue is becoming less negative over time
        let recent_years: Vec<i32> = sorted_years.iter()
            .filter(|&&y| y >= 2022)
            .cloned()
            .collect();
            
        if recent_years.len() >= 2 {
            let mut recent_energy_pcts = Vec::new();
            for &year in &recent_years {
                if let Some(pcts) = year_stats.get(&year) {
                    recent_energy_pcts.push(pcts.iter().sum::<f64>() / pcts.len() as f64);
                }
            }
            
            if recent_energy_pcts.len() >= 2 {
                let trend = recent_energy_pcts.last().unwrap() - recent_energy_pcts.first().unwrap();
                if trend > 0.0 {
                    println!("✅ Energy revenue share is INCREASING in recent years (+{:.1}% points)", trend);
                } else {
                    println!("❌ Energy revenue share is DECREASING in recent years ({:.1}% points)", trend);
                }
            }
        }
        
        Ok(())
    }
}

pub fn generate_yearly_analysis() -> Result<()> {
    let output_dir = PathBuf::from("bess_analysis");
    let analyzer = BessYearlyAnalysis::new(output_dir);
    analyzer.generate_yearly_analysis()
}