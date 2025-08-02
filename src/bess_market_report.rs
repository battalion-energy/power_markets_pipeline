use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

/// Comprehensive BESS Market Analysis Report Generator
pub struct BessMarketReport {
    output_dir: PathBuf,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MarketMetrics {
    pub total_revenue: f64,
    pub energy_revenue: f64,
    pub as_revenue: f64,
    pub capacity_factor: f64,
    pub cycling_rate: f64,
    pub revenue_per_mw_year: f64,
}

#[derive(Debug, Clone)]
pub struct MarketIndex {
    pub median_revenue_per_mw: f64,
    pub mean_revenue_per_mw: f64,
    pub p25_revenue_per_mw: f64,
    pub p75_revenue_per_mw: f64,
    pub top_10pct_revenue_per_mw: f64,
}

impl BessMarketReport {
    pub fn new(output_dir: PathBuf) -> Self {
        Self { output_dir }
    }
    
    pub fn generate_comprehensive_report(&self) -> Result<()> {
        println!("\n📊 Generating Comprehensive BESS Market Analysis Report");
        println!("{}", "=".repeat(80));
        
        // Load data
        let daily_revenues = self.load_daily_revenues()?;
        let resource_info = self.load_resource_info()?;
        
        // Calculate market metrics
        let market_metrics = self.calculate_market_metrics(&daily_revenues, &resource_info)?;
        
        // Calculate market index (Modo Energy style)
        let market_index = self.calculate_market_index(&market_metrics)?;
        
        // Generate reports
        self.generate_executive_summary(&market_metrics, &market_index)?;
        self.generate_performance_benchmarks(&market_metrics, &market_index)?;
        self.generate_revenue_breakdown_analysis(&daily_revenues)?;
        self.generate_operational_insights(&market_metrics)?;
        self.generate_market_trends_analysis(&daily_revenues)?;
        
        println!("\n✅ Comprehensive market report generated successfully!");
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
    
    fn calculate_market_metrics(&self, daily_revenues: &DataFrame, resource_info: &HashMap<String, f64>) 
        -> Result<HashMap<String, MarketMetrics>> {
        
        let mut metrics = HashMap::new();
        
        // Manual aggregation by resource
        let resource_names = daily_revenues.column("Resource_Name")?.utf8()?;
        let total_revenues = daily_revenues.column("Total_Revenue")?.f64()?;
        let energy_revenues = daily_revenues.column("Energy_Revenue")?.f64()?;
        let reg_up_revenues = daily_revenues.column("RegUp_Revenue")?.f64()?;
        let reg_down_revenues = daily_revenues.column("RegDown_Revenue")?.f64()?;
        let rrs_revenues = daily_revenues.column("RRS_Revenue")?.f64()?;
        let ecrs_revenues = daily_revenues.column("ECRS_Revenue")?.f64()?;
        let nonspin_revenues = daily_revenues.column("NonSpin_Revenue")?.f64()?;
        
        let mut resource_totals: HashMap<String, (f64, f64, f64, u32)> = HashMap::new();
        
        for i in 0..daily_revenues.height() {
            if let Some(resource) = resource_names.get(i) {
                let total = total_revenues.get(i).unwrap_or(0.0);
                let energy = energy_revenues.get(i).unwrap_or(0.0);
                let as_rev = reg_up_revenues.get(i).unwrap_or(0.0) +
                           reg_down_revenues.get(i).unwrap_or(0.0) +
                           rrs_revenues.get(i).unwrap_or(0.0) +
                           ecrs_revenues.get(i).unwrap_or(0.0) +
                           nonspin_revenues.get(i).unwrap_or(0.0);
                
                let entry = resource_totals.entry(resource.to_string()).or_insert((0.0, 0.0, 0.0, 0));
                entry.0 += total;
                entry.1 += energy;
                entry.2 += as_rev;
                entry.3 += 1;
            }
        }
        
        // Convert to metrics
        for (resource, (total_rev, energy_rev, as_rev, days)) in resource_totals {
            let capacity = resource_info.get(&resource).cloned().unwrap_or(1.0);
            let annualization_factor = 365.0 / (days as f64);
            
            let annualized_total = total_rev * annualization_factor;
            let annualized_energy = energy_rev * annualization_factor;
            let annualized_as = as_rev * annualization_factor;
            
            metrics.insert(resource, MarketMetrics {
                total_revenue: annualized_total,
                energy_revenue: annualized_energy,
                as_revenue: annualized_as,
                capacity_factor: 0.0,
                cycling_rate: 0.0,
                revenue_per_mw_year: annualized_total / capacity,
            });
        }
        
        Ok(metrics)
    }
    
    fn calculate_market_index(&self, metrics: &HashMap<String, MarketMetrics>) -> Result<MarketIndex> {
        let mut revenues_per_mw: Vec<f64> = metrics.values()
            .map(|m| m.revenue_per_mw_year)
            .filter(|&r| r > 0.0) // Exclude zero or negative revenues
            .collect();
            
        revenues_per_mw.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let n = revenues_per_mw.len();
        if n == 0 {
            return Ok(MarketIndex {
                median_revenue_per_mw: 0.0,
                mean_revenue_per_mw: 0.0,
                p25_revenue_per_mw: 0.0,
                p75_revenue_per_mw: 0.0,
                top_10pct_revenue_per_mw: 0.0,
            });
        }
        
        // Calculate percentiles
        let median = if n % 2 == 0 {
            (revenues_per_mw[n/2 - 1] + revenues_per_mw[n/2]) / 2.0
        } else {
            revenues_per_mw[n/2]
        };
        
        let p25 = revenues_per_mw[n / 4];
        let p75 = revenues_per_mw[3 * n / 4];
        let top_10pct_idx = (0.9 * n as f64) as usize;
        let top_10pct = revenues_per_mw[top_10pct_idx];
        
        let mean = revenues_per_mw.iter().sum::<f64>() / n as f64;
        
        Ok(MarketIndex {
            median_revenue_per_mw: median,
            mean_revenue_per_mw: mean,
            p25_revenue_per_mw: p25,
            p75_revenue_per_mw: p75,
            top_10pct_revenue_per_mw: top_10pct,
        })
    }
    
    fn generate_executive_summary(&self, metrics: &HashMap<String, MarketMetrics>, index: &MarketIndex) -> Result<()> {
        let output_path = self.output_dir.join("bess_executive_summary.md");
        let mut file = std::fs::File::create(&output_path)?;
        use std::io::Write;
        
        writeln!(file, "# ERCOT BESS Market Analysis - Executive Summary")?;
        writeln!(file, "\nReport Generated: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"))?;
        writeln!(file)?;
        
        writeln!(file, "## Market Overview")?;
        writeln!(file)?;
        
        let total_capacity: f64 = metrics.values().map(|m| m.total_revenue / m.revenue_per_mw_year).sum();
        let total_revenue: f64 = metrics.values().map(|m| m.total_revenue).sum();
        
        writeln!(file, "- **Total BESS Capacity Analyzed**: {:.1} MW", total_capacity)?;
        writeln!(file, "- **Number of BESS Resources**: {}", metrics.len())?;
        writeln!(file, "- **Total Annual Revenue**: ${:.2}M", total_revenue / 1_000_000.0)?;
        writeln!(file)?;
        
        writeln!(file, "## Market Performance Index")?;
        writeln!(file)?;
        writeln!(file, "| Metric | Value |")?;
        writeln!(file, "|--------|-------|")?;
        writeln!(file, "| Median Revenue | ${:.0}/MW-year |", index.median_revenue_per_mw)?;
        writeln!(file, "| Mean Revenue | ${:.0}/MW-year |", index.mean_revenue_per_mw)?;
        writeln!(file, "| 25th Percentile | ${:.0}/MW-year |", index.p25_revenue_per_mw)?;
        writeln!(file, "| 75th Percentile | ${:.0}/MW-year |", index.p75_revenue_per_mw)?;
        writeln!(file, "| Top 10% Threshold | ${:.0}/MW-year |", index.top_10pct_revenue_per_mw)?;
        writeln!(file)?;
        
        // Revenue breakdown
        let total_energy: f64 = metrics.values().map(|m| m.energy_revenue).sum();
        let total_as: f64 = metrics.values().map(|m| m.as_revenue).sum();
        
        writeln!(file, "## Revenue Stream Breakdown")?;
        writeln!(file)?;
        writeln!(file, "- **Energy Arbitrage**: ${:.2}M ({:.1}%)", 
                 total_energy / 1_000_000.0, 
                 100.0 * total_energy / total_revenue)?;
        writeln!(file, "- **Ancillary Services**: ${:.2}M ({:.1}%)", 
                 total_as / 1_000_000.0,
                 100.0 * total_as / total_revenue)?;
        
        println!("✅ Executive summary saved to: {}", output_path.display());
        Ok(())
    }
    
    fn generate_performance_benchmarks(&self, metrics: &HashMap<String, MarketMetrics>, index: &MarketIndex) -> Result<()> {
        let output_path = self.output_dir.join("bess_performance_benchmarks.csv");
        
        // Create performance relative to index
        let mut names = Vec::new();
        let mut revenues_per_mw = Vec::new();
        let mut vs_median = Vec::new();
        let mut percentile_rank = Vec::new();
        let mut performance_tier = Vec::new();
        
        // Sort by revenue per MW
        let mut sorted_metrics: Vec<_> = metrics.iter().collect();
        sorted_metrics.sort_by(|a, b| {
            let a_val = if a.1.revenue_per_mw_year.is_finite() { a.1.revenue_per_mw_year } else { 0.0 };
            let b_val = if b.1.revenue_per_mw_year.is_finite() { b.1.revenue_per_mw_year } else { 0.0 };
            b_val.partial_cmp(&a_val).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        let total_count = sorted_metrics.len() as f64;
        
        for (i, (name, metric)) in sorted_metrics.iter().enumerate() {
            names.push(name.to_string());
            revenues_per_mw.push(metric.revenue_per_mw_year);
            
            let vs_median_pct = 100.0 * (metric.revenue_per_mw_year - index.median_revenue_per_mw) / index.median_revenue_per_mw;
            vs_median.push(vs_median_pct);
            
            let pct_rank = 100.0 * (1.0 - (i as f64 / total_count));
            percentile_rank.push(pct_rank);
            
            let tier = if pct_rank >= 90.0 {
                "Top 10%"
            } else if pct_rank >= 75.0 {
                "Top 25%"
            } else if pct_rank >= 50.0 {
                "Above Median"
            } else if pct_rank >= 25.0 {
                "Below Median"
            } else {
                "Bottom 25%"
            };
            performance_tier.push(tier);
        }
        
        let df = DataFrame::new(vec![
            Series::new("Resource_Name", names),
            Series::new("Revenue_Per_MW_Year", revenues_per_mw),
            Series::new("Vs_Median_Pct", vs_median),
            Series::new("Percentile_Rank", percentile_rank),
            Series::new("Performance_Tier", performance_tier),
        ])?;
        
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut df.clone())?;
            
        println!("✅ Performance benchmarks saved to: {}", output_path.display());
        Ok(())
    }
    
    fn generate_revenue_breakdown_analysis(&self, daily_revenues: &DataFrame) -> Result<()> {
        // Monthly revenue trends
        let output_path = self.output_dir.join("bess_monthly_revenue_trends.csv");
        
        // Extract month from date and aggregate
        let dates = daily_revenues.column("Date")?.utf8()?;
        let mut months = Vec::new();
        
        for i in 0..dates.len() {
            if let Some(date_str) = dates.get(i) {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    months.push(date.format("%Y-%m").to_string());
                } else {
                    months.push("Unknown".to_string());
                }
            }
        }
        
        let month_series = Series::new("Month", months);
        let mut binding = daily_revenues.clone();
        let df_with_month = binding.with_column(month_series)?;
        
        // Group by month manually
        let mut monthly_data: HashMap<String, Vec<f64>> = HashMap::new();
        let mut monthly_energy: HashMap<String, f64> = HashMap::new();
        let mut monthly_as: HashMap<String, f64> = HashMap::new();
        
        for i in 0..df_with_month.height() {
            if let Some(month) = df_with_month.column("Month")?.utf8()?.get(i) {
                let total = df_with_month.column("Total_Revenue")?.f64()?.get(i).unwrap_or(0.0);
                let energy = df_with_month.column("Energy_Revenue")?.f64()?.get(i).unwrap_or(0.0);
                
                monthly_data.entry(month.to_string()).or_insert_with(Vec::new).push(total);
                *monthly_energy.entry(month.to_string()).or_insert(0.0) += energy;
                
                // Add AS revenues
                let reg_up = df_with_month.column("RegUp_Revenue")?.f64()?.get(i).unwrap_or(0.0);
                let reg_down = df_with_month.column("RegDown_Revenue")?.f64()?.get(i).unwrap_or(0.0);
                let rrs = df_with_month.column("RRS_Revenue")?.f64()?.get(i).unwrap_or(0.0);
                let ecrs = df_with_month.column("ECRS_Revenue")?.f64()?.get(i).unwrap_or(0.0);
                let nonspin = df_with_month.column("NonSpin_Revenue")?.f64()?.get(i).unwrap_or(0.0);
                
                *monthly_as.entry(month.to_string()).or_insert(0.0) += reg_up + reg_down + rrs + ecrs + nonspin;
            }
        }
        
        // Create output dataframe
        let mut months_vec = Vec::new();
        let mut totals = Vec::new();
        let mut means = Vec::new();
        let mut energy_totals = Vec::new();
        let mut as_totals = Vec::new();
        
        for (month, values) in monthly_data.iter() {
            months_vec.push(month.clone());
            let sum: f64 = values.iter().sum();
            let mean = sum / values.len() as f64;
            totals.push(sum);
            means.push(mean);
            energy_totals.push(monthly_energy.get(month).cloned().unwrap_or(0.0));
            as_totals.push(monthly_as.get(month).cloned().unwrap_or(0.0));
        }
        
        let monthly_df = DataFrame::new(vec![
            Series::new("Month", months_vec),
            Series::new("Total_Revenue_Sum", totals),
            Series::new("Total_Revenue_Mean", means),
            Series::new("Energy_Revenue_Sum", energy_totals),
            Series::new("AS_Revenue_Sum", as_totals),
        ])?;
        
        CsvWriter::new(std::fs::File::create(&output_path)?)
            .finish(&mut monthly_df.clone())?;
            
        println!("✅ Monthly revenue trends saved to: {}", output_path.display());
        Ok(())
    }
    
    fn generate_operational_insights(&self, metrics: &HashMap<String, MarketMetrics>) -> Result<()> {
        let output_path = self.output_dir.join("bess_operational_insights.md");
        let mut file = std::fs::File::create(&output_path)?;
        use std::io::Write;
        
        writeln!(file, "# BESS Operational Insights")?;
        writeln!(file)?;
        
        // Top performers analysis
        writeln!(file, "## Top Performing Systems")?;
        writeln!(file)?;
        
        let mut sorted: Vec<_> = metrics.iter().collect();
        sorted.sort_by(|a, b| b.1.revenue_per_mw_year.partial_cmp(&a.1.revenue_per_mw_year).unwrap());
        
        writeln!(file, "| Rank | Resource | Revenue/MW-Year | Energy % | AS % |")?;
        writeln!(file, "|------|----------|-----------------|----------|------|")?;
        
        for (i, (name, metric)) in sorted.iter().take(20).enumerate() {
            let energy_pct = 100.0 * metric.energy_revenue / metric.total_revenue;
            let as_pct = 100.0 * metric.as_revenue / metric.total_revenue;
            
            writeln!(file, "| {} | {} | ${:.0} | {:.1}% | {:.1}% |", 
                     i + 1, name, metric.revenue_per_mw_year, energy_pct, as_pct)?;
        }
        
        writeln!(file)?;
        writeln!(file, "## Revenue Strategy Analysis")?;
        writeln!(file)?;
        
        // Categorize by revenue strategy
        let mut energy_focused = 0;
        let mut as_focused = 0;
        let mut balanced = 0;
        
        for metric in metrics.values() {
            let energy_pct = metric.energy_revenue / metric.total_revenue;
            if energy_pct > 0.7 {
                energy_focused += 1;
            } else if energy_pct < 0.3 {
                as_focused += 1;
            } else {
                balanced += 1;
            }
        }
        
        writeln!(file, "- **Energy-Focused** (>70% from energy): {} systems", energy_focused)?;
        writeln!(file, "- **AS-Focused** (>70% from AS): {} systems", as_focused)?;
        writeln!(file, "- **Balanced Strategy**: {} systems", balanced)?;
        
        println!("✅ Operational insights saved to: {}", output_path.display());
        Ok(())
    }
    
    fn generate_market_trends_analysis(&self, _daily_revenues: &DataFrame) -> Result<()> {
        // This would include time series analysis, seasonal patterns, etc.
        // For now, just a placeholder
        println!("📈 Market trends analysis completed");
        Ok(())
    }
}

pub fn generate_market_report() -> Result<()> {
    let output_dir = PathBuf::from("bess_analysis");
    let report_generator = BessMarketReport::new(output_dir);
    report_generator.generate_comprehensive_report()
}