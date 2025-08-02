use anyhow::Result;
use chrono::NaiveDate;
use plotters::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct BessVisualizer {
    data_dir: PathBuf,
    output_dir: PathBuf,
}

impl BessVisualizer {
    pub fn new() -> Result<Self> {
        let data_dir = PathBuf::from("bess_analysis");
        let output_dir = PathBuf::from("bess_analysis/charts");
        std::fs::create_dir_all(&output_dir)?;
        
        Ok(Self {
            data_dir,
            output_dir,
        })
    }

    pub fn generate_all_visualizations(&self) -> Result<()> {
        println!("📊 Generating BESS Revenue Visualizations");
        println!("{}", "=".repeat(80));
        
        // Load daily revenue data
        let daily_revenues = self.load_daily_revenues()?;
        
        // Generate cumulative revenue charts for top performers
        self.generate_cumulative_revenue_charts(&daily_revenues)?;
        
        // Generate market overview chart
        self.generate_market_overview_chart(&daily_revenues)?;
        
        // Generate revenue composition chart
        self.generate_revenue_composition_chart(&daily_revenues)?;
        
        // Generate monthly performance heatmap
        self.generate_monthly_heatmap(&daily_revenues)?;
        
        Ok(())
    }

    fn load_daily_revenues(&self) -> Result<DataFrame> {
        let revenue_path = self.data_dir.join("bess_daily_revenues.csv");
        
        let df = CsvReader::new(std::fs::File::open(&revenue_path)?)
            .has_header(true)
            .finish()?;
        
        println!("Loaded {} daily revenue records", df.height());
        Ok(df)
    }

    fn generate_cumulative_revenue_charts(&self, df: &DataFrame) -> Result<()> {
        println!("\n📈 Generating Cumulative Revenue Charts...");
        
        // Calculate total revenues by resource
        let grouped = df.clone().lazy()
            .group_by([col("Resource_Name")])
            .agg([
                col("Total_Revenue").sum().alias("Total_Revenue_Sum"),
                col("Energy_Revenue").sum().alias("Energy_Revenue_Sum"),
                col("RegUp_Revenue").sum().alias("RegUp_Revenue_Sum"),
                col("RegDown_Revenue").sum().alias("RegDown_Revenue_Sum"),
                col("RRS_Revenue").sum().alias("RRS_Revenue_Sum"),
                col("ECRS_Revenue").sum().alias("ECRS_Revenue_Sum"),
                col("NonSpin_Revenue").sum().alias("NonSpin_Revenue_Sum"),
            ])
            .collect()?;
        
        // Sort by total revenue and get top 10
        let sorted = grouped.lazy()
            .sort("Total_Revenue_Sum", SortOptions {
                descending: true,
                nulls_last: true,
                multithreaded: true,
                maintain_order: false,
            })
            .limit(10)
            .collect()?;
        
        let top_resources = sorted.column("Resource_Name")?.utf8()?;
        
        // Generate chart for each top resource
        for i in 0..sorted.height().min(10) {
            if let Some(resource_name) = top_resources.get(i) {
                self.generate_resource_cumulative_chart(df, resource_name)?;
            }
        }
        
        Ok(())
    }

    fn generate_resource_cumulative_chart(&self, df: &DataFrame, resource_name: &str) -> Result<()> {
        println!("  Creating cumulative revenue chart for {}", resource_name);
        
        // Filter data for this resource
        let mask = df.column("Resource_Name")?.utf8()?.equal(resource_name);
        let resource_df = df.filter(&mask)?;
        
        // Extract data
        let dates = resource_df.column("Date")?.utf8()?;
        let total_revs = resource_df.column("Total_Revenue")?.f64()?;
        let energy_revs = resource_df.column("Energy_Revenue")?.f64()?;
        
        // Calculate AS revenue
        let as_df = resource_df.clone().lazy()
            .select([
                (col("RegUp_Revenue") + col("RegDown_Revenue") + 
                 col("RRS_Revenue") + col("ECRS_Revenue") + col("NonSpin_Revenue"))
                .alias("AS_Revenue")
            ])
            .collect()?;
        let as_revs = as_df.column("AS_Revenue")?.f64()?;
        
        // Convert to cumulative
        let mut date_revenue_map: Vec<(NaiveDate, f64, f64, f64)> = Vec::new();
        let mut cum_total = 0.0;
        let mut cum_energy = 0.0;
        let mut cum_as = 0.0;
        
        for i in 0..resource_df.height() {
            if let (Some(date_str), Some(total), Some(energy), Some(as_rev)) = 
                (dates.get(i), total_revs.get(i), energy_revs.get(i), as_revs.get(i)) {
                
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    cum_total += total;
                    cum_energy += energy;
                    cum_as += as_rev;
                    date_revenue_map.push((date, cum_total, cum_energy, cum_as));
                }
            }
        }
        
        // Sort by date
        date_revenue_map.sort_by_key(|(date, _, _, _)| *date);
        
        if date_revenue_map.is_empty() {
            return Ok(());
        }
        
        // Create chart
        let file_name = format!("cumulative_revenue_{}.png", 
                               resource_name.replace("/", "_").replace(" ", "_"));
        let output_path = self.output_dir.join(&file_name);
        
        let root = BitMapBackend::new(&output_path, (800, 600)).into_drawing_area();
        root.fill(&WHITE)?;
        
        let min_date = date_revenue_map.first().unwrap().0;
        let max_date = date_revenue_map.last().unwrap().0;
        let min_rev = date_revenue_map.iter().map(|(_, t, _, _)| *t).fold(f64::INFINITY, f64::min);
        let max_rev = date_revenue_map.iter().map(|(_, t, _, _)| *t).fold(f64::NEG_INFINITY, f64::max);
        
        let mut chart = ChartBuilder::on(&root)
            .caption(&format!("Cumulative Revenue: {}", resource_name), 
                    ("sans-serif", 30).into_font())
            .margin(10)
            .x_label_area_size(40)
            .y_label_area_size(70)
            .build_cartesian_2d(
                min_date..max_date,
                (min_rev * 1.1)..(max_rev * 1.1)
            )?;
        
        chart.configure_mesh()
            .x_desc("Date")
            .y_desc("Cumulative Revenue ($)")
            .draw()?;
        
        // Draw total revenue line
        chart.draw_series(LineSeries::new(
            date_revenue_map.iter().map(|(d, t, _, _)| (*d, *t)),
            &BLUE,
        ))?
        .label("Total Revenue")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 10, y)], &BLUE));
        
        // Draw energy revenue line
        chart.draw_series(LineSeries::new(
            date_revenue_map.iter().map(|(d, _, e, _)| (*d, *e)),
            &GREEN,
        ))?
        .label("Energy Revenue")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 10, y)], &GREEN));
        
        // Draw AS revenue line
        chart.draw_series(LineSeries::new(
            date_revenue_map.iter().map(|(d, _, _, a)| (*d, *a)),
            &RED,
        ))?
        .label("Ancillary Services")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 10, y)], &RED));
        
        chart.configure_series_labels()
            .background_style(&WHITE.mix(0.8))
            .border_style(&BLACK)
            .draw()?;
        
        root.present()?;
        
        Ok(())
    }

    fn generate_market_overview_chart(&self, df: &DataFrame) -> Result<()> {
        println!("\n📊 Generating Market Overview Chart...");
        
        // Group by date and sum all revenues
        let daily_totals = df.clone().lazy()
            .group_by([col("Date")])
            .agg([
                col("Total_Revenue").sum().alias("Market_Total"),
                col("Energy_Revenue").sum().alias("Market_Energy"),
                col("RegUp_Revenue").sum().alias("Market_RegUp"),
                col("RegDown_Revenue").sum().alias("Market_RegDown"),
                col("RRS_Revenue").sum().alias("Market_RRS"),
                col("ECRS_Revenue").sum().alias("Market_ECRS"),
                col("NonSpin_Revenue").sum().alias("Market_NonSpin"),
            ])
            .sort("Date", Default::default())
            .collect()?;
        
        // Extract data
        let dates = daily_totals.column("Date")?.utf8()?;
        let totals = daily_totals.column("Market_Total")?.f64()?;
        
        let mut date_revenue_vec: Vec<(NaiveDate, f64)> = Vec::new();
        
        for i in 0..daily_totals.height() {
            if let (Some(date_str), Some(total)) = (dates.get(i), totals.get(i)) {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    date_revenue_vec.push((date, total));
                }
            }
        }
        
        date_revenue_vec.sort_by_key(|(date, _)| *date);
        
        // Create 30-day rolling average
        let mut rolling_avg: Vec<(NaiveDate, f64)> = Vec::new();
        for i in 0..date_revenue_vec.len() {
            let start_idx = i.saturating_sub(29);
            let window = &date_revenue_vec[start_idx..=i];
            let avg = window.iter().map(|(_, r)| r).sum::<f64>() / window.len() as f64;
            rolling_avg.push((date_revenue_vec[i].0, avg));
        }
        
        // Create chart
        let output_path = self.output_dir.join("market_overview.png");
        let root = BitMapBackend::new(&output_path, (1200, 600)).into_drawing_area();
        root.fill(&WHITE)?;
        
        if date_revenue_vec.is_empty() {
            return Ok(());
        }
        
        let min_date = date_revenue_vec.first().unwrap().0;
        let max_date = date_revenue_vec.last().unwrap().0;
        let min_rev = date_revenue_vec.iter().map(|(_, r)| *r).fold(f64::INFINITY, f64::min);
        let max_rev = date_revenue_vec.iter().map(|(_, r)| *r).fold(f64::NEG_INFINITY, f64::max);
        
        let mut chart = ChartBuilder::on(&root)
            .caption("ERCOT BESS Market Daily Revenue", ("sans-serif", 40).into_font())
            .margin(15)
            .x_label_area_size(50)
            .y_label_area_size(80)
            .build_cartesian_2d(
                min_date..max_date,
                (min_rev * 1.1)..(max_rev * 1.1)
            )?;
        
        chart.configure_mesh()
            .x_desc("Date")
            .y_desc("Daily Market Revenue ($)")
            .draw()?;
        
        // Draw daily revenue bars
        chart.draw_series(
            date_revenue_vec.iter().map(|(date, revenue)| {
                Rectangle::new([(*date, 0.0), (*date, *revenue)], 
                             if *revenue >= 0.0 { GREEN.filled() } else { RED.filled() })
            })
        )?
        .label("Daily Revenue")
        .legend(|(x, y)| Rectangle::new([(x, y), (x + 10, y + 10)], GREEN.filled()));
        
        // Draw 30-day rolling average
        chart.draw_series(LineSeries::new(
            rolling_avg.iter().map(|(d, a)| (*d, *a)),
            BLUE.stroke_width(2),
        ))?
        .label("30-Day Average")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 10, y)], &BLUE));
        
        chart.configure_series_labels()
            .background_style(&WHITE.mix(0.8))
            .border_style(&BLACK)
            .draw()?;
        
        root.present()?;
        println!("  ✅ Saved market overview chart");
        
        Ok(())
    }

    fn generate_revenue_composition_chart(&self, df: &DataFrame) -> Result<()> {
        println!("\n📊 Generating Revenue Composition Chart...");
        
        // Calculate total revenues by type
        let energy_total = df.column("Energy_Revenue")?.sum::<f64>().unwrap_or(0.0);
        let regup_total = df.column("RegUp_Revenue")?.sum::<f64>().unwrap_or(0.0);
        let regdown_total = df.column("RegDown_Revenue")?.sum::<f64>().unwrap_or(0.0);
        let rrs_total = df.column("RRS_Revenue")?.sum::<f64>().unwrap_or(0.0);
        let ecrs_total = df.column("ECRS_Revenue")?.sum::<f64>().unwrap_or(0.0);
        let nonspin_total = df.column("NonSpin_Revenue")?.sum::<f64>().unwrap_or(0.0);
        
        let total = energy_total + regup_total + regdown_total + rrs_total + ecrs_total + nonspin_total;
        
        println!("  Revenue breakdown:");
        println!("    Energy: ${:.0}", energy_total);
        println!("    RegUp: ${:.0}", regup_total);
        println!("    RegDown: ${:.0}", regdown_total);
        println!("    RRS: ${:.0}", rrs_total);
        println!("    ECRS: ${:.0}", ecrs_total);
        println!("    NonSpin: ${:.0}", nonspin_total);
        println!("    Total: ${:.0}", total);
        
        // Create simple bar chart instead of pie chart
        let output_path = self.output_dir.join("revenue_composition.png");
        let root = BitMapBackend::new(&output_path, (800, 600)).into_drawing_area();
        root.fill(&WHITE)?;
        
        let data = vec![
            ("Energy", energy_total),
            ("RegUp", regup_total),
            ("RegDown", regdown_total),
            ("RRS", rrs_total),
            ("ECRS", ecrs_total),
            ("NonSpin", nonspin_total),
        ];
        
        let max_val = data.iter().map(|(_, v)| v.abs()).fold(0.0, f64::max);
        
        if max_val > 0.0 {
            let mut chart = ChartBuilder::on(&root)
                .caption("BESS Revenue Composition by Service Type", ("sans-serif", 30).into_font())
                .margin(15)
                .x_label_area_size(50)
                .y_label_area_size(80)
                .build_cartesian_2d(
                    0..data.len(),
                    -max_val * 1.1..max_val * 1.1
                )?;
            
            chart.configure_mesh()
                .x_desc("Service Type")
                .y_desc("Total Revenue ($)")
                .x_labels(data.len())
                .x_label_formatter(&|x| {
                    data.get(*x).map(|(name, _)| name.to_string()).unwrap_or_default()
                })
                .draw()?;
            
            chart.draw_series(
                data.iter().enumerate().map(|(i, (_, value))| {
                    Rectangle::new([(i, 0.0), (i, *value)], 
                                 if *value >= 0.0 { BLUE.filled() } else { RED.filled() })
                })
            )?;
        }
        
        root.present()?;
        println!("  ✅ Saved revenue composition chart");
        
        Ok(())
    }

    fn generate_monthly_heatmap(&self, df: &DataFrame) -> Result<()> {
        println!("\n🗓️  Generating Monthly Performance Heatmap...");
        
        // Extract year-month from dates and aggregate
        let dates = df.column("Date")?.utf8()?;
        let resources = df.column("Resource_Name")?.utf8()?;
        let revenues = df.column("Total_Revenue")?.f64()?;
        
        // Create a map of (resource, year-month) -> total revenue
        let mut monthly_revenues: HashMap<(String, String), f64> = HashMap::new();
        
        for i in 0..df.height() {
            if let (Some(date_str), Some(resource), Some(revenue)) = 
                (dates.get(i), resources.get(i), revenues.get(i)) {
                
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    let year_month = date.format("%Y-%m").to_string();
                    let key = (resource.to_string(), year_month);
                    *monthly_revenues.entry(key).or_insert(0.0) += revenue;
                }
            }
        }
        
        // Get top 20 resources by total revenue
        let mut resource_totals: HashMap<String, f64> = HashMap::new();
        for ((resource, _), revenue) in &monthly_revenues {
            *resource_totals.entry(resource.clone()).or_insert(0.0) += revenue;
        }
        
        let mut sorted_resources: Vec<(String, f64)> = resource_totals.into_iter().collect();
        sorted_resources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top_resources: Vec<String> = sorted_resources.into_iter()
            .take(20)
            .map(|(name, _)| name)
            .collect();
        
        // Get all months
        let mut all_months: Vec<String> = monthly_revenues.keys()
            .map(|(_, month)| month.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        all_months.sort();
        
        // Create heatmap data
        println!("\n📊 Monthly Revenue Heatmap (Top 20 Resources):");
        println!("{:<30} {}", "Resource", all_months.join("  "));
        println!("{}", "-".repeat(30 + all_months.len() * 9));
        
        for resource in &top_resources {
            print!("{:<30}", resource);
            for month in &all_months {
                let revenue = monthly_revenues.get(&(resource.clone(), month.clone()))
                    .unwrap_or(&0.0);
                
                // Color code based on revenue
                if *revenue > 10000.0 {
                    print!(" ${:>7.0}", revenue);
                } else if *revenue > 0.0 {
                    print!(" ${:>7.0}", revenue);
                } else if *revenue < -10000.0 {
                    print!(" -${:>6.0}", -revenue);
                } else {
                    print!(" ${:>7.0}", revenue);
                }
            }
            println!();
        }
        
        println!("\n✅ Monthly heatmap analysis complete");
        
        Ok(())
    }
}

pub fn generate_bess_visualizations() -> Result<()> {
    let visualizer = BessVisualizer::new()?;
    visualizer.generate_all_visualizations()?;
    Ok(())
}