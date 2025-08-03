use anyhow::Result;
use chrono::NaiveDate;
use clap::{Parser, ValueEnum};
use env_logger;
use log::info;
use std::path::Path;
use tbx_calculator::{
    BlendedOptimizer, DataLoader, SettlementMapper, TbxCalculator, TbxConfig,
};

#[derive(Parser)]
#[command(name = "tbx_calculator")]
#[command(about = "Calculate Top-Bottom X hours energy arbitrage for BESS")]
struct Args {
    /// TBX variant (TB1, TB2, or TB4)
    #[arg(short, long, value_enum)]
    variant: TbxVariant,

    /// Battery power in MW
    #[arg(short, long, default_value = "100.0")]
    power_mw: f64,

    /// Round-trip efficiency (0-1)
    #[arg(short, long, default_value = "0.85")]
    efficiency: f64,

    /// Start date (YYYY-MM-DD)
    #[arg(long)]
    start_date: String,

    /// End date (YYYY-MM-DD)
    #[arg(long)]
    end_date: String,

    /// Path to settlement point mapping CSV
    #[arg(long)]
    mapping_file: String,

    /// DA price data path pattern (use {date} for date substitution)
    #[arg(long)]
    da_path_pattern: String,

    /// RT price data path pattern (use {date} for date substitution)
    #[arg(long)]
    rt_path_pattern: String,

    /// Resource name to analyze (or "ALL" for all BESS)
    #[arg(short, long, default_value = "ALL")]
    resource: String,

    /// Output format
    #[arg(short, long, value_enum, default_value = "json")]
    output: OutputFormat,

    /// Use Arrow instead of Polars for data loading
    #[arg(long)]
    use_arrow: bool,

    /// Calculate blended DA+RT optimization
    #[arg(long)]
    blended: bool,
}

#[derive(Clone, ValueEnum)]
enum TbxVariant {
    TB1,
    TB2,
    TB4,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Json,
    Csv,
    Summary,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    info!("Starting TBX calculation");

    // Create configuration
    let config = match args.variant {
        TbxVariant::TB1 => TbxConfig::new_tb1(args.power_mw),
        TbxVariant::TB2 => TbxConfig::new_tb2(args.power_mw),
        TbxVariant::TB4 => TbxConfig::new_tb4(args.power_mw),
    };

    // Override efficiency if specified
    let mut config = config;
    config.round_trip_efficiency = args.efficiency;

    // Parse dates
    let start_date = NaiveDate::parse_from_str(&args.start_date, "%Y-%m-%d")?;
    let end_date = NaiveDate::parse_from_str(&args.end_date, "%Y-%m-%d")?;

    // Load settlement mappings
    info!("Loading settlement point mappings");
    let mapper = SettlementMapper::from_ercot_files(&args.mapping_file)?;

    // Determine resources to analyze
    let resources: Vec<_> = if args.resource == "ALL" {
        mapper.get_all_bess().into_iter().map(|m| m.clone()).collect()
    } else {
        mapper
            .get_mapping(&args.resource)
            .map(|m| vec![m.clone()])
            .unwrap_or_default()
    };

    if resources.is_empty() {
        anyhow::bail!("No resources found matching '{}'", args.resource);
    }

    info!("Analyzing {} resources", resources.len());

    // Create data loader
    let loader = DataLoader::new(args.use_arrow);

    // Process each resource
    let mut all_results = Vec::new();

    for resource in resources {
        info!("Processing {}", resource.resource_name);

        // Get settlement points
        let settlement_points = vec![resource.settlement_point.clone()];

        // Load price data
        let prices = loader.load_prices_range(
            &args.da_path_pattern,
            &args.rt_path_pattern,
            &settlement_points,
            start_date,
            end_date,
        )?;

        info!("Loaded {} price points", prices.len());

        // Calculate TBX for each day
        let calculator = TbxCalculator::new(config.clone());
        let mut current_date = start_date;

        while current_date <= end_date {
            // Filter prices for this day
            let day_prices: Vec<_> = prices
                .iter()
                .filter(|p| p.timestamp.date_naive() == current_date)
                .cloned()
                .collect();

            if !day_prices.is_empty() {
                let mut result = calculator.calculate_daily_arbitrage(
                    &day_prices,
                    &resource.resource_name,
                    &resource.settlement_point,
                    current_date,
                );

                // Calculate blended if requested
                if args.blended {
                    let da_prices: Vec<_> = day_prices
                        .iter()
                        .filter(|p| p.market == tbx_calculator::models::MarketType::DayAhead)
                        .cloned()
                        .collect();
                    
                    let rt_prices: Vec<_> = day_prices
                        .iter()
                        .filter(|p| {
                            matches!(
                                p.market,
                                tbx_calculator::models::MarketType::RealTime5Min
                                    | tbx_calculator::models::MarketType::RealTime15Min
                            )
                        })
                        .cloned()
                        .collect();

                    if !da_prices.is_empty() && !rt_prices.is_empty() {
                        let optimizer = BlendedOptimizer::new(config.clone());
                        let blended_windows = optimizer.optimize_blended(&da_prices, &rt_prices);
                        
                        result.blended_windows = blended_windows.clone();
                        result.revenue_blended = blended_windows.iter().map(|w| w.revenue).sum();
                        result.avg_spread_blended = if !blended_windows.is_empty() {
                            let total_spread: f64 = blended_windows
                                .iter()
                                .map(|w| (w.discharge_price - w.charge_price) * w.energy_mwh)
                                .sum();
                            let total_energy: f64 = blended_windows.iter().map(|w| w.energy_mwh).sum();
                            total_spread / total_energy
                        } else {
                            0.0
                        };
                    }
                }

                all_results.push(result);
            }

            current_date += chrono::Duration::days(1);
        }
    }

    // Output results
    match args.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&all_results)?;
            println!("{}", json);
        }
        OutputFormat::Csv => {
            println!("Resource,Date,Strategy,Revenue,AvgSpread,Utilization");
            for result in &all_results {
                println!(
                    "{},{},{},{:.2},{:.2},{:.2}",
                    result.resource_name,
                    result.date,
                    result.best_strategy(),
                    result.best_revenue(),
                    result.avg_spread_da.max(result.avg_spread_rt).max(result.avg_spread_blended),
                    result.utilization_factor
                );
            }
        }
        OutputFormat::Summary => {
            // Group by resource
            let mut resource_totals: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
            
            for result in &all_results {
                *resource_totals.entry(result.resource_name.clone()).or_insert(0.0) += result.best_revenue();
            }

            println!("TBX Analysis Summary");
            println!("===================");
            println!("Period: {} to {}", start_date, end_date);
            println!("Configuration: {} MW / {} MWh battery", args.power_mw, config.battery_capacity_mwh);
            println!("Efficiency: {:.1}%", config.round_trip_efficiency * 100.0);
            println!();
            println!("Total Revenue by Resource:");
            
            let mut sorted_resources: Vec<_> = resource_totals.into_iter().collect();
            sorted_resources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            
            for (resource, total_revenue) in sorted_resources {
                let days = (end_date - start_date).num_days() + 1;
                let daily_avg = total_revenue / days as f64;
                println!(
                    "  {}: ${:.2} total (${:.2}/day)",
                    resource, total_revenue, daily_avg
                );
            }
        }
    }

    Ok(())
}