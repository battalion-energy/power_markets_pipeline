use chrono::{DateTime, Duration, NaiveDate, Utc};
use tbx_calculator::{
    models::{MarketType, PriceData},
    TbxCalculator, TbxConfig,
};

fn main() {
    // Create a TB2 configuration (2-hour battery)
    let config = TbxConfig::new_tb2(100.0); // 100 MW battery
    let calculator = TbxCalculator::new(config);

    // Create sample price data for one day
    let base_time = DateTime::parse_from_rfc3339("2024-01-15T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let mut prices = vec![];

    // Generate 24 hours of prices with clear arbitrage opportunity
    // Night hours (cheap): $20/MWh
    // Day hours (medium): $50/MWh  
    // Evening peak (expensive): $100/MWh
    for hour in 0..24 {
        let price = match hour {
            0..=5 | 21..=23 => 20.0,  // Night (cheap)
            18..=20 => 100.0,          // Evening peak (expensive)
            _ => 50.0,                 // Day (medium)
        };

        prices.push(PriceData {
            timestamp: base_time + Duration::hours(hour),
            settlement_point: "EXAMPLE_NODE".to_string(),
            price,
            market: MarketType::DayAhead,
        });
    }

    // Calculate arbitrage opportunities
    let result = calculator.calculate_daily_arbitrage(
        &prices,
        "EXAMPLE_BESS",
        "EXAMPLE_NODE",
        base_time.date_naive(),
    );

    // Print results
    println!("TBX Analysis Results");
    println!("===================");
    println!("Resource: {}", result.resource_name);
    println!("Date: {}", result.date);
    println!("Configuration: {} MW / {} MWh", 
        result.config.battery_power_mw, 
        result.config.battery_capacity_mwh
    );
    println!();
    println!("Day-Ahead Revenue: ${:.2}", result.revenue_da);
    println!("Average Spread: ${:.2}/MWh", result.avg_spread_da);
    println!("Utilization: {:.1}%", result.utilization_factor * 100.0);
    println!();
    
    if !result.da_windows.is_empty() {
        println!("Arbitrage Windows:");
        for (i, window) in result.da_windows.iter().enumerate() {
            println!("  Window {}:", i + 1);
            println!("    Charge: {} to {} @ ${:.2}/MWh",
                window.charge_start.format("%H:%M"),
                window.charge_end.format("%H:%M"),
                window.charge_price
            );
            println!("    Discharge: {} to {} @ ${:.2}/MWh",
                window.discharge_start.format("%H:%M"),
                window.discharge_end.format("%H:%M"),
                window.discharge_price
            );
            println!("    Energy: {:.1} MWh", window.energy_mwh);
            println!("    Revenue: ${:.2}", window.revenue);
        }
    }
}