use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tbx_calculator::{DataLoader, TbxCalculator, TbxConfig};

fn benchmark_polars_loading(c: &mut Criterion) {
    c.bench_function("polars_load_da_prices", |b| {
        let loader = DataLoader::new(false); // Use Polars
        let settlement_points = vec!["TEST_NODE".to_string()];
        
        b.iter(|| {
            // This would need a real test file
            let _prices = black_box(
                loader.load_da_prices("test_data/da_prices.parquet", &settlement_points)
            );
        });
    });
}

fn benchmark_arrow_loading(c: &mut Criterion) {
    c.bench_function("arrow_load_da_prices", |b| {
        let loader = DataLoader::new(true); // Use Arrow
        let settlement_points = vec!["TEST_NODE".to_string()];
        
        b.iter(|| {
            // This would need a real test file
            let _prices = black_box(
                loader.load_da_prices("test_data/da_prices.parquet", &settlement_points)
            );
        });
    });
}

fn benchmark_tbx_calculation(c: &mut Criterion) {
    use chrono::{DateTime, Duration, Utc};
    use tbx_calculator::models::{MarketType, PriceData};
    
    // Generate test data
    let base_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    
    let mut prices = vec![];
    for hour in 0..24 {
        let price = if hour < 6 || hour > 20 {
            20.0
        } else if hour >= 18 && hour <= 20 {
            100.0
        } else {
            50.0
        };
        
        prices.push(PriceData {
            timestamp: base_time + Duration::hours(hour),
            settlement_point: "TEST_NODE".to_string(),
            price,
            market: MarketType::DayAhead,
        });
    }
    
    c.bench_function("tb2_calculation", |b| {
        let config = TbxConfig::new_tb2(100.0);
        let calculator = TbxCalculator::new(config);
        
        b.iter(|| {
            let _result = black_box(
                calculator.calculate_daily_arbitrage(
                    &prices,
                    "TEST_BATTERY",
                    "TEST_NODE",
                    base_time.date_naive(),
                )
            );
        });
    });
}

criterion_group!(
    benches,
    benchmark_polars_loading,
    benchmark_arrow_loading,
    benchmark_tbx_calculation
);
criterion_main!(benches);