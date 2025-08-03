use crate::models::{MarketType, PriceData};
use anyhow::Result;
use arrow::array::{Float64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::TimeUnit;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::sync::Arc;

pub struct DataLoader {
    use_arrow: bool,
}

impl DataLoader {
    pub fn new(use_arrow: bool) -> Self {
        Self { use_arrow }
    }

    /// Load day-ahead prices from Parquet file
    pub fn load_da_prices(&self, file_path: &str, settlement_points: &[String]) -> Result<Vec<PriceData>> {
        if self.use_arrow {
            self.load_da_prices_arrow(file_path, settlement_points)
        } else {
            self.load_da_prices_polars(file_path, settlement_points)
        }
    }

    /// Load real-time prices from Parquet file
    pub fn load_rt_prices(&self, file_path: &str, settlement_points: &[String]) -> Result<Vec<PriceData>> {
        if self.use_arrow {
            self.load_rt_prices_arrow(file_path, settlement_points)
        } else {
            self.load_rt_prices_polars(file_path, settlement_points)
        }
    }

    /// Load DA prices using Polars
    fn load_da_prices_polars(&self, file_path: &str, settlement_points: &[String]) -> Result<Vec<PriceData>> {
        let df = LazyFrame::scan_parquet(file_path, Default::default())?
            .filter(col("SettlementPoint").is_in(lit(Series::from_iter(settlement_points))))
            .collect()?;

        let mut prices = Vec::new();

        // Extract columns - adjust names based on actual schema
        let timestamps = df.column("DeliveryDate")?.datetime()?;
        let hours = df.column("DeliveryHour")?.i32()?;
        let points = df.column("SettlementPoint")?.str()?;
        let values = df.column("SettlementPointPrice")?.f64()?;

        for idx in 0..df.height() {
            if let (Some(date_val), Some(hour), Some(point), Some(price)) = (
                timestamps.get(idx),
                hours.get(idx),
                points.get(idx),
                values.get(idx),
            ) {
                // Convert to proper timestamp
                let timestamp = DateTime::<Utc>::from_timestamp(date_val / 1000, 0)
                    .unwrap()
                    .with_hour(hour as u32)
                    .unwrap();

                prices.push(PriceData {
                    timestamp,
                    settlement_point: point.to_string(),
                    price,
                    market: MarketType::DayAhead,
                });
            }
        }

        Ok(prices)
    }

    /// Load RT prices using Polars
    fn load_rt_prices_polars(&self, file_path: &str, settlement_points: &[String]) -> Result<Vec<PriceData>> {
        let df = LazyFrame::scan_parquet(file_path, Default::default())?
            .filter(col("SettlementPointName").is_in(lit(Series::from_iter(settlement_points))))
            .collect()?;

        let mut prices = Vec::new();

        // For SCED data with 15-minute intervals
        let timestamps = df.column("SCEDTimestamp")?.datetime()?;
        let points = df.column("SettlementPointName")?.str()?;
        let values = df.column("LMP")?.f64()?;

        for idx in 0..df.height() {
            if let (Some(timestamp_val), Some(point), Some(price)) = (
                timestamps.get(idx),
                points.get(idx),
                values.get(idx),
            ) {
                let timestamp = DateTime::<Utc>::from_timestamp(timestamp_val / 1000, 0).unwrap();

                prices.push(PriceData {
                    timestamp,
                    settlement_point: point.to_string(),
                    price,
                    market: MarketType::RealTime15Min,
                });
            }
        }

        Ok(prices)
    }

    /// Load DA prices using Arrow (for performance comparison)
    fn load_da_prices_arrow(&self, file_path: &str, settlement_points: &[String]) -> Result<Vec<PriceData>> {
        use arrow::record_batch::RecordBatchReader;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;

        let mut prices = Vec::new();
        let settlement_set: std::collections::HashSet<_> = settlement_points.iter().collect();

        for batch in reader {
            let batch = batch?;
            
            // Get column arrays
            let points = batch
                .column_by_name("SettlementPoint")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| anyhow::anyhow!("SettlementPoint column not found"))?;
            
            let dates = batch
                .column_by_name("DeliveryDate")
                .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
                .ok_or_else(|| anyhow::anyhow!("DeliveryDate column not found"))?;
            
            let hours = batch
                .column_by_name("DeliveryHour")
                .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int32Array>())
                .ok_or_else(|| anyhow::anyhow!("DeliveryHour column not found"))?;
            
            let values = batch
                .column_by_name("SettlementPointPrice")
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                .ok_or_else(|| anyhow::anyhow!("SettlementPointPrice column not found"))?;

            for row in 0..batch.num_rows() {
                if let Some(point) = points.value(row) {
                    if settlement_set.contains(&point.to_string()) {
                        let timestamp = DateTime::<Utc>::from_timestamp(
                            dates.value(row) / 1_000_000, // Convert microseconds to seconds
                            0,
                        )
                        .unwrap()
                        .with_hour(hours.value(row) as u32)
                        .unwrap();

                        prices.push(PriceData {
                            timestamp,
                            settlement_point: point.to_string(),
                            price: values.value(row),
                            market: MarketType::DayAhead,
                        });
                    }
                }
            }
        }

        Ok(prices)
    }

    /// Load RT prices using Arrow
    fn load_rt_prices_arrow(&self, file_path: &str, settlement_points: &[String]) -> Result<Vec<PriceData>> {
        // Similar implementation to load_da_prices_arrow but for RT data
        // Adjust column names as needed
        todo!("Implement Arrow-based RT price loading")
    }

    /// Load prices for a date range
    pub fn load_prices_range(
        &self,
        da_path_pattern: &str,
        rt_path_pattern: &str,
        settlement_points: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<PriceData>> {
        let mut all_prices = Vec::new();

        // Iterate through dates
        let mut current_date = start_date;
        while current_date <= end_date {
            // Format paths with date
            let da_path = da_path_pattern.replace("{date}", &current_date.format("%Y%m%d").to_string());
            let rt_path = rt_path_pattern.replace("{date}", &current_date.format("%Y%m%d").to_string());

            // Load DA prices if file exists
            if std::path::Path::new(&da_path).exists() {
                match self.load_da_prices(&da_path, settlement_points) {
                    Ok(prices) => all_prices.extend(prices),
                    Err(e) => log::warn!("Failed to load DA prices for {}: {}", current_date, e),
                }
            }

            // Load RT prices if file exists
            if std::path::Path::new(&rt_path).exists() {
                match self.load_rt_prices(&rt_path, settlement_points) {
                    Ok(prices) => all_prices.extend(prices),
                    Err(e) => log::warn!("Failed to load RT prices for {}: {}", current_date, e),
                }
            }

            current_date += chrono::Duration::days(1);
        }

        Ok(all_prices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_loader_creation() {
        let loader = DataLoader::new(false); // Use Polars
        assert!(!loader.use_arrow);

        let loader = DataLoader::new(true); // Use Arrow
        assert!(loader.use_arrow);
    }
}