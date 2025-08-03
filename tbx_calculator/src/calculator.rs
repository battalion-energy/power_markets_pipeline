use crate::models::{ArbitrageWindow, MarketType, PriceData, TbxConfig, TbxResult};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use std::collections::HashMap;

pub struct TbxCalculator {
    config: TbxConfig,
}

impl TbxCalculator {
    pub fn new(config: TbxConfig) -> Self {
        Self { config }
    }

    /// Calculate arbitrage opportunities for a single day
    pub fn calculate_daily_arbitrage(
        &self,
        prices: &[PriceData],
        resource_name: &str,
        settlement_point: &str,
        date: NaiveDate,
    ) -> TbxResult {
        let mut result = TbxResult::new(
            resource_name.to_string(),
            settlement_point.to_string(),
            date,
            self.config.clone(),
        );

        // Separate prices by market type
        let da_prices: Vec<_> = prices
            .iter()
            .filter(|p| p.market == MarketType::DayAhead)
            .cloned()
            .collect();
        
        let rt_prices: Vec<_> = prices
            .iter()
            .filter(|p| matches!(p.market, MarketType::RealTime5Min | MarketType::RealTime15Min))
            .cloned()
            .collect();

        // Calculate DA-only arbitrage
        if !da_prices.is_empty() {
            let da_windows = self.calculate_tbx_windows(&da_prices, MarketType::DayAhead);
            result.da_windows = da_windows.clone();
            result.revenue_da = da_windows.iter().map(|w| w.revenue).sum();
            result.avg_spread_da = self.calculate_avg_spread(&da_windows);
        }

        // Calculate RT-only arbitrage
        if !rt_prices.is_empty() {
            let rt_windows = self.calculate_tbx_windows(&rt_prices, MarketType::RealTime15Min);
            result.rt_windows = rt_windows.clone();
            result.revenue_rt = rt_windows.iter().map(|w| w.revenue).sum();
            result.avg_spread_rt = self.calculate_avg_spread(&rt_windows);
        }

        // Calculate utilization and cycles
        result.utilization_factor = self.calculate_utilization(&result);
        result.cycles_per_day = result.utilization_factor;

        result
    }

    /// Core TBX algorithm: find top X and bottom X hours for arbitrage
    fn calculate_tbx_windows(&self, prices: &[PriceData], market_type: MarketType) -> Vec<ArbitrageWindow> {
        let mut windows = Vec::new();

        // Group prices by hour for DA, or by appropriate interval for RT
        let interval_prices = self.group_prices_by_interval(prices, market_type);
        
        // For each 24-hour period, find arbitrage opportunities
        let daily_groups = self.group_by_day(&interval_prices);

        for (_date, day_prices) in daily_groups {
            if day_prices.len() < 2 {
                continue;
            }

            // Sort prices to find cheapest and most expensive periods
            let mut sorted_prices = day_prices.clone();
            sorted_prices.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let num_intervals = self.config.duration_hours as usize * self.intervals_per_hour(market_type);
            
            if sorted_prices.len() < num_intervals * 2 {
                continue; // Not enough intervals for a full cycle
            }

            // Get bottom X intervals (for charging)
            let charge_intervals = &sorted_prices[..num_intervals];
            let avg_charge_price: f64 = charge_intervals.iter().map(|(_, p)| p).sum::<f64>() / num_intervals as f64;

            // Get top X intervals (for discharging)
            let discharge_intervals = &sorted_prices[sorted_prices.len() - num_intervals..];
            let avg_discharge_price: f64 = discharge_intervals.iter().map(|(_, p)| p).sum::<f64>() / num_intervals as f64;

            // Check if spread meets threshold
            let spread = avg_discharge_price - avg_charge_price;
            if spread < self.config.min_spread_threshold {
                continue;
            }

            // Calculate revenue considering efficiency
            let one_way_efficiency = self.config.one_way_efficiency();
            let energy_per_interval = self.config.battery_power_mw / self.intervals_per_hour(market_type) as f64;
            let total_energy = energy_per_interval * num_intervals as f64;
            
            let revenue = total_energy * spread * self.config.round_trip_efficiency;

            // Create arbitrage window
            let charge_start = charge_intervals[0].0;
            let charge_end = self.add_duration(charge_intervals.last().unwrap().0, market_type);
            let discharge_start = discharge_intervals[0].0;
            let discharge_end = self.add_duration(discharge_intervals.last().unwrap().0, market_type);

            windows.push(ArbitrageWindow {
                charge_start,
                charge_end,
                charge_price: avg_charge_price,
                discharge_start,
                discharge_end,
                discharge_price: avg_discharge_price,
                energy_mwh: total_energy,
                revenue,
            });
        }

        windows
    }

    /// Group prices by appropriate interval based on market type
    fn group_prices_by_interval(
        &self,
        prices: &[PriceData],
        market_type: MarketType,
    ) -> Vec<(DateTime<Utc>, f64)> {
        prices
            .iter()
            .map(|p| (p.timestamp, p.price))
            .collect()
    }

    /// Group interval prices by day
    fn group_by_day(
        &self,
        interval_prices: &[(DateTime<Utc>, f64)],
    ) -> HashMap<NaiveDate, Vec<(DateTime<Utc>, f64)>> {
        let mut daily_groups = HashMap::new();

        for (timestamp, price) in interval_prices {
            let date = timestamp.date_naive();
            daily_groups
                .entry(date)
                .or_insert_with(Vec::new)
                .push((*timestamp, *price));
        }

        daily_groups
    }

    /// Get number of intervals per hour based on market type
    fn intervals_per_hour(&self, market_type: MarketType) -> usize {
        match market_type {
            MarketType::DayAhead => 1,
            MarketType::RealTime5Min => 12,
            MarketType::RealTime15Min => 4,
        }
    }

    /// Add appropriate duration based on market type
    fn add_duration(&self, timestamp: DateTime<Utc>, market_type: MarketType) -> DateTime<Utc> {
        match market_type {
            MarketType::DayAhead => timestamp + Duration::hours(1),
            MarketType::RealTime5Min => timestamp + Duration::minutes(5),
            MarketType::RealTime15Min => timestamp + Duration::minutes(15),
        }
    }

    /// Calculate average spread from arbitrage windows
    fn calculate_avg_spread(&self, windows: &[ArbitrageWindow]) -> f64 {
        if windows.is_empty() {
            return 0.0;
        }

        let total_spread: f64 = windows
            .iter()
            .map(|w| (w.discharge_price - w.charge_price) * w.energy_mwh)
            .sum();
        
        let total_energy: f64 = windows.iter().map(|w| w.energy_mwh).sum();
        
        if total_energy > 0.0 {
            total_spread / total_energy
        } else {
            0.0
        }
    }

    /// Calculate battery utilization factor
    fn calculate_utilization(&self, result: &TbxResult) -> f64 {
        let max_daily_energy = self.config.battery_capacity_mwh;
        
        let actual_energy = result
            .blended_windows
            .iter()
            .chain(result.da_windows.iter())
            .chain(result.rt_windows.iter())
            .map(|w| w.energy_mwh)
            .fold(0.0, |a, b| a.max(b));

        if max_daily_energy > 0.0 {
            actual_energy / max_daily_energy
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tb2_calculation() {
        let config = TbxConfig::new_tb2(100.0);
        let calculator = TbxCalculator::new(config);

        // Create sample price data
        let mut prices = vec![];
        let base_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Create 24 hours of prices with clear arbitrage opportunity
        for hour in 0..24 {
            let price = if hour < 6 || hour > 20 {
                20.0 // Low price (night)
            } else if hour >= 18 && hour <= 20 {
                100.0 // High price (evening peak)
            } else {
                50.0 // Medium price (day)
            };

            prices.push(PriceData {
                timestamp: base_time + Duration::hours(hour),
                settlement_point: "TEST_NODE".to_string(),
                price,
                market: MarketType::DayAhead,
            });
        }

        let result = calculator.calculate_daily_arbitrage(
            &prices,
            "TEST_BATTERY",
            "TEST_NODE",
            base_time.date_naive(),
        );

        assert!(result.revenue_da > 0.0);
        assert!(!result.da_windows.is_empty());
        assert!(result.avg_spread_da > 50.0); // Should find the 100-20 spread
    }
}