use crate::models::{ArbitrageWindow, MarketType, PriceData, TbxConfig};
use chrono::{DateTime, Duration, Timelike, Utc};
use std::collections::BTreeMap;

/// Interval representation for optimization
#[derive(Debug, Clone)]
struct Interval {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    price: f64,
    market: MarketType,
    available_mw: f64,
}

/// Battery state at a point in time
#[derive(Debug, Clone)]
struct BatteryState {
    soc_mwh: f64,
    power_mw: f64, // positive = discharge, negative = charge
}

pub struct BlendedOptimizer {
    config: TbxConfig,
}

impl BlendedOptimizer {
    pub fn new(config: TbxConfig) -> Self {
        Self { config }
    }

    /// Optimize battery dispatch across DA and RT markets
    pub fn optimize_blended(
        &self,
        da_prices: &[PriceData],
        rt_prices: &[PriceData],
    ) -> Vec<ArbitrageWindow> {
        // Convert to unified interval representation
        let mut intervals = self.create_intervals(da_prices, rt_prices);
        
        // Sort by timestamp
        intervals.sort_by_key(|i| i.start);

        // Find optimal dispatch using dynamic programming
        let dispatch_plan = self.optimize_dispatch(&intervals);

        // Convert dispatch plan to arbitrage windows
        self.create_arbitrage_windows(dispatch_plan)
    }

    /// Create unified interval representation from DA and RT prices
    fn create_intervals(&self, da_prices: &[PriceData], rt_prices: &[PriceData]) -> Vec<Interval> {
        let mut intervals = Vec::new();

        // Process DA prices (hourly intervals)
        for price in da_prices {
            intervals.push(Interval {
                start: price.timestamp,
                end: price.timestamp + Duration::hours(1),
                price: price.price,
                market: MarketType::DayAhead,
                available_mw: self.config.battery_power_mw,
            });
        }

        // Process RT prices (15-min intervals)
        // Group by hour to check for spikes
        let mut rt_by_hour: BTreeMap<DateTime<Utc>, Vec<&PriceData>> = BTreeMap::new();
        for price in rt_prices {
            let hour_start = price
                .timestamp
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap();
            rt_by_hour.entry(hour_start).or_default().push(price);
        }

        // Check each hour for RT price spikes
        for (hour, hour_prices) in rt_by_hour {
            // Find DA price for this hour
            let da_price = da_prices
                .iter()
                .find(|p| p.timestamp == hour)
                .map(|p| p.price)
                .unwrap_or(50.0); // Default if not found

            // Check if any RT interval significantly exceeds DA
            for rt_price in hour_prices {
                let premium = rt_price.price - da_price;
                
                // If RT price is significantly higher, create a high-priority interval
                if premium > 10.0 {
                    // Find existing DA interval and reduce its available MW
                    if let Some(da_interval) = intervals.iter_mut().find(|i| {
                        i.market == MarketType::DayAhead
                            && i.start == hour
                    }) {
                        da_interval.available_mw -= self.config.battery_power_mw / 4.0;
                    }

                    intervals.push(Interval {
                        start: rt_price.timestamp,
                        end: rt_price.timestamp + Duration::minutes(15),
                        price: rt_price.price,
                        market: MarketType::RealTime15Min,
                        available_mw: self.config.battery_power_mw,
                    });
                }
            }
        }

        intervals
    }

    /// Optimize dispatch using a greedy algorithm with lookahead
    fn optimize_dispatch(&self, intervals: &[Interval]) -> Vec<(Interval, f64)> {
        let mut dispatch_plan = Vec::new();
        let mut battery_soc = self.config.battery_capacity_mwh * 0.5; // Start at 50% SOC
        
        // Find daily price patterns
        let daily_stats = self.calculate_daily_stats(intervals);
        
        for (idx, interval) in intervals.iter().enumerate() {
            let hours_remaining = 24.0 - interval.start.hour() as f64;
            let current_stats = &daily_stats[&interval.start.date_naive()];
            
            // Decide whether to charge, discharge, or hold
            let action = self.decide_action(
                interval,
                battery_soc,
                current_stats,
                hours_remaining,
            );
            
            if action != 0.0 {
                // Update SOC
                let energy_mwh = action.abs() * self.interval_duration_hours(interval);
                if action > 0.0 {
                    // Discharging
                    battery_soc -= energy_mwh / self.config.one_way_efficiency();
                } else {
                    // Charging
                    battery_soc += energy_mwh * self.config.one_way_efficiency();
                }
                
                // Ensure SOC stays within bounds
                battery_soc = battery_soc.clamp(0.0, self.config.battery_capacity_mwh);
                
                dispatch_plan.push((interval.clone(), action));
            }
        }
        
        dispatch_plan
    }

    /// Calculate daily price statistics
    fn calculate_daily_stats(&self, intervals: &[Interval]) -> BTreeMap<chrono::NaiveDate, DailyStats> {
        let mut stats_map = BTreeMap::new();
        
        for interval in intervals {
            let date = interval.start.date_naive();
            let stats = stats_map.entry(date).or_insert_with(|| DailyStats {
                prices: Vec::new(),
                avg_price: 0.0,
                p10_price: 0.0,
                p90_price: 0.0,
            });
            stats.prices.push(interval.price);
        }
        
        // Calculate percentiles
        for stats in stats_map.values_mut() {
            stats.prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let len = stats.prices.len();
            stats.avg_price = stats.prices.iter().sum::<f64>() / len as f64;
            stats.p10_price = stats.prices[len / 10];
            stats.p90_price = stats.prices[len * 9 / 10];
        }
        
        stats_map
    }

    /// Decide whether to charge, discharge, or hold
    fn decide_action(
        &self,
        interval: &Interval,
        current_soc: f64,
        daily_stats: &DailyStats,
        hours_remaining: f64,
    ) -> f64 {
        let soc_percent = current_soc / self.config.battery_capacity_mwh;
        
        // High price and sufficient SOC -> discharge
        if interval.price > daily_stats.p90_price && soc_percent > 0.2 {
            return interval.available_mw.min(self.config.battery_power_mw);
        }
        
        // Low price and room to charge -> charge
        if interval.price < daily_stats.p10_price && soc_percent < 0.8 {
            return -interval.available_mw.min(self.config.battery_power_mw);
        }
        
        // RT spike -> prioritize discharge
        if interval.market == MarketType::RealTime15Min 
            && interval.price > daily_stats.avg_price * 1.5 
            && soc_percent > 0.1 {
            return interval.available_mw.min(self.config.battery_power_mw);
        }
        
        0.0 // Hold
    }

    /// Get interval duration in hours
    fn interval_duration_hours(&self, interval: &Interval) -> f64 {
        match interval.market {
            MarketType::DayAhead => 1.0,
            MarketType::RealTime5Min => 1.0 / 12.0,
            MarketType::RealTime15Min => 0.25,
        }
    }

    /// Convert dispatch plan to arbitrage windows
    fn create_arbitrage_windows(&self, dispatch_plan: Vec<(Interval, f64)>) -> Vec<ArbitrageWindow> {
        let mut windows = Vec::new();
        let mut current_charge: Option<ChargeWindow> = None;
        let mut current_discharge: Option<DischargeWindow> = None;
        
        for (interval, power) in dispatch_plan {
            if power < 0.0 {
                // Charging
                match &mut current_charge {
                    Some(charge) => {
                        charge.end = interval.end;
                        charge.total_energy += power.abs() * self.interval_duration_hours(&interval);
                        charge.total_cost += interval.price * power.abs() * self.interval_duration_hours(&interval);
                    }
                    None => {
                        current_charge = Some(ChargeWindow {
                            start: interval.start,
                            end: interval.end,
                            total_energy: power.abs() * self.interval_duration_hours(&interval),
                            total_cost: interval.price * power.abs() * self.interval_duration_hours(&interval),
                        });
                    }
                }
            } else if power > 0.0 {
                // Discharging
                match &mut current_discharge {
                    Some(discharge) => {
                        discharge.end = interval.end;
                        discharge.total_energy += power * self.interval_duration_hours(&interval);
                        discharge.total_revenue += interval.price * power * self.interval_duration_hours(&interval);
                    }
                    None => {
                        current_discharge = Some(DischargeWindow {
                            start: interval.start,
                            end: interval.end,
                            total_energy: power * self.interval_duration_hours(&interval),
                            total_revenue: interval.price * power * self.interval_duration_hours(&interval),
                        });
                    }
                }
            }
            
            // Check if we have a complete cycle
            if let (Some(charge), Some(discharge)) = (&current_charge, &current_discharge) {
                let energy = charge.total_energy.min(discharge.total_energy);
                let avg_charge_price = charge.total_cost / charge.total_energy;
                let avg_discharge_price = discharge.total_revenue / discharge.total_energy;
                let revenue = energy * (avg_discharge_price - avg_charge_price) * self.config.round_trip_efficiency;
                
                windows.push(ArbitrageWindow {
                    charge_start: charge.start,
                    charge_end: charge.end,
                    charge_price: avg_charge_price,
                    discharge_start: discharge.start,
                    discharge_end: discharge.end,
                    discharge_price: avg_discharge_price,
                    energy_mwh: energy,
                    revenue,
                });
                
                current_charge = None;
                current_discharge = None;
            }
        }
        
        windows
    }
}

#[derive(Debug)]
struct DailyStats {
    prices: Vec<f64>,
    avg_price: f64,
    p10_price: f64,
    p90_price: f64,
}

#[derive(Debug)]
struct ChargeWindow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    total_energy: f64,
    total_cost: f64,
}

#[derive(Debug)]
struct DischargeWindow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    total_energy: f64,
    total_revenue: f64,
}