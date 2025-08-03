use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TbxConfig {
    pub duration_hours: u8, // 1, 2, or 4
    pub battery_power_mw: f64,
    pub battery_capacity_mwh: f64,
    pub round_trip_efficiency: f64,
    pub min_spread_threshold: f64, // Minimum $/MWh spread to arbitrage
}

impl TbxConfig {
    pub fn new_tb1(power_mw: f64) -> Self {
        Self {
            duration_hours: 1,
            battery_power_mw: power_mw,
            battery_capacity_mwh: power_mw * 1.0,
            round_trip_efficiency: 0.85,
            min_spread_threshold: 5.0,
        }
    }

    pub fn new_tb2(power_mw: f64) -> Self {
        Self {
            duration_hours: 2,
            battery_power_mw: power_mw,
            battery_capacity_mwh: power_mw * 2.0,
            round_trip_efficiency: 0.85,
            min_spread_threshold: 5.0,
        }
    }

    pub fn new_tb4(power_mw: f64) -> Self {
        Self {
            duration_hours: 4,
            battery_power_mw: power_mw,
            battery_capacity_mwh: power_mw * 4.0,
            round_trip_efficiency: 0.85,
            min_spread_threshold: 5.0,
        }
    }

    pub fn one_way_efficiency(&self) -> f64 {
        self.round_trip_efficiency.sqrt()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceData {
    pub timestamp: DateTime<Utc>,
    pub settlement_point: String,
    pub price: f64,
    pub market: MarketType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum MarketType {
    DayAhead,
    RealTime5Min,
    RealTime15Min,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageWindow {
    pub charge_start: DateTime<Utc>,
    pub charge_end: DateTime<Utc>,
    pub charge_price: f64,
    pub discharge_start: DateTime<Utc>,
    pub discharge_end: DateTime<Utc>,
    pub discharge_price: f64,
    pub energy_mwh: f64,
    pub revenue: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TbxResult {
    pub resource_name: String,
    pub settlement_point: String,
    pub date: NaiveDate,
    pub config: TbxConfig,
    
    // Revenue by market
    pub revenue_da: f64,
    pub revenue_rt: f64,
    pub revenue_blended: f64,
    
    // Arbitrage windows
    pub da_windows: Vec<ArbitrageWindow>,
    pub rt_windows: Vec<ArbitrageWindow>,
    pub blended_windows: Vec<ArbitrageWindow>,
    
    // Statistics
    pub avg_spread_da: f64,
    pub avg_spread_rt: f64,
    pub avg_spread_blended: f64,
    pub utilization_factor: f64,
    pub cycles_per_day: f64,
}

impl TbxResult {
    pub fn new(resource_name: String, settlement_point: String, date: NaiveDate, config: TbxConfig) -> Self {
        Self {
            resource_name,
            settlement_point,
            date,
            config,
            revenue_da: 0.0,
            revenue_rt: 0.0,
            revenue_blended: 0.0,
            da_windows: vec![],
            rt_windows: vec![],
            blended_windows: vec![],
            avg_spread_da: 0.0,
            avg_spread_rt: 0.0,
            avg_spread_blended: 0.0,
            utilization_factor: 0.0,
            cycles_per_day: 0.0,
        }
    }

    pub fn best_revenue(&self) -> f64 {
        self.revenue_da.max(self.revenue_rt).max(self.revenue_blended)
    }

    pub fn best_strategy(&self) -> &str {
        if self.revenue_blended >= self.revenue_da && self.revenue_blended >= self.revenue_rt {
            "Blended"
        } else if self.revenue_rt >= self.revenue_da {
            "RealTime"
        } else {
            "DayAhead"
        }
    }
}