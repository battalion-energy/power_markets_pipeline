# TBX (Top-Bottom X Hours) Energy Arbitrage Calculator Design

## Overview
TBX calculation identifies the top X most expensive hours and bottom X cheapest hours within each 24-hour period to calculate potential energy arbitrage revenue for battery energy storage systems.

## Key Concepts

### 1. TBX Variants
- **TB1**: Top-Bottom 1 hour (1-hour battery)
- **TB2**: Top-Bottom 2 hours (2-hour battery)
- **TB4**: Top-Bottom 4 hours (4-hour battery)

### 2. Price Sources
- **Day-Ahead (DA)**: Hourly prices from DAM
- **Real-Time (RT)**: 5-minute or 15-minute prices from RTM
- **Blended**: Optimal mix of DA and RT based on price signals

### 3. Settlement Point Mapping
Each generator/battery maps to a specific settlement point for price lookup.

## Algorithm Design

### Basic TBX Calculation (Single Market)
```rust
pub struct TbxCalculator {
    duration_hours: u8, // 1, 2, or 4
    battery_power_mw: f64,
    battery_efficiency: f64, // Round-trip efficiency
}

impl TbxCalculator {
    pub fn calculate_daily_arbitrage(&self, prices: &[f64]) -> ArbitrageResult {
        // 1. Find top X most expensive hours
        // 2. Find bottom X cheapest hours
        // 3. Calculate revenue: (avg_high - avg_low) * power * efficiency * duration
        // 4. Return results with charge/discharge schedule
    }
}
```

### Blended DA+RT Algorithm
For real-time optimization with 15-minute intervals:

1. **Identify High-Value Periods**:
   - Check both DA hourly prices and RT 15-minute prices
   - If RT has short spikes (e.g., 15-30 min), capture those
   - Fill remaining capacity with DA opportunities

2. **Dispatch Logic**:
   ```
   For each 15-minute interval:
   - If RT price > threshold AND battery has capacity:
     → Discharge at RT price
   - Else if DA price for hour > threshold AND battery has capacity:
     → Discharge at DA price
   - For charging: similar logic but for low prices
   ```

3. **Constraints**:
   - Total discharge ≤ battery_power_mw * duration_hours
   - Cannot charge and discharge simultaneously
   - Respect round-trip efficiency losses

## Data Requirements

### Input Files
1. **Price Data** (Parquet/Arrow):
   - DA: Settlement Point, Hour, Price
   - RT: Settlement Point, Interval, Price

2. **Generator Mapping** (CSV/Parquet):
   - Generator Name → Settlement Point
   - Battery specifications (MW, MWh)

### Output Format
```rust
pub struct TbxResult {
    pub resource_name: String,
    pub date: NaiveDate,
    pub revenue_da: f64,
    pub revenue_rt: f64,
    pub revenue_blended: f64,
    pub charge_hours: Vec<(DateTime, f64)>, // (time, MW)
    pub discharge_hours: Vec<(DateTime, f64)>, // (time, MW)
    pub avg_spread: f64,
    pub utilization_factor: f64,
}
```

## Performance Considerations

### Parquet vs Arrow
- **Parquet**: Better compression, good for storage
- **Arrow**: Zero-copy reads, better for in-memory processing
- Recommendation: Use Arrow for computation, Parquet for storage

### Optimization Strategies
1. Pre-sort prices by settlement point
2. Use parallel processing for multiple resources
3. Cache daily price statistics
4. Vectorized operations for price sorting

## Implementation Plan

### Phase 1: Core TBX Engine
- Basic TB1, TB2, TB4 calculations
- DA-only implementation
- Single settlement point

### Phase 2: Real-Time Integration
- RT price handling (5-min and 15-min)
- Blended DA+RT optimization
- Multiple interval support

### Phase 3: Full System
- Settlement point mapping
- Batch processing for all generators
- Performance optimization
- Results aggregation

## Example Calculation

### TB2 for 100MW/200MWh Battery
```
Day: 2024-01-15
DA Prices at SETTLEMENT_POINT_X:
- Hour 00: $20/MWh
- Hour 01: $18/MWh
- ...
- Hour 19: $95/MWh (peak)
- Hour 20: $88/MWh (peak)

Bottom 2 hours: 01:00 ($18), 02:00 ($19) → Avg: $18.50
Top 2 hours: 19:00 ($95), 20:00 ($88) → Avg: $91.50

Spread: $91.50 - $18.50 = $73/MWh
Revenue: $73 * 100MW * 2h * 0.85 efficiency = $12,410/day
```

## Testing Strategy
1. Unit tests for price sorting algorithms
2. Integration tests with sample price data
3. Performance benchmarks for large datasets
4. Validation against known BESS revenues