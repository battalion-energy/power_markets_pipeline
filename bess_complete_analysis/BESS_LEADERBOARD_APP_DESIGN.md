# BESS Leaderboard Next.js Application Design

## Overview
Design document for a comprehensive BESS (Battery Energy Storage System) performance analytics and leaderboard application, inspired by Modo Energy's approach but with enhanced features for ERCOT market analysis.

## Core Features

### 1. Interactive Leaderboard Dashboard

#### Main Leaderboard View
```typescript
interface BessPerformance {
  resourceName: string;
  year: number;
  month?: number;
  totalRevenue: number;
  revenuePerMW: number;
  revenuePerMWh: number;
  energyRevenue: number;
  asRevenue: number;
  capacityFactor: number;
  cyclesPerDay: number;
  performanceRank: number;
  percentileRank: number;
}
```

**Key Metrics to Display:**
- **Revenue Rankings**: Sort by total revenue, $/MW, $/MWh
- **Performance Percentiles**: Show where each BESS falls (top 10%, median, bottom quartile)
- **Revenue Mix**: Pie chart showing energy vs AS revenue split
- **Trending**: Up/down arrows showing month-over-month changes

#### Interactive Features:
- **Time Range Selector**: View by year, quarter, month, or custom range
- **Filter Controls**: 
  - By capacity (MW)
  - By duration (hours)
  - By operator/owner
  - By region/zone
  - By commissioning date
- **Comparison Mode**: Select 2-5 BESS for side-by-side comparison
- **Export**: Download data as CSV/Excel

### 2. Nodal Analysis & Location Impact

#### Nodal Revenue Analysis
```typescript
interface NodalAnalysis {
  resourceName: string;
  settlementPoint: string;
  nodeType: 'Resource' | 'Hub' | 'LoadZone';
  avgNodalPrice: number;
  hubPriceDelta: number; // Difference from hub price
  systemLambdaDelta: number; // Difference from system lambda
  congestionRevenue: number;
  locationValueScore: number; // 0-100 score
}
```

**Visualization Components:**
- **Heat Map**: Texas map showing revenue by location
- **Scatter Plot**: X-axis: Hub price delta, Y-axis: Total revenue
- **Box Plots**: Revenue distribution by zone/region
- **Time Series**: Nodal vs hub price tracking

#### Location Impact Metrics:
1. **Basis Risk Analysis**
   - Average basis (node - hub price)
   - Basis volatility
   - Correlation with hub prices

2. **Congestion Revenue**
   - Revenue from positive/negative congestion
   - Frequency of congestion events
   - Impact on dispatch decisions

3. **Location Value Score**
   - Composite metric considering:
     - Average price premium/discount
     - Price volatility (arbitrage opportunity)
     - Transmission constraints
     - Local renewable penetration

### 3. Bidding Strategy Analysis & Clustering

#### Strategy Identification
```typescript
interface BiddingPattern {
  resourceName: string;
  strategyCluster: string;
  primaryStrategy: 'EnergyArbitrage' | 'ASFocused' | 'Hybrid' | 'Opportunistic';
  characteristics: {
    avgDailyCharges: number;
    avgDailyDischarges: number;
    peakHourActivity: number; // % of dispatch during peak
    asParticipation: {
      regUp: number;
      regDown: number;
      rrs: number;
      ecrs: number;
      nonSpin: number;
    };
    bidAggressiveness: number; // 0-100 scale
  };
}
```

#### Clustering Analysis Features:
1. **K-Means Clustering** on bidding behaviors:
   - Charge/discharge timing patterns
   - AS participation rates
   - Price responsiveness
   - State of charge management

2. **Strategy Profiles**:
   - **"Peak Shaver"**: High activity during peak hours
   - **"AS Specialist"**: >70% revenue from AS
   - **"Arbitrage Master"**: Multiple daily cycles
   - **"Opportunist"**: Variable pattern based on prices
   - **"Conservative"**: Low utilization, risk-averse

3. **Performance by Strategy**:
   - Revenue comparison across strategies
   - Risk-adjusted returns
   - Seasonal strategy effectiveness

### 4. Advanced Analytics

#### Revenue Attribution Analysis
```typescript
interface RevenueAttribution {
  totalRevenue: number;
  breakdown: {
    energyArbitrage: {
      damEnergy: number;
      rtEnergyNet: number; // discharge revenue - charge cost
    };
    ancillaryServices: {
      regUp: number;
      regDown: number;
      rrs: number;
      ecrs: number;
      nonSpin: number;
    };
    nodalPremium: number; // Extra revenue from location
  };
}
```

#### Comparative Analytics:
1. **Peer Comparison**
   - Compare to similar duration BESS
   - Compare to same zone/region
   - Compare to same operator

2. **What-If Analysis**
   - "If this BESS was at hub, revenue would be..."
   - "With average bidding strategy, revenue would be..."
   - "At 90th percentile performance, revenue would be..."

3. **Efficiency Metrics**
   - Revenue per cycle
   - Round-trip efficiency impact
   - Degradation-adjusted returns

### 5. Time Series Visualizations

#### Interactive Charts:
1. **Revenue Stack Area Chart**
   - Stacked by revenue stream over time
   - Interactive tooltips with details
   - Zoom and pan capabilities

2. **State of Charge Heatmap**
   - 24x7 grid showing typical SOC patterns
   - Overlay with price data
   - Identify optimization opportunities

3. **Performance Evolution**
   - Show how BESS performance changes over time
   - Identify learning curves or degradation
   - Seasonal patterns

### 6. Database Schema

```sql
-- Core tables
CREATE TABLE bess_resources (
  id SERIAL PRIMARY KEY,
  resource_name VARCHAR(100) UNIQUE,
  settlement_point VARCHAR(100),
  capacity_mw DECIMAL(10,2),
  duration_hours DECIMAL(5,2),
  commissioning_date DATE,
  operator VARCHAR(100),
  zone VARCHAR(50)
);

CREATE TABLE bess_hourly_performance (
  id SERIAL PRIMARY KEY,
  resource_id INTEGER REFERENCES bess_resources(id),
  timestamp TIMESTAMP,
  dam_award_mw DECIMAL(10,2),
  dam_price DECIMAL(10,2),
  rt_dispatch_mw DECIMAL(10,2),
  rt_price DECIMAL(10,2),
  soc_pct DECIMAL(5,2),
  -- AS awards
  reg_up_mw DECIMAL(10,2),
  reg_down_mw DECIMAL(10,2),
  rrs_mw DECIMAL(10,2),
  ecrs_mw DECIMAL(10,2),
  non_spin_mw DECIMAL(10,2),
  -- AS prices
  reg_up_mcpc DECIMAL(10,2),
  reg_down_mcpc DECIMAL(10,2),
  rrs_mcpc DECIMAL(10,2),
  ecrs_mcpc DECIMAL(10,2),
  non_spin_mcpc DECIMAL(10,2)
);

CREATE TABLE nodal_prices (
  id SERIAL PRIMARY KEY,
  settlement_point VARCHAR(100),
  timestamp TIMESTAMP,
  lmp DECIMAL(10,2),
  energy_component DECIMAL(10,2),
  congestion_component DECIMAL(10,2),
  loss_component DECIMAL(10,2)
);

-- Materialized views for performance
CREATE MATERIALIZED VIEW bess_daily_summary AS
SELECT 
  resource_id,
  DATE(timestamp) as date,
  SUM(CASE WHEN rt_dispatch_mw > 0 THEN rt_dispatch_mw * rt_price / 12 ELSE 0 END) as rt_revenue,
  SUM(dam_award_mw * dam_price) as dam_revenue,
  SUM(reg_up_mw * reg_up_mcpc) as reg_up_revenue,
  -- ... other AS revenues
  COUNT(DISTINCT CASE WHEN rt_dispatch_mw > 10 THEN EXTRACT(HOUR FROM timestamp) END) as discharge_hours,
  AVG(soc_pct) as avg_soc
FROM bess_hourly_performance
GROUP BY resource_id, DATE(timestamp);
```

### 7. API Endpoints

```typescript
// Next.js API routes
GET /api/leaderboard
  ?period=2024-01
  &metric=revenue_per_mw
  &limit=50

GET /api/bess/:resourceName/performance
  ?startDate=2024-01-01
  &endDate=2024-12-31
  &granularity=daily

GET /api/bess/:resourceName/nodal-analysis
  ?compareToHub=true
  &includeSystemLambda=true

GET /api/strategies/clusters
  ?period=2024
  &minResources=5

POST /api/analytics/what-if
  {
    resourceName: "BESS_NAME",
    scenario: {
      location: "HB_NORTH",
      strategy: "average_performer"
    }
  }
```

### 8. Tech Stack

**Frontend:**
- Next.js 14 with App Router
- TypeScript
- Tailwind CSS
- Apache ECharts for all visualizations
- Tanstack Query for data fetching
- Zustand for state management

**Backend:**
- Next.js API routes
- Prisma ORM
- PostgreSQL with TimescaleDB
- Redis for caching
- Python microservices for ML clustering

**Infrastructure:**
- Vercel or AWS deployment
- GitHub Actions CI/CD
- Datadog monitoring

### 9. Key Differentiators

1. **Nodal Impact Quantification**: Clear visualization of how location affects revenue
2. **Strategy Clustering**: ML-driven identification of bidding patterns
3. **What-If Scenarios**: Interactive exploration of different strategies/locations
4. **Peer Benchmarking**: Context-aware comparisons
5. **Open Data Integration**: Combine with weather, load, renewable generation data

### 10. Revenue Impact Analysis Features

#### Location vs Strategy Matrix
```
                High Performing Strategy
                |
Hub Location    |  Scenario A: $150/kW-yr
                |  "Good strategy at average location"
                |
----------------|------------------
                |
Premium Node    |  Scenario B: $200/kW-yr
                |  "Good strategy at good location"
                |
                Low Performing Strategy
```

This allows users to see:
- How much is due to good location selection
- How much is due to good operational strategy
- The interaction effects between location and strategy

### Implementation Priority

1. **Phase 1**: Basic leaderboard with revenue rankings
2. **Phase 2**: Nodal analysis and location impact
3. **Phase 3**: Strategy clustering and pattern recognition
4. **Phase 4**: What-if analysis and advanced analytics
5. **Phase 5**: ML-driven insights and predictions