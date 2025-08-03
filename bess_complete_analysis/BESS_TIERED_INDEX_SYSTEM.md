# BESS Tiered Index System

## Overview
A comprehensive three-tier index system for tracking ERCOT battery storage performance, adapted for the current market size of ~50 operational BESS assets.

## Index Hierarchy

### BESS-ALL: The Market Index
**Purpose**: Complete market representation
**Target Audience**: Analysts, researchers, market observers

```typescript
interface BESSALL {
  composition: {
    constituents: "All eligible BESS"; // Currently ~50
    minCapacity: 5; // MW
    minOperatingHistory: 3; // months
    rebalancing: "quarterly";
  };
  
  characteristics: {
    marketCoverage: "95-100%"; // of total ERCOT BESS capacity
    diversification: "Maximum";
    stability: "Moderate";
    turnover: "5-10% quarterly";
  };
  
  useCase: [
    "Complete market performance",
    "Academic research",
    "Long-term trend analysis",
    "Regulatory reporting baseline"
  ];
}
```

### BESS-25: The Core Index
**Purpose**: Core market performance focusing on established assets
**Target Audience**: Investors, developers, operators

```typescript
interface BESS25 {
  composition: {
    constituents: 25;
    minCapacity: 10; // MW
    minOperatingHistory: 6; // months
    rebalancing: "quarterly";
    selectionCriteria: {
      capacity: 40,      // weight %
      revenue: 30,       // weight %
      operational: 20,   // weight %
      strategic: 10      // weight %
    };
  };
  
  characteristics: {
    marketCoverage: "75-85%"; // of total ERCOT BESS capacity
    diversification: "Moderate";
    stability: "High";
    turnover: "10-15% quarterly";
  };
  
  useCase: [
    "Investment benchmarking",
    "Performance contracts",
    "Financial products base",
    "Operator target-setting"
  ];
}
```

### BESS-10: The Elite Index
**Purpose**: Track top performers and market leaders
**Target Audience**: C-suite, investors, media

```typescript
interface BESS10 {
  composition: {
    constituents: 10;
    minCapacity: 50; // MW
    minOperatingHistory: 18; // months
    rebalancing: "monthly"; // More frequent due to smaller size
    selectionCriteria: {
      totalRevenue: 25,     // weight %
      revenuePerMW: 25,     // weight %
      consistency: 25,      // weight %
      innovation: 25        // weight %
    };
  };
  
  characteristics: {
    marketCoverage: "35-40%"; // of total ERCOT BESS capacity
    diversification: "Low";
    stability: "Low"; // More volatile
    turnover: "10-20% monthly";
  };
  
  useCase: [
    "Headlines and media",
    "Executive dashboards",
    "Best practices identification",
    "Technology leadership tracking"
  ];
}
```

## Market Reality Check

### Current ERCOT BESS Market Size
```typescript
interface MarketSnapshot2024 {
  totalOperationalBESS: 50; // Approximate count
  byCapacity: {
    "100MW+": 8,
    "50-100MW": 15,
    "20-50MW": 17,
    "10-20MW": 7,
    "<10MW": 3
  };
  totalCapacityMW: 3500; // Approximate
  marketGrowth: "15-20 new BESS annually";
}
```

### Index Evolution Strategy
```typescript
interface IndexEvolution {
  currentPhase: {
    year: 2024,
    indices: ["BESS-ALL (~50)", "BESS-25", "BESS-10"]
  };
  
  futurePhases: [
    {
      year: 2026,
      marketSize: 75,
      indices: ["BESS-50", "BESS-25", "BESS-10"]
    },
    {
      year: 2028,
      marketSize: 100,
      indices: ["BESS-100", "BESS-50", "BESS-10"]
    }
  ];
}
```

## Detailed Selection Methodology

### BESS-ALL Selection Process

```python
def select_bess_all(all_bess_data):
    # Step 1: Basic eligibility (very inclusive)
    eligible = all_bess_data[
        (all_bess_data.capacity_mw >= 5) &
        (all_bess_data.months_operating >= 3) &
        (all_bess_data.data_quality_score >= 0.7)
    ]
    
    # Step 2: Calculate weight for index
    # Capacity-weighted but with caps to prevent domination
    eligible['index_weight'] = eligible.capacity_mw.clip(upper=200)
    eligible['index_weight'] = (
        eligible['index_weight'] / eligible['index_weight'].sum()
    )
    
    # Step 3: Include all eligible (currently ~50)
    selected = eligible
    
    # Step 4: Flag any data quality issues
    flag_low_quality_constituents(selected)
    
    return selected
```

### BESS-25 Selection Process

```python
def select_bess_25(all_bess_data):
    # Criteria for top half of market
    eligible = all_bess_data[
        (all_bess_data.capacity_mw >= 10) &
        (all_bess_data.months_operating >= 6) &
        (all_bess_data.data_quality >= 0.9)  # High quality data only
    ]
    
    # Multi-factor scoring
    eligible['capacity_score'] = normalize(eligible.capacity_mw) * 40
    eligible['revenue_score'] = normalize(eligible.revenue_per_mw_yr) * 30
    eligible['operational_score'] = (
        normalize(eligible.capacity_factor) * 10 +
        normalize(eligible.round_trip_efficiency) * 10
    ) # Total 20%
    eligible['strategic_score'] = (
        normalize(eligible.as_revenue_pct) * 5 +
        normalize(eligible.location_value) * 5
    ) # Total 10%
    
    eligible['total_score'] = (
        eligible.capacity_score + 
        eligible.revenue_score + 
        eligible.operational_score + 
        eligible.strategic_score
    )
    
    return eligible.nlargest(25, 'total_score')
```

### BESS-10 Selection Process

```python
def select_bess_10(all_bess_data):
    # Elite criteria
    eligible = all_bess_data[
        (all_bess_data.capacity_mw >= 50) &
        (all_bess_data.months_operating >= 18) &
        (all_bess_data.revenue_per_mw_yr > percentile(all_bess, 75))
    ]
    
    # Equal weight scoring for elite
    eligible['total_revenue_score'] = normalize(eligible.total_revenue) * 25
    eligible['revenue_intensity_score'] = normalize(eligible.revenue_per_mw_yr) * 25
    eligible['consistency_score'] = (
        (1 - eligible.revenue_volatility) * 12.5 +
        eligible.availability * 12.5
    ) # Total 25%
    eligible['innovation_score'] = calculate_innovation_score(eligible) * 25
    
    eligible['elite_score'] = (
        eligible.total_revenue_score + 
        eligible.revenue_intensity_score + 
        eligible.consistency_score + 
        eligible.innovation_score
    )
    
    # Manual review for final selection
    top_15 = eligible.nlargest(15, 'elite_score')
    return manual_review_for_final_10(top_15)
```

## Innovation Scoring for BESS-10

```typescript
interface InnovationScore {
  components: {
    strategyDiversity: number;    // Revenue from multiple sources
    cyclingIntensity: number;     // Above-average utilization
    gridServices: number;         // Participation in new markets
    technology: number;           // Advanced features/capabilities
  };
  
  calculation: {
    // Example: First to participate in ECRS
    marketPioneer: boolean;
    
    // Example: Achieving 3+ cycles/day consistently
    operationalExcellence: boolean;
    
    // Example: Co-located with renewable generation
    hybridConfiguration: boolean;
    
    // Example: Using AI/ML for optimization
    advancedControls: boolean;
  };
}
```

## Index Calculation Examples

### Daily Index Values

```typescript
// BESS-ALL: Complete market
const calculateBESSALL = (constituents: BESS[]) => {
  // Cap individual weights to prevent single-asset domination
  const cappedCapacities = constituents.map(bess => ({
    ...bess,
    cappedCapacity: Math.min(bess.capacityMW, 200)
  }));
  
  const totalWeightedRevenue = cappedCapacities.reduce((sum, bess) => 
    sum + (bess.dailyRevenue * bess.cappedCapacity), 0
  );
  const totalCappedCapacity = cappedCapacities.reduce((sum, bess) => 
    sum + bess.cappedCapacity, 0
  );
  
  return {
    value: totalWeightedRevenue / totalCappedCapacity,
    unit: "$/MW-day",
    change: calculateDailyChange(),
    constituents: constituents.length,
    marketCoverage: "95%+"
  };
};

// BESS-25: Core market
const calculateBESS25 = (constituents: BESS[]) => {
  // Similar but with quality adjustment
  const qualityAdjustedRevenue = constituents.reduce((sum, bess) => 
    sum + (bess.dailyRevenue * bess.capacityMW * bess.qualityFactor), 0
  );
  const qualityAdjustedCapacity = constituents.reduce((sum, bess) => 
    sum + (bess.capacityMW * bess.qualityFactor), 0
  );
  
  return {
    value: qualityAdjustedRevenue / qualityAdjustedCapacity,
    unit: "$/MW-day",
    premium: calculatePremiumToBESS100()
  };
};

// BESS-10: Elite performers
const calculateBESS10 = (constituents: BESS[]) => {
  // Simple average for elite group
  const avgRevenue = constituents.reduce((sum, bess) => 
    sum + bess.revenuePerMW, 0
  ) / constituents.length;
  
  return {
    value: avgRevenue,
    unit: "$/MW-day",
    multiple: avgRevenue / bess100Value, // "BESS-10 trades at 1.8x BESS-100"
    leaders: constituents.map(b => b.name).slice(0, 3) // Top 3 names
  };
};
```

## Index Relationships and Analysis

### Typical Relationships
```
BESS-10 / BESS-ALL Ratio: 1.4x - 2.2x (indicates elite premium)
BESS-25 / BESS-ALL Ratio: 1.1x - 1.25x (indicates core outperformance)
BESS-10 / BESS-25 Ratio:  1.2x - 1.7x (indicates elite vs core spread)

When ratios are:
- Below typical: Market convergence, commoditization
- Above typical: Skill differentiation, market inefficiency
- Diverging: Increasing value of expertise
- Converging: Market maturation
```

### Index Divergence Analysis

```typescript
interface IndexDivergence {
  // Bull market for BESS (all indices rising)
  bullSignal: {
    bess10: "+15% MoM",
    bess50: "+10% MoM", 
    bess100: "+7% MoM",
    interpretation: "Rising tide lifts all boats, led by elite"
  };
  
  // Bear market (all indices falling)
  bearSignal: {
    bess10: "-12% MoM",
    bess50: "-8% MoM",
    bess100: "-5% MoM", 
    interpretation: "Elite feel pain first, flight to quality"
  };
  
  // Skill market (divergence)
  skillSignal: {
    bess10: "+8% MoM",
    bess50: "+2% MoM",
    bess100: "-3% MoM",
    interpretation: "Operational excellence being rewarded"
  };
}
```

## Implementation Timeline

### Phase 1: BESS-ALL Launch (Month 1-2)
- Include all ~50 operational BESS
- Historical backfill to 2022
- Daily calculation infrastructure
- Basic website/API

### Phase 2: BESS-25 Addition (Month 3-4)
- Selection committee formation
- Enhanced criteria development
- Financial product readiness

### Phase 3: BESS-10 Elite (Month 5-6)
- Media partnership development
- Monthly rebalancing system
- Innovation scoring system

## Index Governance Structure

### Index Committee Composition
```typescript
interface IndexCommittee {
  members: {
    independent: 3,      // Academic/research
    operators: 2,        // BESS operators (rotating)
    developers: 2,       // Project developers
    financial: 2,        // Trading/finance
    ercot: 1,           // Market operator liaison
    total: 10
  };
  
  responsibilities: [
    "Methodology review (annual)",
    "Constituent changes (per rebalancing)",
    "Special circumstances (force majeure)",
    "Innovation scoring criteria",
    "Index integrity protection"
  ];
  
  meetings: {
    regular: "Monthly",
    rebalancing: "Day before each rebalance",
    emergency: "As needed"
  };
}
```

## Performance Reporting Format

### Daily Index Report
```
ERCOT BESS Index Report - [DATE]
================================

BESS-ALL: $342.50/MW-day (+2.3% DoD, +8.5% MoM)
BESS-25:  $389.25/MW-day (+2.8% DoD, +10.2% MoM)  
BESS-10:  $612.80/MW-day (+4.1% DoD, +15.6% MoM)

Ratios:
- BESS-10/ALL: 1.79x (↑ from 1.75x)
- BESS-25/ALL: 1.14x (↑ from 1.13x)
- BESS-10/25:  1.57x (↑ from 1.55x)

Market Stats:
- Total BESS Count: 52
- New This Quarter: 3
- Average Capacity: 67 MW

Top Movers (BESS-10):
1. EXAMPLE_BESS1: +12.3% (New AS strategy)
2. EXAMPLE_BESS2: +8.7%  (Location arbitrage)
3. EXAMPLE_BESS3: -4.2%  (Maintenance outage)

New Additions:
- BESS-25: NEW_BATTERY_1 replacing UNDERPERFORMER_X
- BESS-ALL: 2 new additions (NEW_PROJECT_A, NEW_PROJECT_B)
```

## Use Cases by Stakeholder

### For Investors
- **BESS-ALL**: Complete market exposure
- **BESS-25**: Core portfolio benchmark  
- **BESS-10**: Alpha generation ideas

### For Operators
- **BESS-ALL**: Full peer universe
- **BESS-25**: Realistic performance targets
- **BESS-10**: Best practices study

### For Developers
- **BESS-ALL**: Total market sizing
- **BESS-25**: Bankable revenue cases
- **BESS-10**: Technology roadmap

### For Policymakers
- **BESS-ALL**: Market health indicator
- **BESS-25**: Core market efficiency
- **BESS-10**: Innovation tracking

### For New Entrants
- **BESS-ALL**: See where you fit
- **BESS-25**: Target performance level
- **BESS-10**: Aspiration goals

## Index Calculation Code Structure

```rust
pub struct TieredIndexSystem {
    bess_all: IndexCalculator,
    bess_25: IndexCalculator,
    bess_10: IndexCalculator,
    market_size: usize,
}

impl TieredIndexSystem {
    pub fn calculate_all(&self, market_data: &MarketData) -> IndexResults {
        // Dynamically adjust for market size
        let all_eligible = market_data.get_eligible_bess();
        let market_size = all_eligible.len();
        
        let bess_all_value = self.bess_all.calculate(market_data);
        let bess_25_value = if market_size >= 35 {
            self.bess_25.calculate(market_data)
        } else {
            // Fallback to top 50% if market too small
            self.calculate_top_half(market_data)
        };
        let bess_10_value = self.bess_10.calculate(market_data);
        
        IndexResults {
            bess_all: bess_all_value,
            bess_25: bess_25_value,
            bess_10: bess_10_value,
            market_stats: MarketStats {
                total_count: market_size,
                total_capacity_mw: all_eligible.total_capacity(),
                new_this_quarter: all_eligible.new_additions(),
            },
            ratios: calculate_ratios(bess_all_value, bess_25_value, bess_10_value),
            divergence: analyze_divergence(),
            movers: identify_top_movers(),
        }
    }
}
```

## Conclusion

The three-tier BESS index system provides:

1. **BESS-ALL**: Complete market representation (currently ~50 assets)
2. **BESS-25**: Core performer benchmark (top half of market)
3. **BESS-10**: Elite performance tracking (top 20% of market)

### Adaptive Design
The system is designed to grow with the market:
- **Today (2024)**: BESS-ALL (~50), BESS-25, BESS-10
- **2026**: Transition to BESS-50, BESS-25, BESS-10 as market grows
- **2028+**: Full BESS-100, BESS-50, BESS-10 structure

This adaptive approach ensures the indices remain relevant and useful regardless of market size, while maintaining consistent methodology that investors and operators can rely on.