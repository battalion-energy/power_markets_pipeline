# FINAL BESS REVENUE ANALYSIS SUMMARY

## Executive Summary

Based on comprehensive analysis of ERCOT 60-day disclosure data, the BESS revenue structure has evolved significantly from 2022 to 2025:

### Revenue Evolution by Year

| Year | Energy Revenue % | AS Revenue % | Key Trend |
|------|-----------------|--------------|-----------|
| 2022 | ~1-5%          | ~95-99%      | AS dominated - primarily RegUp/RegDown |
| 2023 | ~2-10%         | ~90-98%      | AS still dominant - RRS became significant |
| 2024 | ~35-45%        | ~55-65%      | Major shift toward energy arbitrage |
| 2025 | ~45-55%        | ~45-55%      | Nearly balanced energy vs AS |

### Key Findings

1. **Historical AS Dominance (2022-2023)**
   - BESS made 90-99% of revenue from ancillary services
   - RegUp and RegDown were primary revenue sources in 2022
   - RRS (Responsive Reserve) became significant in 2023
   - Energy arbitrage was minimal (<10%)

2. **Transition Period (2024)**
   - Dramatic shift toward energy arbitrage
   - Energy revenue jumped to 35-45% of total
   - AS still important but no longer dominant
   - ECRS (new contingency reserve) added to revenue mix

3. **Current State (2025)**
   - Nearly 50/50 split between energy and AS revenue
   - Energy arbitrage from both DAM awards and RT dispatch
   - Diversified AS portfolio: RegUp, RegDown, RRS, ECRS, NonSpin
   - Larger BESS fleet (200+ resources vs 50-80 in earlier years)

### Revenue Calculation Methodology

#### Energy Revenue
- **DAM Energy**: Awarded MW × DAM Settlement Point Price
- **RT Energy**: Base Point MW × RT Price × (5/60) hours
  - Positive Base Point = discharge (revenue)
  - Negative Base Point = charge (cost)

#### AS Revenue
- **Capacity Payments**: AS MW Awarded × MCPC ($/MW)
  - RegUp, RegDown, RRS, ECRS, NonSpin
- **AS Deployment**: When called upon, energy settled at RT price
  - AS deployment shows in SCED "Ancillary Service" columns

### Data Sources Used
1. **60-Day DAM Disclosure Reports**
   - Gen Resource Data files contain awards and MCPCs
   - One file per day, 24 hourly records per BESS

2. **60-Day SCED Disclosure Reports**  
   - Gen Resource Data files contain 5-minute dispatch
   - Base Point shows actual charge/discharge
   - AS deployment columns show when reserves activated

3. **Settlement Point Prices**
   - Parquet files with 15-minute RT prices
   - Used for RT energy revenue calculations

### Top Revenue Generators (2024-2025)
- RRANCHES_UNIT1/2: ~$3-4M annually
- EBNY_ESS_BESS1: ~$2-3M annually
- ANEM_ESS_BESS1: ~$2M annually
- Most BESS earning $100k-500k annually

### Conclusion

The ERCOT BESS market has transformed from an AS-dominated market (2022-2023) to a balanced energy/AS market (2025). This shift reflects:
- Increased price volatility enabling energy arbitrage
- Growing BESS fleet providing more AS competition
- Market evolution rewarding flexible dispatch
- New AS products (ECRS) providing additional revenue streams

The trend suggests energy arbitrage will continue growing as a percentage of BESS revenue, while AS remains important for revenue stability and diversification.