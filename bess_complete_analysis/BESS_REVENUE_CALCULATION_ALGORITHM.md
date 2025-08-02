# BESS Revenue Calculation Algorithm - Detailed Documentation

## Overview
This document explains EXACTLY how BESS revenues are calculated using ERCOT 60-day disclosure data and price files. **NO MOCK OR FAKE DATA IS USED** - all calculations use actual ERCOT published data.

## Data Sources

### 1. 60-Day Disclosure Directories

#### A. DAM Disclosure Reports
**Directory**: `/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports/csv/`

**Key File Pattern**: `60d_DAM_Gen_Resource_Data-DD-MMM-YY.csv`
- Example: `60d_DAM_Gen_Resource_Data-16-NOV-24.csv`
- One file per operating day
- Contains 24 hourly records per resource

**Columns Used**:
```
- Delivery Date: Date of operation (MM/DD/YYYY)
- Hour Ending: Hour 1-24
- Resource Name: BESS identifier (e.g., "NF_BRP_BES1")
- Resource Type: Filter for "PWRSTR" (Power Storage)
- Awarded Quantity: DAM energy award in MW
- Energy Settlement Point Price: DAM energy price in $/MWh
- RegUp Awarded: Regulation Up capacity awarded in MW
- RegUp MCPC: Regulation Up clearing price in $/MW
- RegDown Awarded: Regulation Down capacity awarded in MW
- RegDown MCPC: Regulation Down clearing price in $/MW
- RRSPFR Awarded: RRS Primary Frequency Response MW
- RRSFFR Awarded: RRS Fast Frequency Response MW
- RRSUFR Awarded: RRS Under Frequency Relay MW
- RRS MCPC: RRS clearing price in $/MW (applies to all RRS types)
- ECRSSD Awarded: ECRS Sustained Discharge MW
- ECRS MCPC: ECRS clearing price in $/MW
- NonSpin Awarded: Non-Spinning Reserve MW
- NonSpin MCPC: Non-Spinning Reserve clearing price in $/MW
```

#### B. SCED Disclosure Reports
**Directory**: `/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports/csv/`

**Key File Pattern**: `60d_SCED_Gen_Resource_Data-DD-MMM-YY.csv`
- Example: `60d_SCED_Gen_Resource_Data-01-APR-24.csv`
- One file per operating day
- Contains 288 five-minute intervals per resource (24 hours × 12 intervals/hour)

**Columns Used**:
```
- SCED Time Stamp: Timestamp (MM/DD/YYYY HH:MM:SS)
- Resource Name: BESS identifier
- Resource Type: Filter for "PWRSTR"
- Base Point: Real-time dispatch in MW
  - Positive = discharge (selling energy)
  - Negative = charge (buying energy)
  - Zero = no dispatch
- Ancillary Service REGUP: RegUp deployment MW
- Ancillary Service REGDN: RegDown deployment MW
- Ancillary Service RRS: RRS deployment MW
- Ancillary Service ECRS: ECRS deployment MW
- Ancillary Service NSRS: NonSpin deployment MW
```

#### C. SASM Disclosure Reports (Supplemental AS)
**Directory**: `/Users/enrico/data/ERCOT_data/60-Day_SASM_Disclosure_Reports/csv/`

**Key File Pattern**: `60d_SASM_Generation_Resource_AS_Offer_Awards-DD-MMM-YY.csv`
- Contains supplemental AS procurements
- Used for additional AS awards beyond DAM

**Columns Used**:
```
- Resource Name
- Resource Type: Filter for "PWRSTR"
- REGUP Awarded, REGUP MCPC
- REGDN Awarded, REGDN MCPC
- RRSPFR/RRSFFR/RRSUFR Awarded, RRS MCPC
- ECRSS Awarded, ECRS MCPC
- NSPIN Awarded, NSPIN MCPC
```

### 2. Price Data Files

#### A. Real-Time Settlement Point Prices
**Directory**: `annual_output/Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones/`

**File Pattern**: `Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_YYYY.parquet`
- Example: `Settlement_Point_Prices_at_Resource_Nodes__Hubs_and_Load_Zones_2024.parquet`
- One file per year
- Contains 15-minute interval prices

**Columns Used**:
```
- DeliveryDate: Date (MM/DD/YYYY)
- DeliveryHour: Hour (1-24)
- DeliveryInterval: 15-minute interval (1-4 within each hour)
- SettlementPointName: Location identifier
- SettlementPointPrice: RT price in $/MWh
```

### 3. BESS Master List
**File**: `bess_analysis/bess_resources_master_list.csv`

**Columns Used**:
```
- Resource_Name: BESS identifier
- Settlement_Point: Maps resource to price location
- Max_Capacity_MW: Maximum capacity
- QSE: Qualified Scheduling Entity
```

## Revenue Calculation Algorithm

### Step 1: Load BESS Resources
```python
# Load master list to get resource->settlement point mapping
bess_df = pd.read_csv("bess_analysis/bess_resources_master_list.csv")
bess_map = {row['Resource_Name']: row['Settlement_Point'] for _, row in bess_df.iterrows()}
```

### Step 2: DAM Revenue Calculation
For each DAM Gen Resource Data file:

```python
# 1. Filter for BESS resources
bess_data = df[df['Resource Type'] == 'PWRSTR']

# 2. Calculate DAM Energy Revenue
dam_energy_revenue = Awarded_Quantity_MW × Energy_Settlement_Point_Price_$/MWh

# 3. Calculate AS Capacity Revenues
# Note: MCPC is same for all resources in given hour
for each hour:
    regup_revenue = RegUp_Awarded_MW × RegUp_MCPC_$/MW
    regdown_revenue = RegDown_Awarded_MW × RegDown_MCPC_$/MW
    
    # RRS combines all three types
    rrs_total_mw = RRSPFR_Awarded + RRSFFR_Awarded + RRSUFR_Awarded
    rrs_revenue = rrs_total_mw × RRS_MCPC_$/MW
    
    ecrs_revenue = ECRSSD_Awarded_MW × ECRS_MCPC_$/MW
    nonspin_revenue = NonSpin_Awarded_MW × NonSpin_MCPC_$/MW
```

### Step 3: RT Energy Revenue Calculation
For each SCED Gen Resource Data file:

```python
# 1. Filter for BESS resources
bess_data = df[df['Resource Type'] == 'PWRSTR']

# 2. For each 5-minute interval:
for each record:
    # Get RT price for this interval
    settlement_point = bess_map[resource_name]
    rt_price = lookup_price(settlement_point, timestamp)
    
    # Calculate energy revenue/cost
    # Base Point > 0: discharge (revenue)
    # Base Point < 0: charge (cost)
    energy_mwh = Base_Point_MW × (5/60)  # 5-minute interval
    rt_revenue = energy_mwh × rt_price_$/MWh
```

### Step 4: AS Deployment Revenue
When BESS provides AS and is deployed:
- AS deployment energy is settled at RT price
- This shows up in SCED "Ancillary Service" columns
- The energy component is already captured in Base Point

### Step 5: Annual Aggregation
```python
for each BESS:
    Annual_Revenue = {
        'RT_Revenue': sum of all RT interval revenues,
        'DA_Revenue': sum of all DAM energy revenues,
        'Spin_Revenue': sum of all RRS capacity payments,
        'NonSpin_Revenue': sum of all NonSpin capacity payments,
        'RegUp_Revenue': sum of all RegUp capacity payments,
        'RegDown_Revenue': sum of all RegDown capacity payments,
        'ECRS_Revenue': sum of all ECRS capacity payments,
        'Total_Revenue': sum of all above
    }
```

## Important Notes

1. **ALL DATA IS REAL** - No mock or synthetic data is used anywhere
2. **Time Alignment**: 
   - DAM files: Hourly data (24 records/day)
   - SCED files: 5-minute data (288 records/day)
   - Price files: 15-minute data (96 records/day)
3. **Settlement Point Mapping**: Critical to match BESS resource to correct price node
4. **AS Revenue**: Capacity payment (MW × MCPC) paid regardless of deployment
5. **Energy Arbitrage**: Includes both DAM awards and RT dispatch (charge/discharge cycles)

## Data Validation Checks

1. Resource Type must equal "PWRSTR" for BESS
2. MCPCs should be consistent within each hour (all resources see same AS prices)
3. Base Point sign indicates charge (-) or discharge (+)
4. Sum of all RRS types before applying RRS MCPC
5. Handle missing data with proper null checks

## File Processing Order

1. Process DAM files first (energy awards + AS capacity)
2. Process SCED files (RT dispatch)
3. Load RT prices and match to SCED intervals
4. Aggregate by resource and year
5. Output comprehensive revenue table

**This algorithm uses ONLY actual ERCOT published data - no mock data anywhere.**