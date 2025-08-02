# 60-Day Disclosure Data Documentation

## Overview
ERCOT publishes 60-day disclosure reports that contain detailed operational data including generator dispatch, ancillary service awards, and market clearing prices. This documentation covers the structure and contents of each disclosure folder.

## Directory Structure

### 1. 60-Day_COP_Adjustment_Period_Snapshot
**Location**: `/Users/enrico/data/ERCOT_data/60-Day_COP_Adjustment_Period_Snapshot`
**Purpose**: Contains Current Operating Plan (COP) snapshots showing resource registration and characteristics
**Key Files**: 
- `60d_COP_Adjustment_Period_Snapshot-DD-MMM-YY.csv`

**Content Structure**:
- Resource registration data
- Generator characteristics (HSL, LSL, ramp rates)
- Resource status information
- NOT used for revenue calculations directly

### 2. 60-Day_DAM_Disclosure_Reports
**Location**: `/Users/enrico/data/ERCOT_data/60-Day_DAM_Disclosure_Reports`
**Purpose**: Day-Ahead Market awards and clearing prices
**Key Files**:
- `60d_DAM_Gen_Resource_Data-DD-MMM-YY.csv` - Generator awards including BESS

**Important Columns for BESS Revenue**:
```
- Delivery Date
- Hour Ending (1-24)
- Resource Name
- Resource Type (filter for 'PWRSTR')
- Awarded Quantity (MW) - Energy award
- Energy Settlement Point Price ($/MWh)
- RegUp Awarded (MW)
- RegUp MCPC ($/MW)
- RegDown Awarded (MW)
- RegDown MCPC ($/MW)
- RRSPFR/RRSFFR/RRSUFR Awarded (MW) - Responsive Reserve
- RRS MCPC ($/MW)
- ECRSSD Awarded (MW) - ECRS Sustained Discharge
- ECRS MCPC ($/MW)
- NonSpin Awarded (MW)
- NonSpin MCPC ($/MW)
```

### 3. 60-Day_SCED_Disclosure_Reports
**Location**: `/Users/enrico/data/ERCOT_data/60-Day_SCED_Disclosure_Reports`
**Purpose**: Real-time (SCED) dispatch and prices
**Key Files**:
- `60d_SCED_Gen_Resource_Data-DD-MMM-YY.csv` - 5-minute dispatch data
- `60d_SCED_SMNE_GEN_RES-DD-MMM-YY.csv` - Settlement Metered Net Energy

**Important Columns for BESS Revenue**:
```
- SCED Time Stamp (MM/DD/YYYY HH:MM:SS)
- Resource Name
- Resource Type (filter for 'PWRSTR')
- Base Point (MW) - RT dispatch
- Telemetered Net Output (MW) - Actual output
- Output Schedule (MW)
- HSL/LSL - High/Low Sustainable Limits
```

### 4. 60-Day_SASM_Disclosure_Reports
**Location**: `/Users/enrico/data/ERCOT_data/60-Day_SASM_Disclosure_Reports`
**Purpose**: Supplemental Ancillary Service Market data
**Key Files**:
- Various SASM procurement and award files
- MCPC (Market Clearing Price for Capacity) by service type

### 5. 60-Day_COP_All_Updates
**Location**: `/Users/enrico/data/ERCOT_data/60-Day_COP_All_Updates`
**Purpose**: All COP updates throughout the operating period
**Key Files**:
- Update records for resource status changes

## Revenue Calculation Methodology

### 1. RT Energy Revenue
```
For each 5-minute interval:
  RT_Revenue = Base_Point_MW * RT_Settlement_Point_Price * (5/60)
```

### 2. DAM Energy Revenue
```
For each hour:
  DAM_Revenue = Awarded_Quantity_MW * Energy_Settlement_Point_Price
```

### 3. Ancillary Service Revenues
```
For each hour and service:
  AS_Revenue = AS_Awarded_MW * AS_MCPC
```

Services include:
- RegUp: Regulation Up
- RegDown: Regulation Down
- RRS: Responsive Reserve Service (includes RRSPFR, RRSFFR, RRSUFR)
- ECRS: ERCOT Contingency Reserve Service
- NonSpin: Non-Spinning Reserve

## File Inspection Results

### Sample DAM Gen Resource Data Analysis
- Total records per file: ~35,000
- BESS records per file: ~5,000-6,000
- Each BESS typically has 24 hourly records per day

### Sample SCED Gen Resource Data Analysis
- 5-minute intervals (288 per day)
- Contains actual dispatch (Base Point) values
- Shows charging (negative MW) and discharging (positive MW)

## Data Quality Considerations

1. **Missing Data**: Some intervals may have missing prices or dispatch values
2. **Settlement Point Mapping**: BESS resources may have different settlement points for energy vs location
3. **Time Zones**: All times are in CPT (Central Prevailing Time)
4. **Data Lag**: 60-day disclosure means data is available 60 days after operating day

## Implementation Notes

1. **Extract all ZIP files** to CSV format first
2. **Filter for Resource Type = 'PWRSTR'** to get BESS resources only
3. **Match settlement points** between price files and resource files
4. **Handle both charging and discharging** for energy arbitrage calculations
5. **Sum all revenue streams** for comprehensive analysis