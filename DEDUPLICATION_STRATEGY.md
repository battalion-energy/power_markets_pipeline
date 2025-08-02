# ERCOT Data Deduplication Strategy

## Overview
This document explains the deduplication strategy for each ERCOT data type and shows the CSV column headers.

## 1. RT Settlement Point Prices
**Folder**: `Settlement_Point_Prices_at_Resource_Nodes,_Hubs_and_Load_Zones`

**CSV Headers**:
```
DeliveryDate,DeliveryHour,DeliveryInterval,SettlementPointName,SettlementPointType,SettlementPointPrice,DSTFlag
```

**Deduplication Keys**: 
- DeliveryDate
- DeliveryHour  
- DeliveryInterval
- SettlementPointName

**Why**: Each unique combination of date + hour (1-24) + interval (1-4 for 15-min intervals) + settlement point name represents a unique price record.

## 2. DAM Hourly LMPs
**Folder**: `DAM_Hourly_LMPs`

**CSV Headers**:
```
DeliveryDate,HourEnding,BusName,LMP,DSTFlag
```

**Deduplication Keys**:
- DeliveryDate
- HourEnding
- BusName

**Why**: Each unique combination of date + hour + bus name represents a unique LMP record.

## 3. DAM Settlement Point Prices
**Folder**: `DAM_Settlement_Point_Prices`

**CSV Headers**:
```
DeliveryDate,HourEnding,SettlementPoint,SettlementPointPrice
```

**Deduplication Keys**:
- DeliveryDate
- HourEnding
- SettlementPoint

**Why**: Each unique combination of date + hour + settlement point represents a unique price record.

## 4. DAM Clearing Prices for Capacity
**Folder**: `DAM_Clearing_Prices_for_Capacity`

**CSV Headers**:
```
DeliveryDate,HourEnding,AncillaryType,MCPC,DSTFlag
```

**Deduplication Keys**:
- DeliveryDate
- HourEnding
- AncillaryType

**Why**: Each unique combination of date + hour + ancillary service type (REGUP, REGDN, RRS, etc.) represents a unique clearing price.

## 5. SCED Shadow Prices
**Folder**: `SCED_Shadow_Prices_and_Binding_Transmission_Constraints`

**CSV Headers**:
```
SCEDTimestamp,ConstraintName,ShadowPrice,MaxShadowPrice,ConstraintLimit,ConstraintValue,ViolationAmount
```

**Deduplication Keys**:
- SCEDTimestamp
- ConstraintName

**Why**: Each unique combination of timestamp + constraint name represents a unique shadow price record.

## 6. DAM Shadow Prices
**Folder**: `DAM_Shadow_Prices`

**CSV Headers**:
```
DeliveryDate,HourEnding,ConstraintName,ConstraintID,ShadowPrice,MaxShadowPrice,ConstraintLimit,ConstraintValue,ViolationAmount
```

**Deduplication Keys**:
- DeliveryDate
- HourEnding
- ConstraintName

**Why**: Each unique combination of date + hour + constraint name represents a unique shadow price record.

## Time Handling

### RT Data (5-minute intervals)
- **DeliveryHour**: 1-24 (hour 24 = midnight of next day)
- **DeliveryInterval**: 1-4 (15-minute intervals)
- Converted to datetime: `date + (hour-1) + (interval-1)*15 minutes`

### DAM Data (hourly)
- **HourEnding**: 1-24 (hour 24 = midnight of next day)
- Converted to datetime: `date + (hour-1)`

## Important Notes

1. **No duplicates should exist** after applying these deduplication keys
2. **DSTFlag** is NOT used for deduplication as it's informational only
3. **Price columns** are NOT used for deduplication - we keep the last occurrence if duplicates exist
4. **All data is sorted by datetime** after deduplication for consistent ordering