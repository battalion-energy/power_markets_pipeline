use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ResourceMapping {
    pub resource_name: String,
    pub unit_name: String,
    pub settlement_point: String,
    pub capacity_mw: Option<f64>,
    pub duration_hours: Option<f64>,
}

pub struct SettlementMapper {
    mappings: HashMap<String, ResourceMapping>,
}

impl SettlementMapper {
    /// Load settlement point mappings from ERCOT CSV files
    pub fn from_ercot_files(resource_node_path: &str) -> Result<Self> {
        let df = CsvReader::from_path(resource_node_path)?
            .has_header(true)
            .finish()?;

        let mut mappings = HashMap::new();

        // Extract columns
        let resource_nodes = df.column("RESOURCE_NODE")?.str()?;
        let unit_names = df.column("UNIT_NAME")?.str()?;
        let unit_substations = df.column("UNIT_SUBSTATION")?.str()?;

        for idx in 0..df.height() {
            if let (Some(resource_node), Some(unit_name), Some(unit_substation)) = (
                resource_nodes.get(idx),
                unit_names.get(idx),
                unit_substations.get(idx),
            ) {
                // For BESS units, the settlement point is typically the resource node
                let mapping = ResourceMapping {
                    resource_name: unit_name.to_string(),
                    unit_name: unit_name.to_string(),
                    settlement_point: resource_node.to_string(),
                    capacity_mw: None, // Would need to load from separate file
                    duration_hours: None,
                };

                mappings.insert(unit_name.to_string(), mapping.clone());
                // Also index by resource node for flexibility
                mappings.insert(resource_node.to_string(), mapping);
            }
        }

        Ok(Self { mappings })
    }

    /// Get settlement point for a resource
    pub fn get_settlement_point(&self, resource_name: &str) -> Option<&str> {
        self.mappings
            .get(resource_name)
            .map(|m| m.settlement_point.as_str())
    }

    /// Get full mapping for a resource
    pub fn get_mapping(&self, resource_name: &str) -> Option<&ResourceMapping> {
        self.mappings.get(resource_name)
    }

    /// Get all BESS resources
    pub fn get_all_bess(&self) -> Vec<&ResourceMapping> {
        self.mappings
            .values()
            .filter(|m| m.unit_name.contains("BESS") || m.unit_name.contains("ESS"))
            .collect()
    }

    /// Add or update a mapping
    pub fn add_mapping(&mut self, mapping: ResourceMapping) {
        self.mappings
            .insert(mapping.resource_name.clone(), mapping.clone());
        self.mappings.insert(mapping.unit_name.clone(), mapping);
    }

    /// Load additional battery specifications from a separate file
    pub fn load_battery_specs(&mut self, specs_path: &str) -> Result<()> {
        let df = CsvReader::from_path(specs_path)?
            .has_header(true)
            .finish()?;

        let resource_names = df.column("resource_name")?.str()?;
        let capacities = df.column("capacity_mw")?.f64()?;
        let durations = df.column("duration_hours")?.f64()?;

        for idx in 0..df.height() {
            if let (Some(name), Some(capacity), Some(duration)) = (
                resource_names.get(idx),
                capacities.get(idx),
                durations.get(idx),
            ) {
                if let Some(mapping) = self.mappings.get_mut(name) {
                    mapping.capacity_mw = Some(capacity);
                    mapping.duration_hours = Some(duration);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapping_creation() {
        let mut mapper = SettlementMapper {
            mappings: HashMap::new(),
        };

        let mapping = ResourceMapping {
            resource_name: "TEST_BESS".to_string(),
            unit_name: "BESS1".to_string(),
            settlement_point: "TEST_NODE_RN".to_string(),
            capacity_mw: Some(100.0),
            duration_hours: Some(2.0),
        };

        mapper.add_mapping(mapping);

        assert_eq!(
            mapper.get_settlement_point("TEST_BESS"),
            Some("TEST_NODE_RN")
        );
        assert_eq!(mapper.get_settlement_point("BESS1"), Some("TEST_NODE_RN"));

        let bess_list = mapper.get_all_bess();
        assert_eq!(bess_list.len(), 1);
    }
}