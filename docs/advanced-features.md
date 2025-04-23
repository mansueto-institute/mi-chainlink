### Bad Address Handling

The framework can exclude known bad addresses from matching:

```python
# Create a CSV with bad addresses
bad_addresses = pd.DataFrame(["PO BOX", "GENERAL DELIVERY", "UNKNOWN"])
bad_addresses.to_csv("data/bad_addresses.csv", index=False)

# Update config
config["options"]["bad_address_path"] = "data/bad_addresses.csv"
```

### Link Exclusions

You can exclude specific types of links:

```python
# Exclude certain match types
config["options"]["link_exclusions"] = ["unit_match", "street_num_match"]
```

### Incremental Updates

The framework supports incremental updates to an existing database:

```python
# First load
chainlink(config)

# Add new data to config
config["schemas"].append({
    "schema_name": "nonprofit",
    "tables": [
        {
            "table_name": "orgs",
            "table_name_path": "data/nonprofits.csv",
            "id_col": "org_id",
            "name_cols": ["organization_name"],
            "address_cols": ["org_address"]
        }
    ]
})

# Update database with new schema
chainlink(config)
```