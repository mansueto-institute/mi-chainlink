### Bad Address Handling

The framework can exclude known bad addresses from matching by adding file paths to the configuration. This is useful for filtering out addresses that are known to be incorrect or problematic. The file should be a CSV with a header row with the first column containing the bad addresses.

```yaml
options:
  bad_address_path: data/bad_addresses.csv
  ...
```

### Link Exclusions

You can exclude specific types of links from being created in the database. This is useful for filtering out certain types of matches that may not be relevant to your analysis. Include the link types you want to exclude in the configuration file.

```yaml:
options:
  link_exclusions:
  - exclude_link_1
  - exclude_link_2
  ...
```

### Incremental Updates

The framework supports incremental updates to an existing database. Change `overwrite_db` option to `false` in the configuration file. This allows you to add new data to the database without overwriting existing data.

```yaml:

options:
  overwrite_db: false
  ...
```