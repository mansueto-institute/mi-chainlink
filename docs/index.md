# chainlink

A powerful, flexible framework for entity resolution and record linkage using DuckDB as the database engine built upon the work of [Who Owns Chicago](https://github.com/mansueto-institute/who-owns-chi/) by the [Mansueto Institute for Urban Innovation](https://miurban.uchicago.edu/) including the work of [Kevin Bryson](https://github.com/jamesturk), [Ana (Anita) Restrepo Lachman](https://github.com/johnketchum), [Caitlin P.](https://github.com/johnketchum), [Joaquin Pinto](https://github.com/johnketchum), and [Divij Sinha](https://github.com/johnketchum). 


This package enables you to load data from various sources, clean and standardize entity names and addresses, and create links between entities based on exact and fuzzy matching techniques.

Source: https://github.com/mansueto-institute/linkage

Documentation: TK

Issues: https://github.com/mansueto-institute/linkage/issues

## Overview

This framework helps you solve the entity resolution problem by:

1. Loading data from multiple sources into a DuckDB database
2. Cleaning and standardizing entity names and addresses
3. Creating exact matches between entities based on names and addresses
4. Generating fuzzy matches using TF-IDF similarity
5. Exporting the resulting linked data for further analysis

The system is designed to be configurable through YAML files and supports incremental updates to an existing database.

## Installation

```bash
# Clone the repository
git clone https://github.com/mansueto-institute/linkage.git
cd linkage
```

[Install uv](https://docs.astral.sh/uv/getting-started/installation/), then run the following command to install the dependencies.

```bash
uv sync
```


## Usage

### Command Line Interface

```bash
# Run with default settings
python main.py

# Run with load only (no linking)
python main.py --load_only=True

# Run without probabilistic matching
python main.py --probabilistic=False
```

## Configuration

The framework uses YAML configuration files to define data sources and linking parameters. See a template file in `chainlink/configs/config_template.yaml`. Here's an example configuration:

```yaml
options:
  force_db_create: false
  export_tables: true
  update_config_only: false
  link_exclusions: []
  bad_address_path: "data/bad_addresses.csv"

schemas:
  - schema_name: "property"
    tables:
      - table_name: "parcels"
        table_name_path: "data/parcels.csv"
        id_col: "parcel_id"
        name_cols: ["owner_name", "taxpayer_name"]
        address_cols: ["property_address", "mailing_address"]
        
  - schema_name: "business"
    tables:
      - table_name: "llc"
        table_name_path: "data/llc_registrations.csv"
        id_col: "llc_id"
        name_cols: ["company_name", "registered_agent"]
        address_cols: ["business_address"]
```

### Interactive Configuration

If you don't have a configuration file, the system will guide you through creating one:

```bash
python main.py
# Enter config path. Type 'create' if you would you like to create one.
create
# Export tables to parquet after load? (y/n)
y
# [Optional] Provide path to bad address csv file:
data/bad_addresses.csv
# Add a new schema? (y/n)
y
# Enter the name of the schema:
property
# Enter the name of dataset:
parcels
# Enter the path to the dataset:
data/parcels.csv
# Enter the id column of the dataset. Must be unique:
parcel_id
# Enter the name column(s) (comma separated):
owner_name,taxpayer_name
# Enter the address column(s) (comma separated):
property_address,mailing_address
# Add a table to this schema? (y/n)
n
# Add another schema? (y/n)
n
```


## Detailed Process

### 1. Loading Data

The framework loads data from CSV or Parquet files as specified in the configuration. For each table:

- Validates required columns exist
- Converts column names to snake_case
- Cleans entity names and addresses
- Creates unique IDs for names, addresses, streets, and street names
- Loads data into the specified schema in the DuckDB database


### 2. Creating Links

#### Exact Matching

The framework creates exact matches between entities based on:

- Name matches: Exact string matches between name fields
- Address matches:
    - Raw address string matches
    - Street matches
    - Unit matches (when street matches)
    - Street name and number matches (when zip code matches)


#### Fuzzy Matching

If enabled, the framework also creates fuzzy matches using TF-IDF:

1. Generates TF-IDF vectors for all entity names
2. Computes similarity scores between entities
3. Stores matches above a threshold (0.8 by default)

### 3. Exporting Results

If configured, the framework exports all tables to Parquet files in the specified directory.