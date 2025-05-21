# chainlink

A powerful, flexible framework for entity resolution and record linkage using DuckDB as the database engine built upon the work of Who Owns Chicago (public release Summer 2025) by the [Mansueto Institute for Urban Innovation](https://miurban.uchicago.edu/) including the work of [Kevin Bryson](https://github.com/cmdkev), [Ana (Anita) Restrepo Lachman](https://github.com/anitarestrepo16), [Caitlin P.](https://github.com/CaitlinCP), [Joaquin Pinto](https://github.com/joaquinpinto), and [Divij Sinha](https://github.com/divij-sinha). 


This package enables you to load data from various sources, clean and standardize entity names and addresses, and create links between entities based on exact and fuzzy matching techniques.

Source: [https://github.com/mansueto-institute/chainlink](https://github.com/mansueto-institute/chainlink)

Documentation: [https://mansueto-institute.github.io/chainlink/](https://mansueto-institute.github.io/chainlink/)

Issues: [https://github.com/mansueto-institute/chainlink/issues](https://github.com/mansueto-institute/chainlink/issues)

## Overview

This framework helps you solve the entity resolution problem by:

1. Loading data from multiple sources into a DuckDB database
2. Cleaning and standardizing entity names and addresses
3. Creating exact matches between entities based on names and addresses
4. Generating fuzzy matches using TF-IDF similarity on names and addresses
5. Exporting the resulting linked data for further analysis

The system is designed to be configurable through YAML files and supports incremental updates to an existing database.

## Installation

```bash
# Clone the repository
git clone https://github.com/mansueto-institute/chainlink.git
cd linkage
```

[Install uv](https://docs.astral.sh/uv/getting-started/installation/), then run the following command to install the dependencies.

```bash
uv sync
```


## Usage

### Command Line Interface

```bash
# precongire YAML config file and run using config
chainlink [<path_to_config_file>]

# or run config creater
chainlink
```

## Configuration

The framework uses YAML configuration files to define data sources and linking parameters. See a template file in `src/chainlink/configs/config_template.yaml`. Here's a template configuration. See an example config at `src/chainlink/configs/woc_config_sample.yaml` :

```yaml
options:
  bad_address_path: data/bad_addresses.csv # path to a csv file with bad addresses that should not be matched
  export_tables: true # bool whether to export the tables to parquet files
  db_path: data/link.db # path to the resulting DuckDB database
  overwrite_db: false # whether to force overwrite the existing database or add to existing tables
  link_exclusions: # can specify exclusions for the matching process
  update_config_only: false # whether to update the config only
  load_only: false # whether to only load the data without matching
  probabilistic: true # whether to use probabilistic matching for name and address
schemas:
  - schema_name: schema1 # name of the schema
    tables:
      - address_cols:
          - address # address column
        id_col: file_num1 # id column
        name_cols:
          - name_raw # name column
        table_name: table1 # name of the table
        table_name_path: data/import/schema1_table1.parquet # path to the table
      - address_cols:
          - address2 # address column
        id_col: file_num2 # id column
        name_cols:
          - name2 # name column
        table_name: table2 # name of the table
        table_name_path: data/schema1_table2.parquet # path to the table
  - schema_name: schema2 # name of the schema
    tables:
      - address_cols:
          - mailing_address # address column
          - property_address # address column
        id_col: pin # id column
        name_cols:
          - tax_payer_name # name column
        table_name: table1 # name of the table
        table_name_path: data/schema2_table1.parquet # path to the table
metadata:
  existing_links:
  last_updated: # date of the last update

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

- Names
  1. Generates TF-IDF vectors for all entity names
  1. Computes similarity scores between entities
  1. Stores matches above a threshold (0.8 by default)
- Addresses
  1. Generates TF-IDF vectors for all entity addresses
  1. Computes similarity scores between entities
  1. Stores matches above a threshold (0.8 by default)

### 3. Exporting Results

If configured, the framework exports all tables to Parquet files in the specified directory.