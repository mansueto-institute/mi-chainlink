## Linkage

A record linkage tool for matching records between multiple datasets built upon the work of [Who Owns Chicago](https://github.com/mansueto-institute/who-owns-chi/) by the [Mansueto Institute for Urban Innovation](https://miurban.uchicago.edu/) including the work of [Kevin Bryson](https://github.com/jamesturk), [Ana (Anita) Restrepo Lachman](https://github.com/johnketchum), [Caitlin P.](https://github.com/johnketchum), [Joaquin Pinto](https://github.com/johnketchum), and [Divij Sinha](https://github.com/johnketchum).

Source: https://github.com/mansueto-institute/linkage

Documentation: TK

Issues: https://github.com/mansueto-institute/linkage/issues

## Installation
First [install uv](https://docs.astral.sh/uv/getting-started/installation/), then run the following command to install the dependencies.

```bash
uv sync
```

## Overview

A flexible record linkage framework that enables matching between multiple datasets using both exact and fuzzy matching techniques. The system supports matching on both name and address fields, with multiple matching strategies including raw string matching, standardized street matching, unit-level matching when streets align, and fuzzy name matching using TF-IDF similarity scores. The tool is built with a generic loading interface that can handle various input formats (CSV, Parquet) and data schemas, making it adaptable for different record linkage scenarios while maintaining data in a DuckDB database for efficient processing. The matching process automatically handles edge cases like duplicate matches, self-matches within datasets, and provides configurable thresholds for fuzzy matching, while also supporting the ability to exclude specific match types through configuration.

#### Input

Use the config file to specify the input data, including the data source, id, name and address columns and additional settings. 

#### Output

The output of the matching process is a database with the following schema:

- `entity.names`: A table containing the matches between the datasets.
- `entity.name_similarity`: A table containing the similarity scores between the matches.
- 


## Example Usage

```bash
python generic_load_link/main.py --config generic_load_link/configs/config_template.yaml
```

