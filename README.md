# mi-chainlink

A powerful, flexible framework for entity resolution and record linkage using DuckDB as the database engine built upon the work of [Who Owns Chicago](https://github.com/mansueto-institute/who-owns-chi/) by the [Mansueto Institute for Urban Innovation](https://miurban.uchicago.edu/) including the work of [Kevin Bryson](https://github.com/cmdkev), [Ana (Anita) Restrepo Lachman](https://github.com/anitarestrepo16), [Caitlin P.](https://github.com/CaitlinCP), [Joaquin Pinto](https://github.com/joaquinpinto), and [Divij Sinha](https://github.com/divij-sinha). 


This package enables you to load data from various sources, clean and standardize entity names and addresses, and create links between entities based on exact and fuzzy matching techniques.

Source: [https://github.com/mansueto-institute/mi-chainlink](https://github.com/mansueto-institute/mi-chainlink)

Documentation: [https://mansueto-institute.github.io/mi-chainlink/](https://mansueto-institute.github.io/mi-chainlink/)

Issues: [https://github.com/mansueto-institute/mi-chainlink/issues](https://github.com/mansueto-institute/mi-chainlink/issues)

## Overview

This framework helps you solve the entity resolution problem by:

1. Loading data from multiple sources into a DuckDB database
2. Cleaning and standardizing entity names and addresses
3. Creating exact matches between entities based on names and addresses
4. Generating fuzzy matches using TF-IDF similarity
5. Exporting the resulting linked data for further analysis

The system is designed to be configurable through YAML files and supports incremental updates to an existing database.

## Installation

Package is available on PyPI. You can install it using pip or uv:

```bash
pip install chainlink
```

```bash
uv add chainlink
```

## Usage

### Command Line Interface

```bash
# Run interactive session
chainlink

# Run with path to config yaml
chainlink path/to/config.yaml
```

```python
# run as a python function

from chainlink import chainlink

chainlink(
    config: dict, ## dict with config details
    config_path: str | Path = DIR / "configs/config.yaml", ## path to store dict post processing
)
```
