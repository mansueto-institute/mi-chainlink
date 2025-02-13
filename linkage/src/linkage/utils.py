import datetime
import os
from typing import Any, dict

import duckdb
import jsonschema
import polars as pl
import yaml
from duckdb import DuckDBPyConnection


def load_config(file_path: str) -> dict:
    """
    load yaml config file, clean up column names

    Returns: dict
    """

    with open(file_path) as file:
        config = yaml.safe_load(file)

    validate_config(config)

    # create snake case columns
    for schema in config["schemas"]:
        for table in schema["tables"]:
            table["name_cols_og"] = table["name_cols"]
            table["name_cols"] = [x.lower().replace(" ", "_") for x in table["name_cols"]]

            table["address_cols_og"] = table["address_cols"]
            table["address_cols"] = [x.lower().replace(" ", "_") for x in table["address_cols"]]

            table["id_col_og"] = table["id_col"]
            table["id_col"] = table["id_col"].lower().replace(" ", "_")

    return config


def validate_config(config: dict[str, Any]) -> None:
    """
    Validates the configuration against a schema
    """
    schema = {
        "type": "object",
        "required": ["options", "schemas"],
        "properties": {
            "options": {
                "type": "object",
                "required": ["db_path"],
                "properties": {
                    "db_path": {"type": "string"},
                    "force_db_create": {"type": "boolean"},
                    "export_tables": {"type": "boolean"},
                    "update_config_only": {"type": "boolean"},
                    "link_exclusions": {"type": "array"},
                    "bad_address_path": {"type": "string"},
                    "export_tables_path": {"type": "string"},
                },
            },
            "schemas": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["schema_name", "tables"],
                    "properties": {
                        "schema_name": {"type": "string"},
                        "tables": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["table_name", "table_name_path", "id_col"],
                                "properties": {
                                    "table_name": {"type": "string"},
                                    "table_name_path": {"type": "string"},
                                    "id_col": {"type": "string"},
                                    "name_cols": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                    "address_cols": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }

    try:
        jsonschema.validate(instance=config, schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        raise ValueError(f"Invalid configuration: {e!s}")


def update_config(db_path: str, config: dict) -> None:
    """
    update config by adding in all existing link columns and last updated time.
    writes config back out to config.yaml

    Returns: None
    """

    with duckdb.connect(db_path) as conn:
        df_db_columns = conn.sql("show all tables").df()

    all_links = []
    for cols in df_db_columns["column_names"].tolist():
        all_links += [col for col in cols if "match" in col]

    config["metadata"]["existing_links"] = all_links
    config["metadata"]["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open("configs/config.yaml", "w+") as f:
        yaml.dump(config, f)


def export_tables(db_path: str, data_path: str) -> None:
    """
    export all tables from database to parquet files in {data_path}/export directory

    Returns: None
    """

    # create export directory if doesn't exist
    if not os.path.exists(f"{data_path}/export"):
        os.makedirs(f"{data_path}/export")

    def find_id_cols(row):
        if row["schema"] == "link" or row["name"] == "name_similarity":
            return row["column_names"][:2]
        elif row["schema"] == "entity":
            return row["column_names"][1]
        else:
            return row["column_names"][0]

    with duckdb.connect(db_path) as conn:
        df_db_columns = conn.sql("show all tables").df()

        df_db_columns["schema_table"] = df_db_columns["schema"] + "." + df_db_columns["name"]
        df_db_columns["id_col"] = df_db_columns.apply(lambda x: find_id_cols(x), axis=1)

        link_filter = (df_db_columns["schema"] == "link") | (df_db_columns["name"] == "name_similarity")

        links_to_export = zip(
            df_db_columns[link_filter]["schema_table"].tolist(),
            df_db_columns[link_filter]["id_col"].tolist(),
        )

        for link in links_to_export:
            links_query = f"""
                (select * from {link[0]}
                order by {link[1][0]} ASC, {link[1][1]} ASC);
            """
            print(links_query)
            d = conn.execute(links_query).pl().cast({link[1][0]: pl.String, link[1][1]: pl.String})
            d.write_parquet(f"{data_path}/export/{link[0].replace('.', '_')}.parquet")

        main_filter = (df_db_columns["schema"] != "link") & (df_db_columns["name"] != "name_similarity")
        main_to_export = zip(
            df_db_columns[main_filter]["schema_table"].tolist(),
            df_db_columns[main_filter]["id_col"].tolist(),
        )

        for table in main_to_export:
            sql_to_exec = f"""
                (select * from {table[0]}
                order by {table[1]} ASC);
            """
            d = conn.execute(sql_to_exec).pl().cast({table[1]: pl.String})
            d.write_parquet(f"{data_path}/export/{table[0].replace('.', '_')}.parquet")

    print("Exported all tables!")


def check_table_exists(db_conn: DuckDBPyConnection, schema: str, table_name: str) -> bool:
    """
    check if a table exists

    Returns: bool
    """

    db_conn.execute(
        f"""    SELECT COUNT(*)
                FROM   information_schema.tables
                WHERE  table_name = '{table_name}'
                AND    table_schema = '{schema}'"""
    )

    return db_conn.fetchone()[0] == 1:
