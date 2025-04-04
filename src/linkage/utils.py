import datetime
import logging
import os
from pathlib import Path

import duckdb
import jsonschema
import polars as pl
import yaml
from duckdb import DuckDBPyConnection


def setup_logger(name: str, log_file: str, level: int | str = logging.DEBUG) -> logging.Logger:
    """
    To setup as many loggers as you want
    # from https://stackoverflow.com/questions/11232230/logging-to-two-files-with-different-settings
    """

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


logger = setup_logger("linkage", "linkage.log")


def load_config(file_path: str) -> dict:
    """
    load yaml config file, clean up column names

    Returns: dict
    """

    with open(file_path) as file:
        config = yaml.safe_load(file)

    # create snake case columns
    for schema in config["schemas"]:
        for table in schema["tables"]:
            if table["name_cols"] is not None:
                table["name_cols_og"] = table["name_cols"]
                table["name_cols"] = [x.lower().replace(" ", "_") for x in table["name_cols"]]
            else:
                table["name_cols"] = []

            if table["address_cols"] is not None:
                table["address_cols_og"] = table["address_cols"]
                table["address_cols"] = [x.lower().replace(" ", "_") for x in table["address_cols"]]
            else:
                table["address_cols"] = []

            table["id_col_og"] = table["id_col"]
            table["id_col"] = table["id_col"].lower().replace(" ", "_")

    return config


def validate_config(config: dict) -> bool:
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
                    "force_db_create": {"type": "boolean"},
                    "export_tables": {"type": "boolean"},
                    "update_config_only": {"type": "boolean"},
                    "link_exclusions": {"type": ["array", "null"]},  # or none
                    "bad_address_path": {"type": "string"},  # or none
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
                                        "type": ["array", "null"],
                                        "items": {"type": "string"},
                                    },
                                    "address_cols": {
                                        "type": ["array", "null"],
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
        # return ValueError(f"Invalid configuration: {e!s}")
        return False
    else:  # no exception
        return True


def update_config(db_path: str | Path, config: dict) -> None:
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


def export_tables(db_path: str | Path, data_path: str | Path) -> None:
    """
    export all tables from database to parquet files in {data_path}/export directory

    Returns: None
    """

    # create export directory if doesn't exist
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    def find_id_cols(row: dict) -> list:  # TODO: check if this is correct
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
            d = conn.execute(links_query).pl().cast({link[1][0]: pl.String, link[1][1]: pl.String})
            d.write_parquet(f"{data_path}/{link[0].replace('.', '_')}.parquet")

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
            d.write_parquet(f"{data_path}/{table[0].replace('.', '_')}.parquet")

    print("Exported all tables!")
    logger.info("Exported all tables!")


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

    return db_conn.fetchone()[0] == 1


def create_config() -> dict:
    """
    Helper to create config file from user input if not pre created
    """

    print("Enter config path. Type 'create' if you would you like to create one.")
    create_config_path = input().strip()
    if create_config_path.lower() != "create":
        while not os.path.exists(create_config_path):
            print("Yaml path does not exist. Please enter a valid path:")
            create_config_path = input().strip()

        config = load_config(create_config_path)

        while True:
            if validate_config(config):
                break
            else:  # invalid config
                # print(validate_config(config))
                print("Invalid config. Please enter a valid yaml config:")
                create_config_path = input().strip()
                config = load_config(create_config_path)

        return config
    else:
        config = {
            "options": {
                "force_db_create": False,
                "export_tables": False,
                "update_config_only": False,
                "link_exclusions": [],
                "bad_address_path": None,
            },
            "schemas": [],
        }
        # build config with user input
        print("Export tables to parquet after load? (y/n)")
        export_tables = input().strip().lower()
        if export_tables == "y":
            config["options"]["export_tables"] = True

        print("[Optional] Provide path to bad address csv file:")
        bad_address_path = input().strip()
        if bad_address_path:
            while not os.path.exists(bad_address_path):
                print("Bad address path does not exist. Please enter a valid path or leave blank:")
                bad_address_path = input().strip()
            config["options"]["bad_address_path"] = bad_address_path

        print("Add a new schema? (y/n)")
        add_schema = input().strip().lower()
        while add_schema == "y":
            config = add_schema_config(config)
            print("Add another schema? (y/n)")
            add_schema = input().strip().lower()

        return config


def add_schema_config(config: dict) -> dict:
    """
    Helper to add a schema to an existing config
    """

    print("Enter the name of the schema:")
    schema_name = input()
    config["schemas"].append({"schema_name": schema_name, "tables": []})
    config = add_table_config(config, schema_name)
    print("Add a table to this schema? (y/n)")
    add_table = input()
    while add_table == "y":
        config = add_table_config(config, schema_name)
        print("Add another table to this schema? (y/n)")
        add_table = input()

    return config


def add_table_config(config: dict, schema_name: str) -> dict:
    """
    Helper to add a table to an existing schema
    """

    print("Enter the name of dataset:")
    table_name = input().lower().replace(" ", "_")
    print("Enter the path to the dataset:")
    table_name_path = input()
    while not os.path.exists(table_name_path):
        print("Path does not exist. Please enter a valid path:")
        table_name_path = input()
    print("Enter the id column of the dataset. Must be unique:")
    id_col = input()
    print("Enter the name column(s) (comma separated):")
    name_cols = input().split(",")
    print("Enter the address column(s) (comma separated):")
    address_cols = input().split(",")

    config["schemas"][schema_name]["tables"].append({
        "table_name": table_name,
        "table_name_path": table_name_path,
        "id_col": id_col,
        "name_cols": name_cols,
        "address_cols": address_cols,
    })

    return config
