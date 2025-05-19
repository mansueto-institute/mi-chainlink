import datetime
import logging
import os
import readline  # noqa: F401
from pathlib import Path

import duckdb
import jsonschema
import polars as pl
import yaml
from duckdb import DuckDBPyConnection
from rich.console import Console
from rich.prompt import Confirm, Prompt

console = Console(color_system="auto")


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


logger = setup_logger("chainlink", "chainlink.log")


def load_config(file_path: str) -> dict:
    """
    load yaml config file, clean up column names

    Returns: dict
    """

    with open(file_path) as file:
        config = yaml.safe_load(file)

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
        console.print(f"[bold red]> Invalid configuration: {e!s}")
        return False
    else:  # no exception
        return True


def update_config(db_path: str | Path, config: dict, config_path: str | Path) -> None:
    """
    update config by adding in all existing link columns and last updated time.
    writes config back out to config.yaml

    Returns: None
    """

    with duckdb.connect(db_path) as conn:
        df_db_columns = conn.sql("show all tables").pl()

    all_links = []
    for cols in df_db_columns["column_names"].to_list():
        all_links += [col for col in cols if "match" in col]

    if "metadata" not in config:
        config["metadata"] = {}

    config["metadata"]["existing_links"] = all_links
    config["metadata"]["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(config_path, "w+") as f:
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
    create_config_path = Prompt.ask(
        "[green]> Enter config path. [Leave blank if you would you like to create a new one]",
        default="",
        show_default=False,
    )
    create_config_path = create_config_path.strip()
    if create_config_path.lower() != "":
        while not os.path.exists(create_config_path):
            create_config_path = Prompt.ask("[red]> Yaml path does not exist. Please enter a valid path")
            create_config_path = create_config_path.strip()

        config = load_config(create_config_path)

        while True:
            if validate_config(config):
                break
            else:  # invalid config
                # print(validate_config(config))
                create_config_path = Prompt.ask("[red]> Invalid config. Please enter a valid yaml config")
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
                "probabilistic": False,
                "load_only": False,
            },
            "schemas": [],
        }
        # build config with user input
        config["options"]["db_path"] = Prompt.ask(
            "[green]> Enter the path to the resulting database", default="db/linked.db", show_default=True
        )

        config["options"]["load_only"] = Confirm.ask(
            "[green]> Only clean and load data to the database (without matching)?", show_default=True, default=False
        )

        if not config["options"]["load_only"]:
            config["options"]["probablistic"] = Confirm.ask(
                "[green]> Run probabilisitic name and address matching?", show_default=True, default=False
            )

        config["options"]["export_tables"] = Confirm.ask(
            "[green]> Export tables to parquet after load?", show_default=True, default=False
        )

        bad_address_path = Prompt.ask(
            "[dim green]> [Optional] Provide path to bad address csv file", default="", show_default=False
        )
        bad_address_path = bad_address_path.strip()
        if bad_address_path:
            while not os.path.exists(bad_address_path):
                console.print("> Bad address path does not exist. Please enter a valid path or leave blank:")
                bad_address_path = input().strip()
            config["options"]["bad_address_path"] = bad_address_path

        add_schema = Confirm.ask("[green]> Add a new schema?", default=True, show_default=True)
        while add_schema:
            config = add_schema_config(config)
            add_schema = Confirm.ask("> Add another schema?", default=False, show_default=True)

        return config


def add_schema_config(config: dict) -> dict:
    """
    Helper to add a schema to an existing config
    """

    schema_name = Prompt.ask("[green]> Enter the name of the schema", default="main")
    config["schemas"].append({"schema_name": schema_name, "tables": []})
    config = add_table_config(config, schema_name)
    add_table = Confirm.ask("[green]> Add a table to this schema?", default=True, show_default=True)
    while add_table:
        config = add_table_config(config, schema_name)
        add_table = Confirm.ask("[green]> Add another table to this schema?", default=False, show_default=True)
    console.print("[green italic]> Schema added successfully!")
    return config


def add_table_config(config: dict, schema_name: str) -> dict:
    """
    Helper to add a table to an existing schema
    """

    table_name = Prompt.ask("[green]> Enter the name of dataset:", default="dataset", show_default=True)
    table_name = table_name.lower().replace(" ", "_")
    table_name_path = Prompt.ask("[green]> Enter the path to the dataset")
    while not os.path.exists(table_name_path):
        table_name_path = Prompt.ask("[red]> Path does not exist. Please enter a valid path")
    id_col = Prompt.ask("[green]> Enter the id column of the dataset. Must be unique")
    name_col_str = Prompt.ask("[green]> Enter the name column(s) (comma separated)")
    name_cols = [_.strip() for _ in name_col_str.split(",")]
    address_col_str = Prompt.ask("[green]> Enter the address column(s) (comma separated)")
    address_cols = [_.strip() for _ in address_col_str.split(",")]

    for idx, schema in enumerate(config["schemas"]):
        if schema["schema_name"] == schema_name:
            config["schemas"][idx]["tables"].append({
                "table_name": table_name,
                "table_name_path": table_name_path,
                "id_col": id_col,
                "name_cols": name_cols,
                "address_cols": address_cols,
            })

    return config
