import os
import pathlib
from pathlib import Path

import duckdb
import pandas as pd
import typer

from linkage.link.link_generic import (
    create_across_links,
    create_tfidf_across_links,
    create_tfidf_within_links,
    create_within_links,
)
from linkage.link.link_utils import generate_tfidf_links
from linkage.load.load_generic import load_generic
from linkage.utils import console, create_config, export_tables, load_config, logger, update_config

# parent path
DIR = pathlib.Path(__file__).parent

app = typer.Typer()

app = typer.Typer()


def linkage(
    config: dict,
    config_path: str | Path = DIR / "configs/config.yaml",
) -> bool:
    """
    Given a correctly formatted config file,
        * load in any schemas in the config that are not already in the database
        * create within links for each new schema
        * create across links for each new schema with all existing schemas


    Returns true if the database was created successfully.
    """
    probabilistic = config["options"].get("probabilistic", False)
    load_only = config["options"].get("load_only", False)
    db_path = config["options"].get("db_path", DIR / "db/linked.db")

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

    # handle options
    force_db_create = config["options"].get("force_db_create", False)

    update_config_only = config["options"].get("update_config_only", False)
    if update_config_only:
        update_config(db_path, config, config_path)
        return True

    bad_address_path = config["options"].get("bad_address_path", None)
    if bad_address_path is not None:
        try:
            bad_addresses_df = pd.read_csv(bad_address_path, keep_default_na=False)
            bad_addresses_df = bad_addresses_df.iloc[:, 0]
            bad_addresses = bad_addresses_df.tolist()
            bad_addresses.append(" ")
            bad_addresses.append("")
        except Exception:
            bad_addresses = []
    else:
        bad_addresses = []

    # list of link exclusions

    link_exclusions = config["options"].get("link_exclusions", None)
    if not link_exclusions:
        link_exclusions = []

    # all columns in db to compare against
    with duckdb.connect(database=db_path, read_only=False) as con:
        df_db_columns = con.sql("show all tables").df()

    schemas = config["schemas"]
    new_schemas = []

    # load each schema. if schema is a new entity, create links
    for schema_config in schemas:
        schema_name = schema_config["schema_name"]

        # if not force create, check if each col exists, and skip if so
        if not force_db_create:
            for table in schema_config["tables"]:
                # if no existing tables, then empty db_columns
                try:
                    db_columns = (
                        df_db_columns[
                            (df_db_columns["schema"] == schema_name) & (df_db_columns["name"] == table["table_name"])
                        ]["column_names"]
                        .values[0]
                        .tolist()
                    )
                except Exception:
                    db_columns = []

                columns = list(table["name_cols"])
                columns += list(table["address_cols"])

                # if all columns are in df_db_columns then continue
                if not all(col in db_columns for col in columns):
                    new_schemas.append(schema_name)
                else:
                    print(f"Skipping schema {schema_name}")
                    logger.debug(f"Skipping schema {schema_name}")
        else:
            new_schemas.append(schema_name)

    # load in all new schemas
    for new_schema in new_schemas:
        schema_config = [schema for schema in schemas if schema["schema_name"] == new_schema][0]

        with console.status(f"[bold yellow] Working on loading {new_schema}") as status:
            # load schema
            load_generic(
                db_path=db_path,
                schema_config=schema_config,
                bad_addresses=bad_addresses,
            )

        if not load_only:
            # create exact links
            with console.status(f"[bold yellow] Working on linking {new_schema}") as status:
                create_within_links(
                    db_path=db_path,
                    schema_config=schema_config,
                    link_exclusions=link_exclusions,
                )

    if not load_only and probabilistic:
        #  generate all the fuzzy links and store in entity.name_similarity
        # only if there are new schemas added
        if len(new_schemas) > 0:
            with console.status("[bold yellow] Working on fuzzy matching scores") as status:
                generate_tfidf_links(db_path, table_location="entity.name_similarity")
                generate_tfidf_links(
                    db_path, table_location="entity.street_name_similarity", source_table_name="entity.street_name"
                )

        # for across link
        links = []
        created_schemas = []

        # create tfidf links within each new schema
        for new_schema in new_schemas:
            schema_config = [schema for schema in schemas if schema["schema_name"] == new_schema][0]

            if probabilistic:
                with console.status(f"[bold yellow] Working on fuzzy matching links in {new_schema}") as status:
                    create_tfidf_within_links(
                        db_path=db_path,
                        schema_config=schema_config,
                        link_exclusions=link_exclusions,
                    )

            # also create across links for each new schema
            existing_schemas = [schema for schema in schemas if schema["schema_name"] != new_schema]

            new_schema_config = [schema for schema in schemas if schema["schema_name"] == new_schema][0]

            # make sure we havent already created this link combo
            for schema in existing_schemas:
                if sorted(new_schema + schema["schema_name"]) not in created_schemas:
                    links.append((new_schema_config, schema))
                    created_schemas.append(sorted(new_schema + schema["schema_name"]))

        # across links for each new_schema, link across to all existing entities
        for new_schema_config, existing_schema in links:
            with console.status(
                f"[bold yellow] Working on links between {new_schema_config} and {existing_schema}"
            ) as status:
                create_across_links(
                    db_path=db_path,
                    new_schema=new_schema_config,
                    existing_schema=existing_schema,
                    link_exclusions=link_exclusions,
                )

            if probabilistic:
                with console.status(
                    f"[bold yellow] Working on fuzzy links between {new_schema_config} and {existing_schema}"
                ) as status:
                    create_tfidf_across_links(
                        db_path=db_path,
                        new_schema=new_schema_config,
                        existing_schema=existing_schema,
                        link_exclusions=link_exclusions,
                    )

    update_config(db_path, config, config_path)

    export_tables_flag = config["options"].get("export_tables", False)
    if export_tables_flag:
        path = DIR / "data" / "export"
        export_tables(db_path, path)

    return True  ## TODO: check if this is true or false


@app.command()
def main(config: str = typer.Argument(DIR / "config" / "linkage_config.yaml", exists=True, readable=True)) -> None:
    """
    Given a correctly formatted config file,
        * load in any schemas in the config that are not already in the database
        * create within links for each new schema
        * create across links for each new schema with all existing schemas

    Returns 'True' if the database was created successfully.
    """
    config_dict = load_config(config) if config is not None and os.path.exists(config) else create_config()
    linkage(config_dict, config_path=config)

    console.print("[green bold] Linkage complete, database created")
    logger.info("Linkage complete, database created")


if __name__ == "__main__":
    # arg parser
    app()
