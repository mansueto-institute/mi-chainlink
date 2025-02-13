import os

import duckdb
import fire
import pandas as pd
import yaml
from generic_load_link.link.link_generic import (
    create_across_links,
    create_tfidf_across_links,
    create_tfidf_within_links,
    create_within_links,
)
from generic_load_link.link.link_utils import generate_tfidf_links
from generic_load_link.load.load_generic import load_generic
from generic_load_link.utils import (
    export_tables,
    load_config,
    update_config,
)


def main(config_path: str) -> bool:
    """
    Given a correctly formatted config file,
        * load in any schemas in the config that are not already in the database
        * create within links for each new schema
        * create across links for each new schema with all existing schemas


    Returns true if the database was created successfully.
    """

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    try:
        config = load_config(config_path)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML configuration file: {e!s}")
    except Exception as e:
        raise Exception(f"Error loading configuration: {e!s}")

    # handle options
    force_db_create = config["options"]["force_db_create"]
    db_path = config["options"]["db_path"]

    update_config_only = config["options"]["update_config_only"]
    if update_config_only:
        update_config(db_path, config)
        return True

    bad_address_path = config["options"]["bad_address_path"]
    try:
        bad_addresses_df = pd.read_csv(bad_address_path, keep_default_na=False)
        bad_addresses_df = bad_addresses_df.iloc[:, 0]
        bad_addresses = bad_addresses_df.tolist()
        bad_addresses.append(" ")
        bad_addresses.append("")
    except Exception:
        bad_addresses = []

    # list of link exclusions

    link_exclusions = config["options"].get("link_exclusions", [])

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

                columns = [col for col in table["name_cols"]]
                columns += [col for col in table["address_cols"]]

                # if all columns are in df_db_columns then continue
                if not all([col in db_columns for col in columns]):
                    new_schemas.append(schema_name)
                else:
                    print(f"Skipping schema {schema_name}")
        else:
            new_schemas.append(schema_name)

    # load in all new schemas
    for new_schema in new_schemas:
        schema_config = [schema for schema in schemas if schema["schema_name"] == new_schema][0]

        # load schema
        load_generic(
            db_path=db_path,
            schema_config=schema_config,
            bad_addresses=bad_addresses,
        )

        # create exact links
        create_within_links(
            db_path=db_path,
            schema_config=schema_config,
            link_exclusions=link_exclusions,
        )

    #  generate all the fuzzy links and store in entity.name_similarity
    # only if there are new schemas added
    if len(new_schemas) > 0:
        generate_tfidf_links(db_path, table_location="entity.name_similarity")

    # for across link
    links = []
    created_schemas = []

    # create tfidf links within each new schema
    for new_schema in new_schemas:
        schema_config = [schema for schema in schemas if schema["schema_name"] == new_schema][0]

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
        create_across_links(
            db_path=db_path,
            new_schema=new_schema_config,
            existing_schema=existing_schema,
            link_exclusions=link_exclusions,
        )

        create_tfidf_across_links(
            db_path=db_path,
            new_schema=new_schema_config,
            existing_schema=existing_schema,
            link_exclusions=link_exclusions,
        )

    update_config(db_path, config)

    export_tables_flag = config["options"]["export_tables"]
    if export_tables_flag:
        path = config["options"].get("export_tables_path", "/data")
        export_tables(db_path, path)

    return True


if __name__ == "__main__":
    fire.Fire(main)
