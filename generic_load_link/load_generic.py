import pandas as pd
import duckdb
import datetime
import numpy as np

from load_utils import (
    load_to_db,
    clean_generic,
    update_entity_ids,
    execute_flag_bad_addresses
)


def load_generic(db_path:str, schema_config: dict, bad_addresses:list) -> None:
    """
    Loads a generic file into the database.

    Reads config file, loops through each file listed, cleans the data,
    creates a unique id for name, street, and street_name,
    loads into cleaned files into a database using the schema name from the config file,
    and lastly updates the entity name files.

    Returns None.
    """

    schema_name = schema_config["schema_name"]

    with duckdb.connect(db_path, read_only=False) as conn:
        for table_config in schema_config["tables"]:
            # Read the data
            print(f"Data: {table_config['table_name']} -- Reading data")
            file_path = table_config.get("table_name_path")
            if not file_path:
                print(f"Data: {table_config['table_name']} -- No file path given")
                return False

            if file_path.split(".")[-1] == "csv":
                df = pd.read_csv(file_path, dtype='string')
            elif file_path.split(".")[-1] == "parquet":
                df = pd.read_parquet(file_path)
                df = df.astype('string')
            else:
                raise TypeError(
                    f"File type not supported: {file_path.split(".")[-1]}"
                )

            # Clean the data and create ids
            print(
                f"""Data: {table_config['table_name']} -- Starting cleaning at 
                {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
            )

      
            all_columns = []
            all_columns.append(table_config["id_col_og"])
            for col in table_config["name_cols_og"]:
                all_columns.append(col)
            for col in table_config["address_cols_og"]:
                all_columns.append(col)

            #check if columns exist and remove from config if not
        
            for col in all_columns:
                if col not in df.columns:
                    if col in table_config["name_cols_og"]:
                        print(col)
                        table_config["name_cols_og"].remove(col)
                        table_config["name_cols"].remove(col.lower().replace(" ", "_"))
                    elif col in table_config["address_cols_og"]:
                        table_config["address_cols_og"].remove(col)
                        table_config["address_cols"].remove(col.lower().replace(" ", "_"))
                    print(f"Column {col} not found in file {file_path}. Removing from config")


            # Make headers snake case
            df.columns = [x.lower() for x in df.columns]
            df.columns = df.columns.str.replace(" ", "_", regex=True)

            df = clean_generic(df, table_config)


            # load the data to db
            print(
                f"""Data: {table_config['table_name']} -- Starting load at
                {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
            )

            table_name = table_config["table_name"]
            load_to_db(
                df=df,
                table_name=table_name,
                db_conn=conn,
                schema=schema_name,
            )

            # add new names to entity_names table
            print(
                f"""Data: {table_config['table_name']} -- Updating entity name tables at 
                {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
            )

            all_id_cols = ["name_id", "address_id", "street_id", "street_name_id"]

            id_cols = []
            for col in df.columns:
                if (
                    any(c in col for c in all_id_cols)
                    and "subaddress_identifier" not in col
                ):
                    id_cols.append(col)

            for col in id_cols:
                update_entity_ids(df=df, entity_id_col=col, db_conn=conn)

            # create bad address flag
            for col in table_config["address_cols"]:
                execute_flag_bad_addresses(
                    db_conn=conn,
                    table=f"{schema_name}.{table_name}",
                    address_col=col,
                    bad_addresses=bad_addresses,
                )


if __name__ == "__main__":
    load_generic("load_config.json")
