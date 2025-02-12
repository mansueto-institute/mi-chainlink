import pandas as pd
from duckdb import DuckDBPyConnection

from woc.create_db.generic_load_link.cleaning.cleaning_functions import (
    clean_address,
    clean_names,
    clean_zipcode,
)
from woc.create_db.generic_load_link.utils import check_table_exists


def load_to_db(df: pd.DataFrame, table_name: str, db_conn, schema: str) -> None:
    """Loads parquet file into table in database.

    Parameters
    ----------
    filepath : str
        Directory of parquet file to load onto database.
    table_name : str
        Name of resulting table in database.
    db_conn : object
        Connection object to desired duckdb database.
    schema : str
        Name of schema for resulting table in database.

    Returns
    -------
    None
    """
    df = df
    query = f"""
            CREATE SCHEMA IF NOT EXISTS {schema};
            DROP TABLE IF EXISTS {schema}.{table_name};
            CREATE TABLE {schema}.{table_name} AS 
               SELECT * 
               FROM df;
            """

    db_conn.execute(query)


def clean_generic(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Cleans the name and address for a generic file. Appends a new
    column with the cleaned name and address.

    Returns a pd.DataFrame
    """

    # Clean the name
    for col in config["name_cols"]:
        # lower snake case
        col = col.lower().replace(" ", "_")

        raw_name = col + "_raw"
        # weird case TODO
        if raw_name in df.columns:
            df.drop(columns=[raw_name], inplace=True)
        df.rename(columns={col: raw_name}, inplace=True)
        df.loc[:, col] = df.loc[:, raw_name].fillna("").str.upper().apply(clean_names)

        # create id col
        id_col_name = col + "_name"
        df[id_col_name] = df[col]

        df = create_id_col(df, id_col_name)

        df.drop(columns=[id_col_name], inplace=True)
    # Clean the address
    for col in config["address_cols"]:
        # lower snake case
        col = col.lower().replace(" ", "_")

        raw_address = col + "_raw"
        temp_address = "temp_" + col

        df[raw_address] = df[col]

        df.loc[:, temp_address] = (
            df.loc[:, raw_address].fillna("").str.upper().apply(clean_address)
        )
        df.reset_index(drop=True, inplace=True)

        df = pd.merge(
            df,
            pd.DataFrame(df.loc[:, temp_address].tolist()).add_prefix(f"{col}_"),
            left_index=True,
            right_index=True,
        )

        # clean zipcode
        df.loc[:, f"{col}_postal_code"] = (
            df.loc[:, f"{col}_postal_code"].astype("str").apply(clean_zipcode)
        )

        # create col for address id
        df[col + "_address"] = df[raw_address]

        id_cols = ["address", "street", "street_name"]

        # create id col
        for id_col in id_cols:
            name = col + "_" + id_col
            df = create_id_col(df, name)

        # drop temp cols
        df = df.drop(columns=[temp_address, col + "_address"])
    return df


def create_id_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Adds an id column to the DataFrame using pd.util.hash_array() function

    Returns a pd.DataFrame
    """

    col_id = col + "_id"
    df[col_id] = pd.util.hash_array(df[col].to_numpy())
    # if col is null then make id null
    df.loc[df[col].isnull(), col_id] = None

    return df


def update_entity_ids(df: pd.DataFrame, entity_id_col: str, db_conn) -> None:
    """
    Adds new ids to the entity schema table. If the value is already in the table, it is not added.

    Returns None
    """

    split_col = entity_id_col.split("_")
    if "street_name_id" == "_".join(split_col[-3:]):
        entity_table_name = "street_name"
        entity_col = entity_id_col.split("_id")[0]
    elif "street_id" == "_".join(split_col[-2:]):
        entity_table_name = "street"
        entity_col = entity_id_col.split("_id")[0]
    elif "address_id" == "_".join(split_col[-2:]):
        entity_col = entity_id_col.replace("_address_id", "")
        entity_table_name = "address"
    elif "name_id" == "_".join(split_col[-2:]):
        entity_col = entity_id_col.replace("_name_id", "")
        entity_table_name = "name"

    if not check_table_exists(db_conn, "entity", entity_table_name):
        # a check if entity tables doesnt exist, just creates it
        query = f"""
                CREATE SCHEMA IF NOT EXISTS entity;

                CREATE TABLE entity.{entity_table_name} AS 
                    SELECT {entity_col} as entity, 
                           {entity_id_col} as {entity_table_name+'_id'} 
                    from  df
                ;
                """

    else:
        # otherwise, add any new entities to the existing table

        query = f"""
                CREATE OR REPLACE TABLE entity.{entity_table_name} AS (


                SELECT {entity_col} as entity, 
                       {entity_id_col} as {entity_table_name+'_id'} 
                from  df

                UNION DISTINCT 

                select entity, 
                       {entity_table_name+'_id'}  
                from   entity.{entity_table_name}
                ) 
                """
    db_conn.execute(query)

    return None


def execute_flag_bad_addresses(
    db_conn: DuckDBPyConnection, table: str, address_col: str, bad_addresses: list
) -> None:
    """
    Flags rows with bad addresses as provided by user
    """
    bad_addresses = tuple(bad_addresses)

    query = f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT *,
                    CASE WHEN 
                        ({address_col} in {bad_addresses} 
                        OR {address_col}_street in {bad_addresses}) THEN 1
                    ELSE 0 END as skip_address
            from {table}
            """

    db_conn.execute(query)

    return None
