import polars as pl
from duckdb import DuckDBPyConnection

from chainlink.cleaning.cleaning_functions import (
    clean_address_batch_parser,
    clean_names,
    clean_zipcode,
)
from chainlink.utils import check_table_exists, console


def load_to_db(df: pl.DataFrame, table_name: str, db_conn: DuckDBPyConnection, schema: str) -> None:
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


def clean_generic(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """
    Cleans the name and address for a generic file. Appends a new
    column with the cleaned name and address.

    Returns a pl.DataFrame
    """

    # Clean the name
    for col in config["name_cols"]:
        # lower snake case
        col = col.lower().replace(" ", "_")

        raw_name = col + "_raw"
        id_col_name = col + "_name"

        # weird case TODO
        if raw_name in df.columns:
            df = df.drop(columns=[raw_name])
        df = (
            df.rename({col: raw_name})
            .with_columns(
                pl.col(raw_name)
                .fill_null("")
                .str.to_uppercase()
                .map_elements(clean_names, return_dtype=pl.String)
                .alias(col)
            )
            .with_columns(pl.col(col).alias(id_col_name))
        )

        df = create_id_col(df, id_col_name)
        df = df.drop(id_col_name)

    # Clean the address
    if config.get("address_cols"):
        for col in config["address_cols"]:
            # lower snake case
            col = col.lower().replace(" ", "_")

            raw_address = col + "_raw"
            temp_address = "temp_" + col
            console.log(f"[yellow] Cleaning address column {col}")

            df = df.with_columns(
                pl.col(col).alias(raw_address), pl.col(col).fill_null("").str.to_uppercase().alias(temp_address)
            )

            lookup = (
                df.select(pl.col(temp_address).alias("temp_address"))
                # select only the address column
                # drop duplicates at the engine level
                .unique()
                # for each batch of ~batch_size rows, call our batch parser
                .with_columns(
                pl.col("temp_address").map_batches(
                    clean_address_batch_parser,
                    return_dtype=pl.Struct([
                        pl.Field("raw", pl.String),
                        pl.Field("address_number", pl.String),
                        pl.Field("street_pre_directional", pl.String),
                        pl.Field("street_name", pl.String),
                        pl.Field("street_post_type", pl.String),
                        pl.Field("unit_type", pl.String),
                        pl.Field("unit_number", pl.String),
                        pl.Field("subaddress_type", pl.String),
                        pl.Field("subaddress_identifier", pl.String),
                        pl.Field("city", pl.String),
                        pl.Field("state", pl.String),
                        pl.Field("postal_code", pl.String),
                        pl.Field("street", pl.String),
                    ]),
                ).alias("parsed")
                )
                .drop("temp_address")
                .rename({"parsed": temp_address})
                .unnest(temp_address)
                .select(
                    pl.col("raw").alias(raw_address),
                    pl.struct(pl.exclude("raw")).alias(temp_address),
                )
            )
            df = df.drop(temp_address).join(lookup, on=raw_address, how="left")
            ta_fields = df[temp_address].struct.fields
            new_fields = [f"{col}_{f}" for f in ta_fields]
            df = (
                df.with_columns(pl.col(temp_address).struct.rename_fields(new_fields))
                .unnest(temp_address)
                .with_columns(
                    pl.col(f"{col}_postal_code").cast(pl.String).map_elements(clean_zipcode, return_dtype=pl.String),
                    pl.col(raw_address).alias(col + "_address"),
                )
            )

            id_cols = ["address", "street", "street_name"]

            # create id col
            for id_col in id_cols:
                name = col + "_" + id_col
                df = create_id_col(df, name)

        # drop temp cols
        df = df.drop(col + "_address")
    return df


def create_id_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Adds an id column to the DataFrame using pl.Series.hash function

    Returns a pl.DataFrame
    """

    col_id = col + "_id"

    df = df.with_columns(pl.col(col).hash().alias(col_id)).with_columns(
        pl.when(pl.col(col).is_null()).then(None).otherwise(pl.col(col_id)).alias(col_id).cast(pl.UInt64),
    )

    return df


def update_entity_ids(df: pl.DataFrame, entity_id_col: str, db_conn: DuckDBPyConnection) -> None:
    """
    Adds new ids to the entity schema table. If the value is already in the table, it is not added.

    Returns None
    """

    split_col = entity_id_col.split("_")
    if "_".join(split_col[-3:]) == "street_name_id":
        entity_table_name = "street_name"
        entity_col = entity_id_col.split("_id")[0]
    elif "_".join(split_col[-2:]) == "street_id":
        entity_table_name = "street"
        entity_col = entity_id_col.split("_id")[0]
    elif "_".join(split_col[-2:]) == "address_id":
        entity_col = entity_id_col.replace("_address_id", "")
        entity_table_name = "address"
    elif "_".join(split_col[-2:]) == "name_id":
        entity_col = entity_id_col.replace("_name_id", "")
        entity_table_name = "name"

    if not check_table_exists(db_conn, "entity", entity_table_name):
        # a check if entity tables doesnt exist, just creates it
        query = f"""
                CREATE SCHEMA IF NOT EXISTS entity;

                CREATE TABLE entity.{entity_table_name} AS
                    SELECT {entity_col} as entity,
                           {entity_id_col} as {entity_table_name + "_id"}
                    from  df
                ;
                """

    else:
        # otherwise, add any new entities to the existing table

        query = f"""
                CREATE OR REPLACE TABLE entity.{entity_table_name} AS (


                SELECT {entity_col} as entity,
                       {entity_id_col} as {entity_table_name + "_id"}
                from  df

                UNION DISTINCT

                select entity,
                       {entity_table_name + "_id"}
                from   entity.{entity_table_name}
                )
                """
    db_conn.execute(query)

    return None


def execute_flag_bad_addresses(db_conn: DuckDBPyConnection, table: str, address_col: str, bad_addresses: list) -> None:
    """
    Flags rows with bad addresses as provided by user
    """
    console.log(f"[yellow] Flagging bad addresses in {table} table for {address_col} column")
    if bad_addresses:
        bad_addresses_tuple = tuple(bad_addresses)

        query = f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT *,
                        CASE WHEN
                            ({address_col} in {bad_addresses_tuple}
                            OR {address_col}_street in {bad_addresses_tuple}) THEN 1
                        ELSE 0 END as {address_col}_skip
                from {table}
                """

    else:
        query = f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT *, 0 as {address_col}_skip
            from {table}
            """
        console.log(f"[yellow] No bad addresses to flag in {table} table for {address_col} column")

    db_conn.execute(query)
    return None


def validate_input_data(df: pl.DataFrame, table_config: dict) -> None:
    """
    Validates input data against configuration requirements
    """
    required_columns = set()
    required_columns.add(table_config["id_col_og"])
    if table_config.get("name_cols_og"):
        required_columns.update(table_config["name_cols_og"])
    if table_config.get("address_cols_og"):
        required_columns.update(table_config["address_cols_og"])

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Check for empty dataframe
    if df.is_empty():
        raise ValueError("Input data is empty")

    # Check for minimum required non-null values
    for col in required_columns:
        null_count = df.select(pl.col(col).is_null().sum()).item()
        if null_count == len(df):
            raise ValueError(f"Column {col} contains all null values")
