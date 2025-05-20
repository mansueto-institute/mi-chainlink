import itertools
from pathlib import Path
from typing import Optional

import duckdb
from duckdb import DuckDBPyConnection

from chainlink.link.tfidf_utils import database_query, superfast_tfidf
from chainlink.utils import console, logger


def execute_match(
    db_path: str | Path,
    match_type: str,
    left_entity: str,
    left_table: str,
    left_matching_col: str,
    left_matching_id: str,
    left_ent_id: str,
    right_entity: str,
    right_table: str,
    right_matching_col: str,
    right_matching_id: str,
    right_ent_id: str,
    skip_address: bool = False,
    link_exclusions: Optional[list] = None,
) -> None:
    """
    Exact matches between two column in two tables.
    Creates a match column called
    {left_entity}_{left_table}_{left_matching_col}_{right_entity}_{right_table}_{right_matching_col}_{match_type}
    and appends to link table link.{left_entity}_{right_entity}

    Returns: None
    """
    if link_exclusions is None:
        link_exclusions = []

    # if two different ids just dont want duplicates
    matching_condition = "!="

    if left_ent_id == right_ent_id and left_entity == right_entity:
        left_ent_id_edit = f"{left_ent_id}_1"
        right_ent_id_edit = f"{right_ent_id}_2"
        # if same id, only want one direction of matches
        matching_condition = "<"
    else:
        left_ent_id_edit = left_ent_id
        right_ent_id_edit = right_ent_id

    link_table = f"link.{left_entity}_{right_entity}"

    # align the names of the match columns
    left_side = f"{left_entity}_{left_table}_{left_matching_col}"
    right_side = f"{right_entity}_{right_table}_{right_matching_col}"
    if left_side < right_side:
        match_name_col = f"{left_side}_{right_side}_{match_type}"
    else:
        match_name_col = f"{right_side}_{left_side}_{match_type}"

    # check link exclusion
    if any(exclusion in match_name_col for exclusion in link_exclusions):
        return None

    if skip_address:
        address_condition = " != 1"
        left_address_condition = f"l.{left_matching_col}_skip {address_condition}"
        right_address_condition = f"r.{right_matching_col}_skip {address_condition}"
        left_extra_col = f", {left_matching_col}_skip"
        right_extra_col = f", {right_matching_col}_skip"
    else:
        left_address_condition = "TRUE"
        right_address_condition = "TRUE"
        left_extra_col = ""
        right_extra_col = ""

    temp_table = match_name_col + "_table"

    matching_query = f"""
            CREATE SCHEMA IF NOT EXISTS link;

            CREATE OR REPLACE TABLE link.{temp_table} AS
            SELECT l.{left_entity}_{left_ent_id_edit},
                   r.{right_entity}_{right_ent_id_edit},
                   1 AS {match_name_col}
            FROM
                (SELECT {left_ent_id} AS {left_entity}_{left_ent_id_edit},
                        {left_matching_id} {left_extra_col}
                FROM {left_entity}.{left_table}
                ) as l
            JOIN
                (SELECT {right_ent_id} AS {right_entity}_{right_ent_id_edit},
                        {right_matching_id} {right_extra_col}
                FROM {right_entity}.{right_table}
                ) as r
                ON l.{left_matching_id} = r.{right_matching_id}
                AND l.{left_matching_id} IS NOT NULL
                AND r.{right_matching_id} IS NOT NULL
                AND l.{left_entity}_{left_ent_id_edit} {matching_condition} r.{right_entity}_{right_ent_id_edit}
            WHERE
                {left_address_condition}
                AND {right_address_condition}
        ;"""

    with duckdb.connect(database=db_path, read_only=False) as db_conn:
        db_conn.execute("""
        SET memory_limit = '175GB';
        SET threads = 4;
        """)
        db_conn.execute(matching_query)
        console.log(f"[yellow] Created {match_name_col}")
        logger.debug(f"Created {match_name_col}")

        execute_match_processing(
            db_conn=db_conn,
            link_table=link_table,
            out_temp_table_name=temp_table,
            id_col_1=f"{left_entity}_{left_ent_id_edit}",
            match_name_col=match_name_col,
            id_col_2=f"{right_entity}_{right_ent_id_edit}",
        )
        logger.debug(f"Finished match processing for {match_name_col}")

    return None


def execute_match_address(
    db_path: str | Path,
    left_entity: str,
    left_table: str,
    left_address: str,
    left_ent_id: str,
    right_entity: str,
    right_table: str,
    right_address: str,
    right_ent_id: str,
    skip_address: bool = False,
    link_exclusions: Optional[list] = None,
) -> None:
    """
    given a two address columns, match the addresses:
        * match by raw address string
        * match by clean street string
        * if street id matches, match by unit
        * match street name and number if zipcode matches

    Creates four match columns called
    {left_entity}_{left_table}_{left_matching_col}_{right_entity}_{right_table}_{right_matching_col}_{match_type}
    and appends to link table link.{left_entity}_{right_entity}



    Returns: None
    """
    if link_exclusions is None:
        link_exclusions = []

    ## Match by raw address string and by street id
    for match in ["street", "address"]:
        logger.debug(f"Executing {match} match")
        execute_match(
            db_path=db_path,
            match_type=f"{match}_match",
            left_entity=left_entity,
            left_table=left_table,
            left_matching_col=left_address,
            left_matching_id=f"{left_address}_{match}_id",
            left_ent_id=left_ent_id,
            right_entity=right_entity,
            right_table=right_table,
            right_matching_col=right_address,
            right_matching_id=f"{right_address}_{match}_id",
            right_ent_id=right_ent_id,
            skip_address=skip_address,
            link_exclusions=link_exclusions,
        )

    ## If street id matches, match by unit
    left_side = f"{left_entity}_{left_table}_{left_address}"
    right_side = f"{right_entity}_{right_table}_{right_address}"
    if left_side < right_side:
        street_match_to_check = f"{left_side}_{right_side}_street_match"
    else:
        street_match_to_check = f"{right_side}_{left_side}_street_match"

    execute_match_unit(
        db_path=db_path,
        left_entity=left_entity,
        right_entity=right_entity,
        # TODO will ording of left and right address mess things up
        street_match_to_check=street_match_to_check,
        left_table=left_table,
        left_address=left_address,
        left_ent_id=left_ent_id,
        right_table=right_table,
        right_address=right_address,
        right_ent_id=right_ent_id,
        skip_address=skip_address,
        link_exclusions=link_exclusions,
    )
    ## match street name and number if zipcode matches
    execute_match_street_name_and_num(
        db_path=db_path,
        left_entity=left_entity,
        right_entity=right_entity,
        left_table=left_table,
        left_address=left_address,
        left_ent_id=left_ent_id,
        right_table=right_table,
        right_address=right_address,
        right_ent_id=right_ent_id,
        skip_address=skip_address,
        link_exclusions=link_exclusions,
    )


# EXECUTE MATCH HELPERS


def execute_match_processing(
    db_conn: DuckDBPyConnection,
    link_table: str,
    out_temp_table_name: str,
    id_col_1: str,
    match_name_col: str,
    id_col_2: str,
) -> None:
    """
    Steps to run after matches are created.
    append matches to link table, set null matches to 0, and drop temp table of matches
    runs in execute_match()

    Returns: None
    """
    # check if link table exists
    link_table_check = db_conn.execute(
        f"""SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{link_table.split(".")[1]}'
               and table_schema = '{link_table.split(".")[0]}'"""
    ).fetchone()[0]

    link_table_exists = link_table_check != 0

    # append to link table
    db_conn.execute(query_append_to_links(link_table_exists, link_table, out_temp_table_name, id_col_1, id_col_2))

    # set null matches to 0
    db_conn.execute(f"UPDATE {link_table} SET {match_name_col} = 0 WHERE {match_name_col} IS NULL")

    cols = [row[1] for row in db_conn.execute(f"PRAGMA table_info('{link_table}')").fetchall()]
    print(cols)
    for col in cols:
        db_conn.execute(f"UPDATE {link_table} SET {col} = 0 WHERE {col} IS NULL")

    # set datatype to int or float as expected
    if "fuzzy" in match_name_col:
        db_conn.execute(f"UPDATE {link_table} SET {match_name_col} = CAST({match_name_col} AS FLOAT)")
    else:
        db_conn.execute(f"UPDATE {link_table} SET {match_name_col} = CAST({match_name_col} AS INT1)")

    # drop temp table of matches
    db_conn.execute(f"DROP TABLE link.{out_temp_table_name}")


def query_append_to_links(
    link_table_exists: bool,
    link_table: str,
    table_to_append: str,
    id_col1: str,
    id_col2: str,
) -> str:
    """
    query to append links to link table
    runs in execute_match_processing()
    """

    # if link table does not exist then its just table to append
    if link_table_exists:
        query = f"""
        CREATE OR REPLACE TABLE {link_table} AS
        SELECT DISTINCT *
        FROM {link_table}
        FULL JOIN link.{table_to_append}
        USING({id_col1}, {id_col2})"""

    else:
        query = f"""
        CREATE OR REPLACE TABLE {link_table} AS
        SELECT DISTINCT *
        FROM link.{table_to_append}"""

    return query


def execute_match_unit(
    db_path: str | Path,
    left_entity: str,
    right_entity: str,
    street_match_to_check: str,
    left_table: str,
    left_address: str,
    left_ent_id: str,
    right_table: str,
    right_address: str,
    right_ent_id: str,
    skip_address: bool = False,
    link_exclusions: Optional[list] = None,
) -> None:
    """
    Given two address columns, if street id matches, match by unit.

    Creates a match column called
    {left_entity}_{left_table}_{left_address}_{right_entity}_{right_table}_{right_address}_unit_match
    and appends to link table link.{left_entity}_{right_entity}
    """
    if link_exclusions is None:
        link_exclusions = []

    # if same id, only want one direction of matches
    if left_ent_id == right_ent_id and left_entity == right_entity:
        left_ent_id_edit = f"{left_ent_id}_1"
        right_ent_id_edit = f"{right_ent_id}_2"
    else:
        left_ent_id_edit = left_ent_id
        right_ent_id_edit = right_ent_id

    link_table = f"link.{left_entity}_{right_entity}"

    # align the names of the match columns
    left_side = f"{left_entity}_{left_table}_{left_address}"
    right_side = f"{right_entity}_{right_table}_{right_address}"
    if left_side < right_side:
        match_name_col = f"{left_side}_{right_side}_unit_match"
    else:
        match_name_col = f"{right_side}_{left_side}_unit_match"

    # check link exclusion
    if any(exclusion in match_name_col for exclusion in link_exclusions):
        return None

    if skip_address:
        address_condition = " != 1"
        left_address_condition = f"{left_address}_skip {address_condition}"
        right_address_condition = f"{right_address}_skip {address_condition}"
    else:
        left_address_condition = "TRUE"
        right_address_condition = "TRUE"

    temp_table = match_name_col + "_table"

    matching_query = f"""

        CREATE OR REPLACE TABLE link.{temp_table} AS

        WITH link as (
            SELECT {left_entity}_{left_ent_id_edit},
                    {right_entity}_{right_ent_id_edit}
            FROM {link_table}
            WHERE {street_match_to_check} = 1
            )

        ,lhs as (
            SELECT {left_ent_id} AS {left_entity}_{left_ent_id_edit},
                    {left_address}_unit_number AS unit_1
            FROM {left_entity}.{left_table}
            where {left_address_condition}

            )

        , rhs as (
            SELECT {right_ent_id} AS {right_entity}_{right_ent_id_edit},
                    {right_address}_unit_number AS unit_2
            FROM {right_entity}.{right_table}
            where {right_address_condition}
            )

        SELECT {left_entity}_{left_ent_id_edit},
               {right_entity}_{right_ent_id_edit},
               1 AS {match_name_col}
        FROM link
        LEFT JOIN lhs
        USING({left_entity}_{left_ent_id_edit})
        LEFT JOIN rhs
        USING({right_entity}_{right_ent_id_edit})
        WHERE unit_1 IS NOT NULL
        AND unit_2 IS NOT NULL
        AND CAST(unit_1 AS VARCHAR) = CAST(unit_2 AS VARCHAR);"""

    with duckdb.connect(database=db_path, read_only=False) as db_conn:
        db_conn.execute(matching_query)
        console.log(f"[yellow] Created {match_name_col}")
        logger.debug(f"Created {match_name_col}")

        execute_match_processing(
            db_conn=db_conn,
            link_table=link_table,
            out_temp_table_name=temp_table,
            id_col_1=f"{left_entity}_{left_ent_id_edit}",
            match_name_col=match_name_col,
            id_col_2=f"{right_entity}_{right_ent_id_edit}",
        )

    return None


def execute_match_street_name_and_num(
    db_path: str | Path,
    left_entity: str,
    left_table: str,
    left_address: str,
    left_ent_id: str,
    right_entity: str,
    right_table: str,
    right_address: str,
    right_ent_id: str,
    skip_address: bool = False,
    link_exclusions: Optional[list] = None,
) -> None:
    """
    Match street name and number if zipcode matches.

    Creates a match column called
    {left_entity}_{left_table}_{left_address}_{right_entity}_{right_table}_{right_address}_street_num_match
    and appends to link table link.{left_entity}_{right_entity}
    """
    if link_exclusions is None:
        link_exclusions = []

    # if two different ids just dont want duplicates
    matching_condition = "!="

    if left_ent_id == right_ent_id and left_entity == right_entity:
        left_ent_id_edit = f"{left_ent_id}_1"
        right_ent_id_edit = f"{right_ent_id}_2"
        # if same id, only want one direction of matches
        matching_condition = "<"
    else:
        left_ent_id_edit = left_ent_id
        right_ent_id_edit = right_ent_id

    link_table = f"link.{left_entity}_{right_entity}"
    # align the names of the match columns
    left_side = f"{left_entity}_{left_table}_{left_address}"
    right_side = f"{right_entity}_{right_table}_{right_address}"
    if left_side < right_side:
        match_name_col = f"{left_side}_{right_side}_street_num_match"
    else:
        match_name_col = f"{right_side}_{left_side}_street_num_match"

    # check link exclusion
    if any(exclusion in match_name_col for exclusion in link_exclusions):
        return None

    if skip_address:
        address_condition = " != 1"
        left_address_condition = f"{left_address}_skip {address_condition}"
        right_address_condition = f"{right_address}_skip {address_condition}"
    else:
        left_address_condition = "TRUE"
        right_address_condition = "TRUE"

    temp_table = match_name_col + "_table"

    matching_query = f"""CREATE OR REPLACE TABLE link.{temp_table} AS

            with lhs as (
                SELECT {left_ent_id} AS {left_entity}_{left_ent_id_edit},
                        {left_address}_postal_code,
                        {left_address}_address_number,
                        {left_address}_street_name_id
                FROM {left_entity}.{left_table}
                WHERE {left_address}_address_number IS NOT NULL
                AND {left_address}_street_name_id IS NOT NULL
                and {left_address_condition}
            )

            , rhs as (
                SELECT {right_ent_id} AS {right_entity}_{right_ent_id_edit},
                        {right_address}_postal_code,
                        {right_address}_address_number,
                        {right_address}_street_name_id
                FROM {right_entity}.{right_table}
                WHERE {right_address}_address_number IS NOT NULL
                AND {right_address}_street_name_id IS NOT NULL
                and {right_address_condition}
            )

            ,final as (
            SELECT DISTINCT lhs.{left_entity}_{left_ent_id_edit},
                            rhs.{right_entity}_{right_ent_id_edit},
                            1 AS {match_name_col}
            FROM lhs
            INNER JOIN rhs
            on lhs.{left_address}_postal_code = rhs.{right_address}_postal_code
            and lhs.{left_address}_address_number = rhs.{right_address}_address_number
            and lhs.{left_address}_street_name_id = rhs.{right_address}_street_name_id
            )

            SELECT *
            from final
            WHERE {left_entity}_{left_ent_id_edit} {matching_condition} {right_entity}_{right_ent_id_edit};"""

    with duckdb.connect(database=db_path, read_only=False) as db_conn:
        db_conn.execute(matching_query)
        console.log(f"[yellow] Created {match_name_col}")
        logger.debug(f"Created {match_name_col}")

        execute_match_processing(
            db_conn=db_conn,
            link_table=link_table,
            out_temp_table_name=temp_table,
            id_col_1=f"{left_entity}_{left_ent_id_edit}",
            match_name_col=match_name_col,
            id_col_2=f"{right_entity}_{right_ent_id_edit}",
        )

    return None


# FUZZY MATCHING UTILS


def generate_tfidf_links(
    db_path: str | Path, table_location: str = "entity.name_similarity", source_table_name: str | None = None
) -> None:
    """
    create a table of tfidf matches between two entities and adds to db

    Returns: None
    """

    console.log("[yellow] Process started")
    logger.info("Process started")

    # retrieve entity list, print length of dataframe
    entity_list = database_query(db_path, table_name=source_table_name)
    console.log(f"[yellow] Query retrieved {len(entity_list)} rows")
    logger.debug(f"Query retrieved {len(entity_list)} rows")

    # returns a pandas df
    entity_col = entity_list.columns[0]
    id_col = entity_list.columns[1]
    matches_df = superfast_tfidf(entity_list, id_col, entity_col)

    console.log("[yellow] Fuzzy Matching done")
    logger.info("Fuzzy Matching done")

    # load back to db
    with duckdb.connect(database=db_path, read_only=False) as db_conn:
        query = f"""CREATE OR REPLACE TABLE {table_location} AS
                    SELECT *
                    FROM  matches_df"""

        db_conn.execute(query)


def execute_fuzzy_link(
    db_path: str | Path,
    left_entity: str,
    left_table: str,
    left_ent_id: str,
    left_name_col: str,
    right_entity: str,
    right_table: str,
    right_ent_id: str,
    right_name_col: str,
    tfidf_table: str = "link.tfidf_staging",
    link_exclusions: Optional[list] = None,
) -> None:
    """

    Given two tables and a tfidf matching entity table, create a fuzzy match between the two tables.
    Creates a match column called
    {left_entity}_{left_table}_{left_name_col}_{right_entity}_{right_table}_{right_name_col}_fuzzy_match
    and appends to link table link.{left_entity}_{right_entity}
    """
    if link_exclusions is None:
        link_exclusions = []

    link_table = f"link.{left_entity}_{right_entity}"

    # align the names of the match columns
    left_side = f"{left_entity}_{left_table}_{left_name_col}"
    right_side = f"{right_entity}_{right_table}_{right_name_col}"
    if left_side < right_side:
        match_name = f"{left_side}_{right_side}_fuzzy_match"
    else:
        match_name = f"{right_side}_{left_side}_fuzzy_match"

    # check link exclusion
    if any(exclusion in match_name for exclusion in link_exclusions):
        return None

    same_condition = "TRUE"

    if left_ent_id == right_ent_id and left_entity == right_entity:
        left_ent_id_rename = f"{left_ent_id}_1"
        right_ent_id_rename = f"{right_ent_id}_2"
        # if same id, want to remove dupes
        same_condition = f"{left_entity}_{left_ent_id_rename} < {right_entity}_{right_ent_id_rename}"
    else:
        left_ent_id_rename = left_ent_id
        right_ent_id_rename = right_ent_id

    query = f"""
    CREATE OR REPLACE TABLE {link_table} AS

    WITH tfidf_matches AS (
        SELECT id_a,
               id_b,
               similarity as {match_name}
        FROM {tfidf_table}
    ),

    left_source AS (
        SELECT {left_ent_id} as {left_entity}_{left_ent_id_rename},
                {left_name_col}_name_id
        FROM {left_entity}.{left_table}
    ),

    right_source AS (
        SELECT {right_ent_id} as {right_entity}_{right_ent_id_rename},
               {right_name_col}_name_id
        FROM {right_entity}.{right_table}
    ),

    fuzzy_match_1 AS (
        SELECT l.{left_entity}_{left_ent_id_rename},
               r.{right_entity}_{right_ent_id_rename},
               m.{match_name}
        FROM   tfidf_matches as m
        INNER JOIN left_source as l
            ON m.id_a = l.{left_name_col}_name_id
        INNER JOIN right_source as r
            ON m.id_b = r.{right_name_col}_name_id
    ),

    fuzzy_match_2 AS (
        SELECT l.{left_entity}_{left_ent_id_rename},
               r.{right_entity}_{right_ent_id_rename},
               m.{match_name}
        FROM   tfidf_matches as m
        INNER JOIN left_source as l
            ON m.id_b = l.{left_name_col}_name_id
        INNER JOIN right_source as r
            ON m.id_a = r.{right_name_col}_name_id
    ),

    all_fuzzy_matches AS (
        SELECT *
        FROM (SELECT * FROM fuzzy_match_1
              UNION
              SELECT * FROM fuzzy_match_2)
        where {same_condition}
    ),

    existing_links AS (
        SELECT *
        FROM {link_table}
    )

    SELECT *
    FROM   all_fuzzy_matches
    FULL JOIN existing_links
        USING({left_entity}_{left_ent_id_rename},{right_entity}_{right_ent_id_rename})

    """

    with duckdb.connect(database=db_path, read_only=False) as db_conn:
        db_conn.execute(query)
        console.log(f"[yellow] Created {match_name}")
        logger.debug(f"Created {match_name}")
        cols = [row[1] for row in db_conn.execute(f"PRAGMA table_info('{link_table}')").fetchall()]
        for col in cols:
            db_conn.execute(f"UPDATE {link_table} SET {col} = 0 WHERE {col} IS NULL")

        # set datatype to int or float as expected
        if "fuzzy" in match_name:
            db_conn.execute(f"UPDATE {link_table} SET {match_name} = CAST({match_name} AS FLOAT)")
        else:
            db_conn.execute(f"UPDATE {link_table} SET {match_name} = CAST({match_name} AS INT1)")

    return None


def execute_address_fuzzy_link(
    db_path: str | Path,
    left_entity: str,
    left_table: str,
    left_ent_id: str,
    left_address_col: str,
    right_entity: str,
    right_table: str,
    right_ent_id: str,
    right_address_col: str,
    tfidf_table: str = "link.tfidf_staging",
    skip_address: bool = False,
    link_exclusions: Optional[list] = None,
) -> None:
    """
    fuzzy address matching
    """
    if link_exclusions is None:
        link_exclusions = []

    link_table = f"link.{left_entity}_{right_entity}"

    # align the names of the match columns
    left_side = f"{left_entity}_{left_table}_{left_address_col}"
    right_side = f"{right_entity}_{right_table}_{right_address_col}"
    match_name_stem = f"{left_side}_{right_side}" if left_side < right_side else f"{right_side}_{left_side}"

    same_condition = "TRUE"

    if left_ent_id == right_ent_id and left_entity == right_entity:
        left_ent_id_rename = f"{left_ent_id}_1"
        right_ent_id_rename = f"{right_ent_id}_2"
        left_unit_num_rename = f"{left_address_col}_unit_number_1"
        right_unit_num_rename = f"{right_address_col}_unit_number_2"
        left_address_num_rename = f"{left_address_col}_address_number_1"
        right_address_num_rename = f"{right_address_col}_address_number_2"
        left_postal_code_rename = f"{left_address_col}_postal_code_1"
        right_postal_code_rename = f"{right_address_col}_postal_code_2"
        left_directional_rename = f"{left_address_col}_street_pre_directional_1"
        right_directional_rename = f"{right_address_col}_street_pre_directional_2"
        # if same id, want to remove dupes
        same_condition = f"{left_entity}_{left_ent_id_rename} < {right_entity}_{right_ent_id_rename}"
    else:
        left_ent_id_rename = left_ent_id
        right_ent_id_rename = right_ent_id
        left_unit_num_rename = f"{left_address_col}_unit_number"
        right_unit_num_rename = f"{right_address_col}_unit_number"
        left_address_num_rename = f"{left_address_col}_address_number"
        right_address_num_rename = f"{right_address_col}_address_number"
        left_postal_code_rename = f"{left_address_col}_postal_code"
        right_postal_code_rename = f"{right_address_col}_postal_code"
        left_directional_rename = f"{left_address_col}_street_pre_directional"
        right_directional_rename = f"{right_address_col}_street_pre_directional"

    if skip_address:
        address_condition = " != 1"
        left_address_condition = f"{left_address_col}_skip {address_condition}"
        right_address_condition = f"{right_address_col}_skip {address_condition}"
    else:
        left_address_condition = "TRUE"
        right_address_condition = "TRUE"

    match_names = [
        f"{match_name_stem}_street_fuzzy_match",
        f"{match_name_stem}_unit_fuzzy_match",
    ]

    conditions = [
        f"""
        {left_entity}_{left_address_num_rename} = {right_entity}_{right_address_num_rename} AND
        {left_entity}_{left_postal_code_rename} = {right_entity}_{right_postal_code_rename}""",
        f"""
        {left_entity}_{left_address_num_rename} = {right_entity}_{right_address_num_rename} AND
        {left_entity}_{left_postal_code_rename} = {right_entity}_{right_postal_code_rename} AND
        CAST({left_entity}_{left_unit_num_rename} AS VARCHAR) = CAST({right_entity}_{right_unit_num_rename} AS VARCHAR)
        """,
    ]
    for match_name, condition in zip(match_names, conditions):
        # check link exclusion
        if any(exclusion in match_name for exclusion in link_exclusions):
            return None
        query = f"""
        CREATE OR REPLACE TABLE {link_table} AS

        WITH tfidf_matches AS (
            SELECT id_a,
                id_b,
                similarity as {match_name}
            FROM {tfidf_table}
        ),

        left_source AS (
            SELECT {left_ent_id} as {left_entity}_{left_ent_id_rename},
                    {left_address_col}_street_name_id,
                    {left_address_col}_unit_number as {left_entity}_{left_unit_num_rename},
                    {left_address_col}_street_pre_directional as {left_entity}_{left_directional_rename},
                    {left_address_col}_address_number as {left_entity}_{left_address_num_rename},
                    {left_address_col}_postal_code as {left_entity}_{left_postal_code_rename}
            FROM {left_entity}.{left_table}
            WHERE {left_address_condition}
        ),

        right_source AS (
            SELECT {right_ent_id} as {right_entity}_{right_ent_id_rename},
                    {right_address_col}_street_name_id,
                    {right_address_col}_unit_number as {right_entity}_{right_unit_num_rename},
                    {right_address_col}_street_pre_directional as {right_entity}_{right_directional_rename},
                    {right_address_col}_address_number as {right_entity}_{right_address_num_rename},
                    {right_address_col}_postal_code as {right_entity}_{right_postal_code_rename}
            FROM {right_entity}.{right_table}
            WHERE {right_address_condition}
        ),

        fuzzy_match_1 AS (
            SELECT l.{left_entity}_{left_ent_id_rename},
                l.{left_entity}_{left_unit_num_rename}, l.{left_entity}_{left_address_num_rename},
                l.{left_entity}_{left_postal_code_rename}, l.{left_entity}_{left_directional_rename},
                r.{right_entity}_{right_ent_id_rename},
                r.{right_entity}_{right_unit_num_rename}, r.{right_entity}_{right_address_num_rename},
                r.{right_entity}_{right_postal_code_rename}, r.{right_entity}_{right_directional_rename},
                m.{match_name}
            FROM   tfidf_matches as m
            INNER JOIN left_source as l
                ON m.id_a = l.{left_address_col}_street_name_id
            INNER JOIN right_source as r
                ON m.id_b = r.{right_address_col}_street_name_id
        ),

        fuzzy_match_2 AS (
            SELECT l.{left_entity}_{left_ent_id_rename},
                l.{left_entity}_{left_unit_num_rename}, l.{left_entity}_{left_address_num_rename},
                l.{left_entity}_{left_postal_code_rename}, l.{left_entity}_{left_directional_rename},
                r.{right_entity}_{right_ent_id_rename},
                r.{right_entity}_{right_unit_num_rename}, r.{right_entity}_{right_address_num_rename},
                r.{right_entity}_{right_postal_code_rename}, r.{right_entity}_{right_directional_rename},
                m.{match_name}
            FROM   tfidf_matches as m
            INNER JOIN left_source as l
                ON m.id_b = l.{left_address_col}_street_name_id
            INNER JOIN right_source as r
                ON m.id_a = r.{right_address_col}_street_name_id
        ),

        all_fuzzy_matches AS (
            SELECT {left_entity}_{left_ent_id_rename},
                    {right_entity}_{right_ent_id_rename},
                    {match_name}
            FROM (SELECT * FROM fuzzy_match_1
                UNION
                SELECT * FROM fuzzy_match_2)
            WHERE {same_condition} AND
            {condition}

        ),

        existing_links AS (
            SELECT *
            FROM {link_table}
        )

        SELECT *
        FROM   all_fuzzy_matches
        FULL JOIN existing_links
            USING({left_entity}_{left_ent_id_rename},{right_entity}_{right_ent_id_rename})

        """

        with duckdb.connect(database=db_path, read_only=False) as db_conn:
            db_conn.execute(query)
            console.log(f"[yellow] Created {match_name}")
            logger.debug(f"Created {match_name}")
            cols = [row[1] for row in db_conn.execute(f"PRAGMA table_info('{link_table}')").fetchall()]
            for col in cols:
                db_conn.execute(f"UPDATE {link_table} SET {col} = 0 WHERE {col} IS NULL")

            # set datatype to int or float as expected
            if "fuzzy" in match_name:
                db_conn.execute(f"UPDATE {link_table} SET {match_name} = CAST({match_name} AS FLOAT)")
            else:
                db_conn.execute(f"UPDATE {link_table} SET {match_name} = CAST({match_name} AS INT1)")

    return None


# OTHER UTILS


def generate_combos_within_across_tables(name_idx: list, address_idx: Optional[list] = None) -> tuple:
    """
    create all possible combinations of across tables in the same entity,
    but do not include combos within the same table
    if address_idx is not empty, also create across combos between address tables
    """
    if address_idx is None:
        address_idx = []

    across_combos_name_idx = list(itertools.combinations(range(len(name_idx)), 2))
    across_name_combos: list = []
    for i, j in across_combos_name_idx:
        across_name_combos += itertools.product(name_idx[i], name_idx[j])

    if len(address_idx) > 0:
        across_address_combos: list = []
        across_combos_address_idx = list(itertools.combinations(range(len(address_idx)), 2))
        for i, j in across_combos_address_idx:
            across_address_combos += itertools.product(address_idx[i], address_idx[j])

        return across_name_combos, across_address_combos

    else:
        return across_name_combos, []
