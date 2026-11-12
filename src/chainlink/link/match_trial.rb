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
    skip_col: bool = False,
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
    if left_entity != right_entity:
        if left_side < right_side:
            match_name_col = f"{left_side}_{right_side}_{match_type}"
        else:
            match_name_col = f"{right_side}_{left_side}_{match_type}"
    else:
        match_name_col = f"{left_side}_{right_side}_{match_type}"

    # check link exclusion
    if any(exclusion in match_name_col for exclusion in link_exclusions):
        return None

    if skip_col:
        col_condition = " != 1"
        left_col_condition = f"l.{left_matching_col}_skip {col_condition}"
        right_col_condition = f"r.{right_matching_col}_skip {col_condition}"
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
    




SELECT property_address_street_id, property_address_street, count(*) as cnt FROM parcel group by property_address_street_id, property_address_street order by cnt desc