import itertools
from pathlib import Path

from chainlink.link.link_utils import (
    execute_address_fuzzy_link,
    execute_fuzzy_link,
    execute_match,
    execute_match_address,
    generate_combos_within_across_tables,
)


def create_within_links(db_path: str | Path, schema_config: dict, link_exclusions: list) -> None:
    """
    Creates exact string matches on name and address fields for entity and
    entity.

    For each file find the links with the file:
        -find all the name links includes name1 to name1, name1 to name2, etc.
        -find all the address links includes address1 to address1, address1 to address2, etc.
            -match by raw address string
            -match by clean street string
            -if street id matches, match by unit
            -match street name and number if zipcode matches
        -find all name and address links across tables within the entity

    Returns: None
    """

    entity = schema_config["schema_name"]

    within_entity_across_tables_names = []
    within_entity_across_tables_addresses = []

    # within each table
    for table_config in schema_config["tables"]:
        table = table_config["table_name"]

        if table_config.get("name_cols"):
            # generate name matches combos
            name_combos = list(itertools.product(table_config["name_cols"], repeat=2))

            for left_name, right_name in name_combos:
                execute_match(
                    db_path=db_path,
                    match_type="name_match",
                    left_entity=entity,
                    left_table=table,
                    left_matching_col=left_name,
                    left_matching_id=f"{left_name}_name_id",
                    left_ent_id=table_config["id_col"],
                    right_entity=entity,
                    right_table=table,
                    right_matching_col=right_name,
                    right_ent_id=table_config["id_col"],
                    right_matching_id=f"{right_name}_name_id",
                    link_exclusions=link_exclusions,
                )

        if table_config.get("address_cols"):
            # address within
            address_combos = list(itertools.product(table_config["address_cols"], repeat=2))

            for left_address, right_address in address_combos:
                execute_match_address(
                    db_path=db_path,
                    left_entity=entity,
                    left_table=table,
                    left_address=left_address,
                    left_ent_id=table_config["id_col"],
                    right_entity=entity,
                    right_table=table,
                    right_address=right_address,
                    right_ent_id=table_config["id_col"],
                    skip_address=True,
                    link_exclusions=link_exclusions,
                )

        # for across tables
        if table_config.get("name_cols"):
            within_entity_across_tables_names.append([
                (name, table, table_config["id_col"]) for name in table_config["name_cols"]
            ])
        if table_config.get("address_cols"):
            within_entity_across_tables_addresses.append([
                (address, table, table_config["id_col"]) for address in table_config["address_cols"]
            ])

    # generate combos across tables
    if within_entity_across_tables_names or within_entity_across_tables_addresses:
        across_name_combos, across_address_combos = generate_combos_within_across_tables(
            within_entity_across_tables_names, within_entity_across_tables_addresses
        )

    # across files for name
    for left, right in across_name_combos:
        left_name, left_table, left_ent_id = left
        right_name, right_table, right_ent_id = right

        execute_match(
            db_path=db_path,
            match_type="name_match",
            left_entity=entity,
            left_table=left_table,
            left_matching_col=left_name,
            left_ent_id=left_ent_id,
            left_matching_id=f"{left_name}_name_id",
            right_entity=entity,
            right_table=right_table,
            right_matching_col=right_name,
            right_ent_id=right_ent_id,
            right_matching_id=f"{right_name}_name_id",
            link_exclusions=link_exclusions,
        )

    # across files for address
    for left, right in across_address_combos:
        left_address, left_table, left_ent_id = left
        right_address, right_table, right_ent_id = right

        execute_match_address(
            db_path=db_path,
            left_entity=entity,
            left_table=left_table,
            left_address=left_address,
            left_ent_id=left_ent_id,
            right_entity=entity,
            right_table=right_table,
            right_address=right_address,
            right_ent_id=right_ent_id,
            skip_address=True,
            link_exclusions=link_exclusions,
        )


def create_across_links(db_path: str | Path, new_schema: dict, existing_schema: dict, link_exclusions: list) -> None:
    """
    For each entity in the existing_db list, create links between the new entity
    and the existing entity.

    for old_entity in existing_db:
        -find all the name links old_entity.name to new_entity.name, etc.
        -find all the address links old_entity.address to new_entity.address, etc.
            -match by raw address string
            -match by clean street string
            -if street id matches, match by unit
            -match street name and number if zipcode matches

    Returns: None
    """

    new_entity = new_schema["schema_name"]

    new_entity_names = []
    new_entity_addresses = []

    # gather all the name and address columns for the new entity
    for table in new_schema["tables"]:
        for name_col in table["name_cols"]:
            new_entity_names.append((table["table_name"], table["id_col"], name_col))
        for address_col in table["address_cols"]:
            new_entity_addresses.append((table["table_name"], table["id_col"], address_col))

    # create name and address matches for each existing entity and new entity
    existing_entity = existing_schema["schema_name"]

    existing_entity_names = []
    existing_entity_addresses = []

    # gather all the name and address columns for this existing entity
    for table in existing_schema["tables"]:
        for name_col in table["name_cols"]:
            existing_entity_names.append((
                table["table_name"],
                table["id_col"],
                name_col,
            ))
        for address_col in table["address_cols"]:
            existing_entity_addresses.append((
                table["table_name"],
                table["id_col"],
                address_col,
            ))
    # generate name match combos
    name_combos = list(itertools.product(new_entity_names, existing_entity_names))

    # need to add in across table within entity combos

    for new, old in name_combos:
        left_table, left_ent_id, left_name = new
        right_table, right_ent_id, right_name = old

        execute_match(
            db_path=db_path,
            left_entity=new_entity,
            left_table=left_table,
            left_matching_col=left_name,
            left_ent_id=left_ent_id,
            match_type="name_match",
            left_matching_id=f"{left_name}_name_id",
            right_entity=existing_entity,
            right_table=right_table,
            right_matching_col=right_name,
            right_ent_id=right_ent_id,
            right_matching_id=f"{right_name}_name_id",
            link_exclusions=link_exclusions,
        )

    # generate address match combos
    address_combos = list(itertools.product(new_entity_addresses, existing_entity_addresses))

    for new, old in address_combos:
        left_table, left_ent_id, left_address = new
        right_table, right_ent_id, right_address = old

        execute_match_address(
            db_path=db_path,
            left_entity=new_entity,
            left_table=left_table,
            left_address=left_address,
            left_ent_id=left_ent_id,
            right_entity=existing_entity,
            right_table=right_table,
            right_address=right_address,
            right_ent_id=right_ent_id,
            skip_address=True,
            link_exclusions=link_exclusions,
        )


def create_tfidf_within_links(db_path: str | Path, schema_config: dict, link_exclusions: list) -> None:
    """
    create tfidf links within entity

    Returns: None
    """

    new_entity = schema_config["schema_name"]
    within_entity_across_tables_names = []
    within_entity_across_tables_addresses = []
    # create fuzzy links
    # generate combos, need all within tables

    for table in schema_config["tables"]:
        # generate name matches combos
        name_combos = list(itertools.product(table["name_cols"], repeat=2))

        for left_name, right_name in name_combos:
            execute_fuzzy_link(
                db_path=db_path,
                left_entity=new_entity,
                left_table=table["table_name"],
                left_ent_id=table["id_col"],
                left_name_col=left_name,
                right_entity=new_entity,
                right_table=table["table_name"],
                right_ent_id=table["id_col"],
                right_name_col=right_name,
                tfidf_table="entity.name_similarity",
                link_exclusions=link_exclusions,
            )

        address_combos = list(itertools.combinations_with_replacement(table["address_cols"], 2))
        for left_address, right_address in address_combos:
            execute_address_fuzzy_link(
                db_path=db_path,
                left_entity=new_entity,
                left_table=table["table_name"],
                left_ent_id=table["id_col"],
                left_address_col=left_address,
                right_entity=new_entity,
                right_table=table["table_name"],
                right_ent_id=table["id_col"],
                right_address_col=right_address,
                tfidf_table="entity.street_name_similarity",
                skip_address=True,
                link_exclusions=link_exclusions,
            )

        # for across tables within entity
        within_entity_across_tables_names.append([
            (name, table["table_name"], table["id_col"]) for name in table["name_cols"]
        ])
        within_entity_across_tables_addresses.append([
            (address, table["table_name"], table["id_col"]) for address in table["address_cols"]
        ])

    # generate combos, across tables within entity
    across_name_combos, across_address_combos = generate_combos_within_across_tables(
        within_entity_across_tables_names, within_entity_across_tables_addresses
    )

    for left, right in across_name_combos:
        left_name, left_table, left_ent_id = left
        right_name, right_table, right_ent_id = right

        execute_fuzzy_link(
            db_path=db_path,
            left_entity=new_entity,
            left_table=left_table,
            left_ent_id=left_ent_id,
            left_name_col=left_name,
            right_entity=new_entity,
            right_table=right_table,
            right_ent_id=right_ent_id,
            right_name_col=right_name,
            tfidf_table="entity.name_similarity",
            link_exclusions=link_exclusions,
        )

    for left, right in across_address_combos:
        left_address, left_table, left_ent_id = left
        right_address, right_table, right_ent_id = right
        execute_address_fuzzy_link(
            db_path=db_path,
            left_entity=new_entity,
            left_table=left_table,
            left_ent_id=left_ent_id,
            left_address_col=left_address,
            right_entity=new_entity,
            right_table=right_table,
            right_ent_id=right_ent_id,
            right_address_col=right_address,
            tfidf_table="entity.street_name_similarity",
            skip_address=True,
            link_exclusions=link_exclusions,
        )


def create_tfidf_across_links(
    db_path: str | Path, new_schema: dict, existing_schema: dict, link_exclusions: list
) -> None:
    """
    create all fuzzy links across new entity and existing entity

    Returns: None
    """
    new_entity = new_schema["schema_name"]

    # gather all the name columns for the new entity
    new_entity_names = []
    new_entity_addresses = []

    for table in new_schema["tables"]:
        for name_col in table["name_cols"]:
            new_entity_names.append((table["table_name"], table["id_col"], name_col))
        for address_col in table["address_cols"]:
            new_entity_addresses.append((table["table_name"], table["id_col"], address_col))

    # go through all the existing entities / schemas

    existing_entity = existing_schema["schema_name"]

    existing_entity_names = []
    existing_entity_addresses = []

    # gather all the name and address columns for this existing entity
    for table in existing_schema["tables"]:
        for name_col in table["name_cols"]:
            existing_entity_names.append((
                table["table_name"],
                table["id_col"],
                name_col,
            ))
        for address_col in table["address_cols"]:
            existing_entity_addresses.append((
                table["table_name"],
                table["id_col"],
                address_col,
            ))
    # generate name match combos

    name_combos = list(itertools.product(new_entity_names, existing_entity_names))

    for new, old in name_combos:
        left_table, left_ent_id, left_name = new
        right_table, right_ent_id, right_name = old

        execute_fuzzy_link(
            db_path=db_path,
            left_entity=new_entity,
            left_table=left_table,
            left_ent_id=left_ent_id,
            left_name_col=left_name,
            right_entity=existing_entity,
            right_table=right_table,
            right_ent_id=right_ent_id,
            right_name_col=right_name,
            tfidf_table="entity.name_similarity",
            link_exclusions=link_exclusions,
        )

    # generate address match combos
    address_combos = list(itertools.product(new_entity_addresses, existing_entity_addresses))
    for new, old in address_combos:
        left_table, left_ent_id, left_address = new
        right_table, right_ent_id, right_address = old

        execute_address_fuzzy_link(
            db_path=db_path,
            left_entity=new_entity,
            left_table=left_table,
            left_ent_id=left_ent_id,
            left_address_col=left_address,
            right_entity=existing_entity,
            right_table=right_table,
            right_ent_id=right_ent_id,
            right_address_col=right_address,
            tfidf_table="entity.street_name_similarity",
            skip_address=True,
            link_exclusions=link_exclusions,
        )

    return None
