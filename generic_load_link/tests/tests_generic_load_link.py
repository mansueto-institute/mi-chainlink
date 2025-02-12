import pandas as pd
import duckdb
import os
import pytest

# cleaning tests (copy from existing functions)


from woc.create_db.generic_load_link.load_utils import (
    load_to_db,
    update_entity_ids,
    clean_generic,
)
from woc.create_db.generic_load_link.link_generic import (
    create_within_links,
    create_across_links,
    create_tfidf_within_links,
    create_tfidf_across_links,
)

from woc.create_db.generic_load_link.link_utils import generate_tfidf_links

# add pytest fixture

CONFIG_SIMPLE_1 = {
    "schema_name": "test_simple1",
    "tables": [
        {
            "table_name": "test1",
            "table_name_path": "data/test1.csv",
            "id_col": "id",
            "name_cols": ["name"],
            "address_cols": ["address"],
        }
    ],
}

CONFIG_SIMPLE_2 = {
    "schema_name": "test_simple2",
    "tables": [
        {
            "table_name": "test2",
            "table_name_path": "data/test2.csv",
            "id_col": "id",
            "name_cols": ["name"],
            "address_cols": ["address"],
        }
    ],
}

CONFIG_SMALL_LLC = {
    "schema_name": "llc",
    "tables": [
        {
            "table_name": "master",
            "table_name_path": "data/master.csv",
            "id_col": "file_num",
            "name_cols": ["name_raw"],
            "address_cols": ["address"],
        }
    ],
}

CONFIG_SMALL_PARCEL = {
    "schema_name": "parcel",
    "tables": [
        {
            "table_name": "parcels",
            "table_name_path": "data/parcels.csv",
            "id_col": "pin",
            "name_cols": ["tax_payer_name"],
            "address_cols": ["mailing_address"],
        }
    ],
}


@pytest.fixture
def make_simple_db():

    if os.path.exists("db/test_simple.db"):
        os.remove("db/test_simple.db")

    file1 = {
        "table_name": "test1",
        "table_name_path": "data/test1.csv",
        "id_col": "id",
        "name_cols": ["name"],
        "address_cols": ["address"],
    }

    file2 = {
        "table_name": "test2",
        "table_name_path": "data/test1.csv",
        "id_col": "id",
        "name_cols": ["name"],
        "address_cols": ["address"],
    }

    df1 = pd.DataFrame(
        {
            "id": ["1", "2", "3", "4"],
            "name": ["Aus St", "Big Calm", "Cool Cool", "Aus St"],
            "address": ["1", "2", "3", "4"],
            "skip_address": [0, 0, 0, 0],
        }
    )
    df2 = pd.DataFrame(
        {
            "id": ["5", "6", "7", "8"],
            "name": ["Aus St", "Erie Erie", "Cool Cool", "Good Doom"],
            "address": ["5", "6", "3", "4"],
            "skip_address": [0, 0, 0, 0],
        }
    )

    df1 = clean_generic(df1, file1)

    df2 = clean_generic(df2, file2)

    db_path = "db/test_simple.db"
    with duckdb.connect(db_path, read_only=False) as db_conn:

        load_to_db(df1, "test1", db_conn, "test_simple1")

        load_to_db(df2, "test2", db_conn, "test_simple2")

        all_id_cols = ["name_id", "address_id", "street_id", "street_name_id"]

        for df in [df1, df2]:
            id_cols = []
            for col in df.columns:
                if (
                    any(c in col for c in all_id_cols)
                    and "subaddress_identifier" not in col
                ):
                    id_cols.append(col)

            for col in id_cols:
                update_entity_ids(df=df, entity_id_col=col, db_conn=db_conn)

    create_within_links(
        db_path=db_path, schema_config=CONFIG_SIMPLE_1, link_exclusions=[]
    )
    create_within_links(
        db_path=db_path, schema_config=CONFIG_SIMPLE_2, link_exclusions=[]
    )
    create_across_links(
        db_path=db_path,
        new_schema=CONFIG_SIMPLE_1,
        existing_schema=CONFIG_SIMPLE_2,
        link_exclusions=[],
    )

    generate_tfidf_links(db_path, table_location="entity.name_similarity")

    create_tfidf_within_links(
        db_path=db_path, schema_config=CONFIG_SIMPLE_1, link_exclusions=[]
    )

    create_tfidf_within_links(
        db_path=db_path, schema_config=CONFIG_SIMPLE_2, link_exclusions=[]
    )

    create_tfidf_across_links(
        db_path=db_path,
        new_schema=CONFIG_SIMPLE_1,
        existing_schema=CONFIG_SIMPLE_2,
        link_exclusions=[],
    )


@pytest.fixture
def make_small_df():

    # test_small.db exists, then delete the db
    if os.path.exists("db/test_small.db"):
        os.remove("db/test_small.db")

    parcel_df = pd.DataFrame(
        {
            "PIN": [
                "20344100300000",
                "24171070561019",
                "25212140150000",
                "25022160020000",
                "25022160020001",
                "25022160020002",
            ],
            "tax_payer_name": [
                "SANJAY PATEL",
                "GRONKA PROPERTIES INC",
                "MOBUCASA INC",
                "TAXPAYER OF",
                "NAPERVILLE BITES AND SITE , LLC",
                "TAXPAYER OF",
            ],
            "mailing_address": [
                "645 LEAMINGTON, WILMETTE, IL 60091",
                "8041 SAYRE AVE, BURBANK, IL 60459",
                "1212 S NAPER BLVD 119, NAPERVILLE, IL 60540",
                "1319 E 89TH ST, CHICAGO, IL 60619",
                "2555 W. 79TH ST. APT 5 CHICAGO IL 60652",
                "8041 SAYRE AVE, BURBANK, IL 60459",
            ],
            "skip_address": [0, 0, 0, 0, 0, 0],
        }
    )
    parcel_file = {
        "table_name": "parcel",
        "table_name_path": "data/parcel.csv",
        "id_col": "PIN",
        "name_cols": ["tax_payer_name"],
        "address_cols": ["mailing_address"],
    }

    llc_df = pd.DataFrame(
        {
            "file_num": [
                1338397,
                1127901,
                325194,
                717605,
                257730,
            ],
            "name_raw": [
                "WOOW HVAC LLC",
                "MOBUCASA INC",
                "WOOW HVAC LLC",
                "SANJAY PATEL",
                "NAPERVILLE BITES AND SITES , LLC",
            ],
            "address": [
                "645 LEAMINGTON, WILMETTE, IL 60091",
                "",
                "2555 W. 79TH ST. CHICAGO IL 60652",
                "8041 SAYRE AVE, BURBANK, IL 60459",
                "1319 E 89TH ST. CHICAGO IL 60638",
            ],
            "skip_address": [0, 0, 0, 0, 0],
        }
    )
    llc_file = {
        "table_name": "master",
        "table_name_path": "data/llc.csv",
        "id_col": "file_num",
        "name_cols": ["name_raw"],
        "address_cols": ["address"],
    }

    parcel_df = clean_generic(parcel_df, parcel_file)
    llc_df = clean_generic(llc_df, llc_file)

    db_path = "db/test_small.db"

    with duckdb.connect(db_path, read_only=False) as db_conn:

        load_to_db(parcel_df, "parcels", db_conn, "parcel")
        load_to_db(llc_df, "master", db_conn, "llc")

        all_id_cols = ["name_id", "address_id", "street_id", "street_name_id"]

        for df in [parcel_df, llc_df]:
            id_cols = []
            for col in df.columns:
                if (
                    any(c in col for c in all_id_cols)
                    and "subaddress_identifier" not in col
                ):
                    id_cols.append(col)

            for col in id_cols:
                update_entity_ids(df=df, entity_id_col=col, db_conn=db_conn)

    create_within_links(
        db_path=db_path, schema_config=CONFIG_SMALL_LLC, link_exclusions=[]
    )

    create_within_links(
        db_path=db_path, schema_config=CONFIG_SMALL_PARCEL, link_exclusions=[]
    )

    create_across_links(
        db_path=db_path,
        new_schema=CONFIG_SMALL_LLC,
        existing_schema=CONFIG_SMALL_PARCEL,
        link_exclusions=[],
    )

    generate_tfidf_links(db_path, table_location="entity.name_similarity")

    create_tfidf_within_links(
        db_path=db_path, schema_config=CONFIG_SMALL_LLC, link_exclusions=[]
    )

    create_tfidf_within_links(
        db_path=db_path, schema_config=CONFIG_SMALL_PARCEL, link_exclusions=[]
    )

    create_tfidf_across_links(
        db_path=db_path,
        new_schema=CONFIG_SMALL_LLC,
        existing_schema=CONFIG_SMALL_PARCEL,
        link_exclusions=[],
    )


def test_simple_exact_within(make_simple_db):

    with duckdb.connect("db/test_simple.db", read_only=True) as db_conn:
        query = "SELECT * FROM link.test_simple1_test_simple1"
        df = db_conn.execute(query).df()

    # one match
    assert df.shape[0] == 1

    # id_1,
    # id_2,
    # test_simple1_test1_name_test_simple1_test1_name_name_match,
    # test_simple1_test1_name_test_simple1_test1_name_fuzzy_match,
    # test_simple1_test1_address_test_simple1_test1_address_address_match,
    # test_simple1_test1_address_test_simple1_test1_address_street_match,
    # test_simple1_test1_address_test_simple1_test1_address_unit_match,
    # test_simple1_test1_address_test_simple1_test1_address_street_num_match
    assert df.shape[1] == 8


def test_simple_exact_across(make_simple_db):

    with duckdb.connect("db/test_simple.db", read_only=True) as db_conn:
        query = "SELECT * FROM link.test_simple1_test_simple2"
        df = db_conn.execute(query).df()

    # one match
    assert df.shape[0] == 4

    # test1_id,
    # test2_id,
    # test_simple1_test1_name_test_simple2_test2_name_name_match,
    # test_simple1_test1_name_test_simple2_test2_name_fuzzy_match,
    # test_simple1_test1_address_test_simple2_test2_address_address_match,
    # test_simple1_test1_address_test_simple2_test2_address_street_match,
    # test_simple1_test1_address_test_simple2_test2_address_unit_match,
    # test_simple1_test1_address_test_simple2_test2_address_street_num_match
    assert df.shape[1] == 8


def test_small_entity_tables(make_small_df):

    db_path = "db/test_small.db"
    with duckdb.connect(db_path, read_only=False) as db_conn:

        query = "SELECT * FROM entity.name"
        df = db_conn.execute(query).df()
        assert df.shape[0] == 7

        query = "SELECT * FROM entity.address"
        df = db_conn.execute(query).df()
        assert df.shape[0] == 8

        query = "SELECT * FROM entity.street"
        df = db_conn.execute(query).df()
        assert df.shape[0] == 6


def test_small_exact_within(make_small_df):

    db_path = "db/test_small.db"
    with duckdb.connect(db_path, read_only=True) as db_conn:
        query = "SELECT * FROM link.llc_llc"
        df = db_conn.execute(query).df()

        # one match
        assert df.shape[0] == 1

        # parcel_pin_1,
        # parcel_pin_2,
        # parcel_parcels_name_raw_parcel_parcels_name_raw_fuzzy_match
        # parcel_parcels_tax_payer_name_parcel_parcels_tax_payer_name_name_match,
        # parcel_parcels_address_parcel_parcels_address_address_match,
        # parcel_parcels_address_parcel_parcels_address_street_match,
        # parcel_parcels_address_parcel_parcels_address_unit_match,
        # parcel_parcels_address_parcel_parcels_address_street_num_match
        assert df.shape[1] == 8

        query = "SELECT * FROM link.parcel_parcel"
        df = db_conn.execute(query).df()

        # one match
        assert df.shape[0] == 1
        # on within fuzzy match
        assert df.shape[1] == 8


def test_small_exact_across(make_small_df):

    db_path = "db/test_small.db"

    with duckdb.connect(db_path, read_only=True) as db_conn:
        query = "SELECT * FROM link.llc_parcel"
        df = db_conn.execute(query).df()

    # eight matches
    assert df.shape[0] == 8

    # llc_file_num,
    # parcel_pin,
    # llc_master_name_raw_parcel_parcels_tax_payer_name_fuzzy_match,
    # llc_master_name_raw_parcel_parcels_tax_payer_name_name_match,
    # llc_master_address_parcel_parcels_mailing_address_address_match,
    # llc_master_address_parcel_parcels_mailing_address_street_match,
    # llc_master_address_parcel_parcels_mailing_address_unit_match,
    # llc_master_address_parcel_parcels_mailing_address_street_num_match
    assert df.shape[1] == 8


def test_small_fuzzy(make_small_df):

    db_path = "db/test_small.db"

    with duckdb.connect(db_path, read_only=True) as db_conn:
        query = "SELECT * FROM link.llc_parcel"
        df = db_conn.execute(query).df()

    # one fuzzy match
    df_test = df[
        df["llc_master_name_raw_parcel_parcels_tax_payer_name_fuzzy_match"] > 0
    ]
    assert df_test.shape[0] == 1
