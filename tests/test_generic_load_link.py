import os

import duckdb
import polars as pl
import pytest

from chainlink.main import chainlink

# add pytest fixture

CONFIG_SIMPLE_1 = {
    "schema_name": "test_simple1",
    "tables": [
        {
            "table_name": "test1",
            "table_name_path": "tests/data/test1.csv",
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
            "table_name_path": "tests/data/test2.csv",
            "id_col": "id",
            "name_cols": ["name"],
            "address_cols": ["address"],
        }
    ],
}
CONFIG_SIMPLE = {
    "options": {
        "db_path": "tests/db/test_simple.db",
        "force_db_create": True,
        "probabilistic": True,
    },
    "schemas": [CONFIG_SIMPLE_1, CONFIG_SIMPLE_2],
}

CONFIG_SMALL_LLC = {
    "schema_name": "llc",
    "tables": [
        {
            "table_name": "master",
            "table_name_path": "tests/data/small_llc.csv",
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
            "table_name_path": "tests/data/small_parcel.csv",
            "id_col": "pin",
            "name_cols": ["tax_payer_name"],
            "address_cols": ["mailing_address"],
        }
    ],
}

CONFIG_SMALL = {
    "options": {
        "db_path": "tests/db/test_small.db",
        "force_db_create": True,
        "probabilistic": True,
    },
    "schemas": [CONFIG_SMALL_LLC, CONFIG_SMALL_PARCEL],
}


@pytest.fixture
def make_simple_db():
    if os.path.exists("tests/db/test_simple.db"):
        os.remove("tests/db/test_simple.db")

    pl.DataFrame({
        "id": ["1", "2", "3", "4"],
        "name": ["Aus St", "Big Calm", "Cool Cool", "Aus St"],
        "address": ["1", "2", "3", "4"],
        "skip_address": [0, 0, 0, 0],
    }).write_csv("tests/data/test1.csv")
    pl.DataFrame({
        "id": ["5", "6", "7", "8"],
        "name": ["Aus St", "Erie Erie", "Cool Cool", "Good Doom"],
        "address": ["5", "6", "3", "4"],
        "skip_address": [0, 0, 0, 0],
    }).write_csv("tests/data/test2.csv")

    chainlink(
        CONFIG_SIMPLE,
        config_path="tests/configs/config_simple.yaml",
    )


@pytest.fixture
def make_small_df():
    # test_small.db exists, then delete the db
    if os.path.exists("tests/db/test_small.db"):
        os.remove("tests/db/test_small.db")

    pl.DataFrame({
        "pin": [
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
    }).write_csv("tests/data/small_parcel.csv")

    pl.DataFrame({
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
    }).write_csv("tests/data/small_llc.csv")

    chainlink(
        CONFIG_SMALL,
        config_path="tests/configs/config_small.yaml",
    )


def test_simple_exact_within(make_simple_db):
    with duckdb.connect("tests/db/test_simple.db", read_only=True) as db_conn:
        query = "SELECT * FROM link.test_simple1_test_simple1"
        df = db_conn.execute(query).pl()

    # one match
    assert df.shape[0] == 1

    # id_1,
    # id_2,
    # test_simple1_test1_name_test_simple1_test1_name_name_match,
    # test_simple1_test1_address_test_simple1_test1_address_street_fuzzy_match
    # test_simple1_test1_address_test_simple1_test1_address_unit_fuzzy_match
    # test_simple1_test1_name_test_simple1_test1_name_fuzzy_match,
    # test_simple1_test1_address_test_simple1_test1_address_address_match,
    # test_simple1_test1_address_test_simple1_test1_address_street_match,
    # test_simple1_test1_address_test_simple1_test1_address_unit_match,
    # test_simple1_test1_address_test_simple1_test1_address_street_num_match
    assert df.shape[1] == 10


def test_simple_exact_across(make_simple_db):
    with duckdb.connect("tests/db/test_simple.db", read_only=True) as db_conn:
        query = "SELECT * FROM link.test_simple1_test_simple2"
        df = db_conn.execute(query).pl()

    # one match
    assert df.shape[0] == 4

    # test1_id,
    # test2_id,
    # test_simple1_test1_name_test_simple2_test2_name_name_match,
    # test_simple1_test1_name_test_simple2_test2_name_fuzzy_match,
    # test_simple1_test1_address_test_simple2_test2_address_address_match,
    # test_simple1_test1_address_test_simple2_test2_address_street_match,
    # test_simple1_test1_address_test_simple2_test2_address_street_fuzzy_match,
    # test_simple1_test1_address_test_simple2_test2_address_unit_match,
    # test_simple1_test1_address_test_simple2_test2_address_unit_fuzzy_match,
    # test_simple1_test1_address_test_simple2_test2_address_street_num_match
    assert df.shape[1] == 10


def test_small_entity_tables(make_small_df):
    db_path = "tests/db/test_small.db"
    with duckdb.connect(db_path, read_only=True) as db_conn:
        query = "SELECT * FROM entity.name"
        df = db_conn.execute(query).pl()
        assert df.shape[0] == 7

        query = "SELECT * FROM entity.address"
        df = db_conn.execute(query).pl()
        assert df.shape[0] == 8

        query = "SELECT * FROM entity.street"
        df = db_conn.execute(query).pl()
        assert df.shape[0] == 6


def test_small_exact_within(make_small_df):
    db_path = "tests/db/test_small.db"
    with duckdb.connect(db_path, read_only=True) as db_conn:
        query = "SELECT * FROM link.llc_llc"
        df = db_conn.execute(query).pl()

        correct_df = pl.DataFrame({
            "llc_file_num_1": ["1338397"],
            "llc_file_num_2": ["325194"],
            "llc_master_address_llc_master_address_unit_fuzzy_match": [0.0],
            "llc_master_address_llc_master_address_street_fuzzy_match": [0.0],
            "llc_master_name_raw_llc_master_name_raw_fuzzy_match": [0.0],
            "llc_master_name_raw_llc_master_name_raw_name_match": [1],
            "llc_master_address_llc_master_address_address_match": [0],
            "llc_master_address_llc_master_address_street_match": [0],
            "llc_master_address_llc_master_address_unit_match": [0],
            "llc_master_address_llc_master_address_street_num_match": [0],
        })
        # for row in df.rows():
        #     print(row)

        # one match
        assert df.shape[0] == 1
        assert df.shape[1] == 10
        assert correct_df.equals(df)

        query = "SELECT * FROM link.parcel_parcel"
        df = db_conn.execute(query).pl()

        correct_df = pl.DataFrame({
            "parcel_pin_1": ["24171070561019"],
            "parcel_pin_2": ["25022160020002"],
            "parcel_parcels_mailing_address_parcel_parcels_mailing_address_unit_fuzzy_match": [0.0],
            "parcel_parcels_mailing_address_parcel_parcels_mailing_address_street_fuzzy_match": [0.0],
            "parcel_parcels_tax_payer_name_parcel_parcels_tax_payer_name_fuzzy_match": [0.0],
            "parcel_parcels_tax_payer_name_parcel_parcels_tax_payer_name_name_match": [None],
            "parcel_parcels_mailing_address_parcel_parcels_mailing_address_address_match": [1],
            "parcel_parcels_mailing_address_parcel_parcels_mailing_address_street_match": [1],
            "parcel_parcels_mailing_address_parcel_parcels_mailing_address_unit_match": [0],
            "parcel_parcels_mailing_address_parcel_parcels_mailing_address_street_num_match": [1],
        })

        # one match
        assert df.shape[0] == 1
        # on within fuzzy match
        assert df.shape[1] == 10
        assert correct_df.equals(df)


def test_small_exact_across(make_small_df):
    db_path = "tests/db/test_small.db"

    with duckdb.connect(db_path, read_only=True) as db_conn:
        query = "SELECT * FROM link.llc_parcel"
        df = db_conn.execute(query).pl()

    correct_df = pl.DataFrame({
        "llc_file_num": [
            "325194",
            "717605",
            "1127901",
            "257730",
            "1338397",
            "717605",
            "717605",
            "257730",
        ],
        "parcel_pin": [
            "25022160020001",
            "20344100300000",
            "25212140150000",
            "25022160020000",
            "20344100300000",
            "25022160020002",
            "24171070561019",
            "25022160020001",
        ],
        "llc_master_address_parcel_parcels_mailing_address_unit_fuzzy_match": [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ],
        "llc_master_address_parcel_parcels_mailing_address_street_fuzzy_match": [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ],
        "llc_master_name_raw_parcel_parcels_tax_payer_name_fuzzy_match": [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.9805094416874218,
        ],
        "llc_master_name_raw_parcel_parcels_tax_payer_name_name_match": [
            None,
            1,
            1,
            None,
            None,
            None,
            None,
            None,
        ],
        "llc_master_address_parcel_parcels_mailing_address_address_match": [
            None,
            0,
            0,
            None,
            1,
            1,
            1,
            None,
        ],
        "llc_master_address_parcel_parcels_mailing_address_street_match": [
            1,
            0,
            0,
            1,
            1,
            1,
            1,
            None,
        ],
        "llc_master_address_parcel_parcels_mailing_address_unit_match": [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            None,
        ],
        "llc_master_address_parcel_parcels_mailing_address_street_num_match": [
            1,
            0,
            0,
            0,
            1,
            1,
            1,
            None,
        ],
    })

    # eight matches
    assert df.shape[0] == 8
    assert df.shape[1] == 10
    assert correct_df.sort("llc_file_num").equals(df.sort("llc_file_num"))


def test_small_fuzzy(make_small_df):
    db_path = "tests/db/test_small.db"

    with duckdb.connect(db_path, read_only=True) as db_conn:
        query = "SELECT * FROM link.llc_parcel"
        df = db_conn.execute(query).pl()

    # Create a DataFrame with the test data
    correct_df = pl.DataFrame({
        "llc_file_num": ["257730"],
        "parcel_pin": ["25022160020001"],
        "llc_master_address_parcel_parcels_mailing_address_unit_fuzzy_match": [0.0],
        "llc_master_address_parcel_parcels_mailing_address_street_fuzzy_match": [0.0],
        "llc_master_name_raw_parcel_parcels_tax_payer_name_fuzzy_match": [0.9805094416874218],
        "llc_master_name_raw_parcel_parcels_tax_payer_name_name_match": [None],
        "llc_master_address_parcel_parcels_mailing_address_address_match": [None],
        "llc_master_address_parcel_parcels_mailing_address_street_match": [None],
        "llc_master_address_parcel_parcels_mailing_address_unit_match": [None],
        "llc_master_address_parcel_parcels_mailing_address_street_num_match": [None],
    })

    # one fuzzy match
    df_test = df.filter(pl.col("llc_master_name_raw_parcel_parcels_tax_payer_name_fuzzy_match") > 0)
    assert df_test.shape[0] == 1
    assert df_test.equals(correct_df)


def test_download_tables():
    pass
