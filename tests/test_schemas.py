from chainlink.utils import validate_config

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
    "options": {"db_path": "tests/db/test_simple.db", "force_db_create": True, "probabilistic": True},
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
    "options": {"db_path": "tests/db/test_small.db", "force_db_create": True, "probabilistic": True},
    "schemas": [CONFIG_SMALL_LLC, CONFIG_SMALL_PARCEL],
}

CONFIG_SMALL_INVALID = {
    "schemas": [CONFIG_SMALL_LLC, CONFIG_SMALL_PARCEL],
}


def test_validate_simple_schema():
    assert validate_config(CONFIG_SIMPLE) is True


def test_validate_small_schema():
    assert validate_config(CONFIG_SMALL) is True


def test_validate_invalid_schema():
    assert validate_config(CONFIG_SMALL_INVALID) is False
