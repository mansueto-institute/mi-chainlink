from unittest import mock

from chainlink.utils import create_config, validate_config

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

CONFIG_SIMPLE_CREATE = {
    "options": {
        "force_db_create": False,
        "export_tables": False,
        "update_config_only": False,
        "link_exclusions": [],
        "bad_address_path": None,
        "probabilistic": False,
        "load_only": False,
        "db_path": "db/linked.db",
        "probablistic": True,
    },
    "schemas": [
        {
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
    ],
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

CONFIG_SMALL_INVALID = {
    "schemas": [CONFIG_SMALL_LLC, CONFIG_SMALL_PARCEL],
}

CONFIG_NON_UNIQUE = {
    "options": {
        "db_path": "tests/db/test_non_unique.db",
        "force_db_create": True,
        "probabilistic": True,
    },
    "schemas": [
        {
            "schema_name": "llc",
            "tables": [
                {
                    "table_name": "master",
                    "table_name_path": "tests/data/small_llc.csv",
                    "id_col": "file_num",
                    "name_cols": ["name_raw"],
                    "address_cols": ["address"],
                },
                {
                    "table_name": "master2",
                    "table_name_path": "tests/data/small_llc2.csv",
                    "id_col": "id",
                    "name_cols": ["name_raw"],
                    "address_cols": ["address"],
                },
            ],
        }
    ],
}


def test_validate_simple_schema():
    assert validate_config(CONFIG_SIMPLE) is True


def test_validate_small_schema():
    assert validate_config(CONFIG_SMALL) is True


def test_validate_invalid_schema():
    assert validate_config(CONFIG_SMALL_INVALID) is False
    assert validate_config(CONFIG_NON_UNIQUE) is False


@mock.patch("chainlink.utils.Prompt.ask", create=True)
@mock.patch("chainlink.utils.Confirm.ask", create=True)
def test_create_config(
    mocked_confirm,
    mocked_ask,
):
    mocked_ask.side_effect = [
        "",  # config input
        "db/linked.db",  # db path
        "",  # bad address path
        "test_simple1",  # schema name
        "test1",  # table name
        "tests/data/test1.csv",  # table name path
        "id",  # id col
        "name",  # name cols
        "address",  # address cols
    ]
    mocked_confirm.side_effect = [
        False,  # load only
        True,  # probabilistic
        False,  # export
        True,  # new schema
        False,  # do not add another table
        False,  # do not add another schema
    ]
    config = create_config()

    assert config == CONFIG_SIMPLE_CREATE
