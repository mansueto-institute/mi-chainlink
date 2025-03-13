from src.linkage.cleaning.cleaning_functions import (
    clean_address,
    clean_names,
    clean_zipcode,
    identify_state_city,
    predict_org,
)


############################## test functions
def test_predict_org():
    assert predict_org("OLIMP FLOORING INC.") == 1
    assert predict_org("718 MULFORD AVENUE CONDOMINIUM ASSOCIATION") == 1
    assert predict_org("TAXPAYER OF") == 0
    assert predict_org("Joe DiMaggio") == 0
    assert predict_org("ELLI D COSKY TRUST") == 1
    assert predict_org("D & L HALFMAN") == 0
    assert predict_org("D & L HALFMAN INC.") == 1


def test_clean_zipcode():
    assert clean_zipcode(0) == "0"
    assert clean_zipcode("") == ""
    assert clean_zipcode(123456789) == "12345"
    assert clean_zipcode("abcdefghijk") == "abcde"


def test_identify_state_city_int_input():
    assert identify_state_city(60615) == ("CHICAGO", "IL")
    assert identify_state_city(0) == (None, None)
    assert identify_state_city(6061578536) == ("CHICAGO", "IL")


def test_identify_state_city_str_input():
    assert identify_state_city("60615") == ("CHICAGO", "IL")
    assert identify_state_city("6061599647") == ("CHICAGO", "IL")
    assert identify_state_city("80126") == ("LITTLETON", "CO")
    assert identify_state_city("abcdefg") == (None, None)
    assert identify_state_city("123xyz") == (None, None)


def test_identify_state_city_invalid_input():
    assert identify_state_city(1.5) == (None, None)
    assert identify_state_city(-1) == (None, None)
    assert identify_state_city(1234) == (None, None)
    assert identify_state_city("1234") == (None, None)


def test_clean_address_correct_address():
    assert clean_address("123 E Hyde Park Blvd Apt. 15 Chicago, IL 60615") == {
        "street": "123 E HYDE PARK BLVD",
        "address_number": "123",
        "street_pre_directional": "E",
        "street_name": "HYDE PARK",
        "street_post_type": "BLVD",
        "unit_type": "APT",
        "unit_number": "15",
        "city": "CHICAGO",
        "state": "IL",
        "postal_code": "60615",
        "subaddress_identifier": None,
        "subaddress_type": None,
    }


def test_clean_address_empty():
    assert clean_address("") == {
        "street": None,
        "address_number": None,
        "street_pre_directional": None,
        "street_name": None,
        "street_post_type": None,
        "unit_type": None,
        "unit_number": None,
        "city": None,
        "state": None,
        "postal_code": None,
        "subaddress_identifier": None,
        "subaddress_type": None,
    }


def test_clean_address_zipcode_mismatch():
    assert clean_address("123 E Hyde Park Blvd #15 Whoeville, Grinchtown 60615") == {
        "street": "123 E HYDE PARK BLVD",
        "address_number": "123",
        "street_pre_directional": "E",
        "street_name": "HYDE PARK",
        "street_post_type": "BLVD",
        "unit_type": "UNIT",
        "unit_number": "15",
        "city": "CHICAGO",
        "state": "IL",
        "postal_code": "60615",
        "subaddress_identifier": None,
        "subaddress_type": None,
    }
    assert clean_address("123 E Hyde Park Blvd #15 Chicago, IL 12345") == {
        "street": "123 E HYDE PARK BLVD",
        "address_number": "123",
        "street_pre_directional": "E",
        "street_name": "HYDE PARK",
        "street_post_type": "BLVD",
        "unit_type": "UNIT",
        "unit_number": "15",
        "city": "CHICAGO",
        "state": "IL",
        "postal_code": "12345",
        "subaddress_identifier": None,
        "subaddress_type": None,
    }


def test_clean_address_missing_parts():
    assert clean_address("123 E Hyde Park Blvd # 15 Whoeville, Grinchtown 60615") == {
        "street": "123 E HYDE PARK BLVD",
        "address_number": "123",
        "street_pre_directional": "E",
        "street_name": "HYDE PARK",
        "street_post_type": "BLVD",
        "unit_type": "UNIT",
        "unit_number": "15",
        "city": "CHICAGO",
        "state": "IL",
        "postal_code": "60615",
        "subaddress_identifier": None,
        "subaddress_type": None,
    }


def test_clean_address_irregular():
    assert clean_address("US STEEL TWR 600 GRANT 44TH FL PITTSBURGH PA 15219") == {
        "street": None,
        "address_number": None,
        "street_pre_directional": None,
        "street_name": None,
        "street_post_type": None,
        "unit_type": None,
        "unit_number": None,
        "city": "PITTSBURGH",
        "state": "PA",
        "postal_code": "15219",
        "subaddress_identifier": "44TH",
        "subaddress_type": "FL",
    }
    assert clean_address("2851 JOHN STREET, SUITE ONE MARKHAM, ONTARIO AO L3R 5") == {
        "street": "2851 JOHN ST",
        "address_number": "2851",
        "street_pre_directional": None,
        "street_name": "JOHN",
        "street_post_type": "ST",
        "unit_type": "UNIT",
        "unit_number": "L3R5SUITE",
        "city": None,
        "state": None,
        "postal_code": None,
        "subaddress_identifier": None,
        "subaddress_type": None,
    }
    assert clean_address("POB 362 CLAY CITY IL 62824") == {
        "street": None,
        "address_number": None,
        "street_pre_directional": None,
        "street_name": None,
        "street_post_type": None,
        "unit_type": None,
        "unit_number": None,
        "city": "CLAY CITY",
        "state": "IL",
        "postal_code": "62824",
        "subaddress_identifier": None,
        "subaddress_type": None,
    }
    assert clean_address("1 WESTBROOK CORPORATE CENTER, SUITE #300 WESTCHESTER IL 60154") == {
        "street": None,
        "address_number": None,
        "street_pre_directional": None,
        "street_name": None,
        "street_post_type": None,
        "unit_type": "UNIT",
        "unit_number": "300",
        "city": "WESTCHESTER",
        "state": "IL",
        "postal_code": "60154",
        "subaddress_identifier": None,
        "subaddress_type": None,
    }


def test_clean_name_correct():
    assert clean_names("Joe DiMaggio") == "JOE DIMAGGIO"


def test_clean_names_excluded():
    assert clean_names("VACANT") is None
    assert clean_names("MERGED") is None
    assert clean_names("SAME AS ABOVE") is None
    assert clean_names("TAX PAYER") is None


def test_clean_names_punct():
    assert clean_names("J. TRAVIS DOWELL") == "J TRAVIS DOWELL"
    assert clean_names("DR. HORTON, INC-MIDWES") == "DR HORTON INC MIDWES"
    assert clean_names("Mr. & Mrs. J. Schuman") == "MR AND MRS J SCHUMAN"
    assert clean_names("CRAFTN' WIT FASHN' L.L.C.") == "CRAFTN WIT FASHN LLC"
    assert clean_names("M&M ROMEOVILLE, LLC") == "MANDM ROMEOVILLE LLC"
    assert clean_names("TOM'S QUALITY AUTO REPAIR, INC.") == "TOMS QUALITY AUTO REPAIR INC"


def test_clean_names_spaces():
    assert clean_names("US BUILDERS  SERIES 15") == "US BUILDERS SERIES 15"
    assert clean_names("US BUILDERS   SERIES 15") == "US BUILDERS SERIES 15"
    assert clean_names("US BUILDERS    SERIES 15") == "US BUILDERS SERIES 15"
    assert clean_names("US BUILDERS SERIES 15") == "US BUILDERS SERIES 15"
    assert clean_names(" ") is None
    assert clean_names("  ") is None
    assert clean_names("   ") is None
    assert clean_names("") is None


def test_clean_names_irregular():
    assert clean_names("CIESLIK RYSZARD2112245") == "CIESLIK RYSZARD2112245"
