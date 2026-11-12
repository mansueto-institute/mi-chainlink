# %%

import os
import duckdb
import polars as pl

years = range(1999, 2023)

temp_names = {
    "main_parcel_mailing_address_full_main_parcel_property_address_full_unit_fuzzy_match": "og_mailing_prop_unit_fuzzy",
    "main_parcel_mailing_address_full_main_parcel_property_address_full_street_fuzzy_match": "og_mailing_prop_street_fuzzy",
    "main_parcel_property_address_full_main_parcel_mailing_address_full_unit_fuzzy_match": "og_prop_mailing_unit_fuzzy",
    "main_parcel_property_address_full_main_parcel_mailing_address_full_street_fuzzy_match": "og_prop_mailing_street_fuzzy",
    "main_parcel_property_address_full_main_parcel_mailing_address_full_street_match": "og_prop_mailing_street",
    "main_parcel_property_address_full_main_parcel_mailing_address_full_address_match": "og_prop_mailing_address",
    "main_parcel_property_address_full_main_parcel_mailing_address_full_unit_match": "og_prop_mailing_unit",
    "main_parcel_mailing_address_full_main_parcel_property_address_full_street_match": "og_mailing_prop_street",
    "main_parcel_mailing_address_full_main_parcel_property_address_full_address_match": "og_mailing_prop_address",
    "main_parcel_mailing_address_full_main_parcel_property_address_full_unit_match": "og_mailing_prop_unit",
}

correct_name = {
    "og_mailing_prop_unit_fuzzy": "main_parcel_property_address_full_main_parcel_mailing_address_full_unit_fuzzy_match",
    "og_mailing_prop_street_fuzzy": "main_parcel_property_address_full_main_parcel_mailing_address_full_street_fuzzy_match",
    "og_prop_mailing_unit_fuzzy": "main_parcel_mailing_address_full_main_parcel_property_address_full_unit_fuzzy_match",
    "og_prop_mailing_street_fuzzy": "main_parcel_mailing_address_full_main_parcel_property_address_full_street_fuzzy_match",
    "og_prop_mailing_street": "main_parcel_mailing_address_full_main_parcel_property_address_full_street_match",
    "og_prop_mailing_address": "main_parcel_mailing_address_full_main_parcel_property_address_full_address_match",
    "og_prop_mailing_unit": "main_parcel_mailing_address_full_main_parcel_property_address_full_unit_match",
    "og_mailing_prop_street": "main_parcel_property_address_full_main_parcel_mailing_address_full_street_match",
    "og_mailing_prop_address": "main_parcel_property_address_full_main_parcel_mailing_address_full_address_match",
    "og_mailing_prop_unit": "main_parcel_property_address_full_main_parcel_mailing_address_full_unit_match",
}
# %%
ps = []
for year in years:
    print(year)
    db_path = f"db/linked_parcel_historic_{year}.db"
    assert os.path.exists(db_path)
    conn = duckdb.connect(db_path)
    df = conn.execute("select * from link.main_main").pl()
    maindf = conn.execute("select * from main.parcel").pl()
    conn.close()
    p1 = df.select(pl.exclude("main_pin_2")).group_by("main_pin_1").agg(pl.sum("*")).rename({"main_pin_1": "pin"})
    p2 = df.select(pl.exclude("main_pin_1")).group_by("main_pin_2").agg(pl.sum("*")).rename({"main_pin_2": "pin"})
    p2 = p2.rename(temp_names)
    p2 = p2.rename(correct_name)
    p = pl.concat([p1, p2], how="diagonal")
    p = p.group_by("pin").agg(pl.sum("*"))
    maindf = maindf.select(
        pl.exclude(
            "mailing_address_full_address_id", "mailing_address_full_street_id", "mailing_address_full_street_name_id","mailing_address_full_address_norm", "mailing_address_full_skip", "mailing_name_name_id",
            "property_address_full_address_id", "property_address_full_street_id", "property_address_full_street_name_id", "property_address_full_address_norm", "property_address_full_skip")
    ).join(p, on="pin",how="left").with_columns(pl.col("^.*_match$").fill_null(0))
    ps.append(maindf)
    del df
    del p
    del p1
    del p2

ps_all = pl.concat(ps)
del ps
ps_all.write_parquet("data/dfc_fulladd_counts_pmn.parquet")
#%%
