import polars as pl
from pathlib import Path

cur_dir = Path(__file__).parent
data_dir = cur_dir / "data"

def class_combos(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("class").is_in(["299", "399"])).then(pl.lit("condos"))
        .otherwise(
            pl.when(pl.col("class").str.starts_with("2")).then(pl.lit("200s"))
            .otherwise(
                pl.when(pl.col("class").str.starts_with("3")).then(pl.lit("300s"))
                .otherwise(pl.lit("other"))
            )
        ).alias("class_combo")
    ).group_by("class_combo").agg(pl.len()).sort("class_combo")


if __name__ == "__main__":
    df_2006 = pl.scan_parquet(data_dir / "dfc_fulladd_2006_correct_address.parquet")
    df_2022 = pl.scan_parquet(data_dir / "parcel-addresses-foia-24-0230.parquet")
    df_2022 = df_2022.with_columns(pl.col("pin").str.replace_all("-", "").alias("pin"))
    print(df_2006.collect_schema())
    print(df_2006.select(pl.len()).collect())
    print(df_2022.collect_schema())
    print(df_2022.select(pl.len()).collect())

    df_2006 = df_2006.select("pin", "mailing_address", "property_address", "tax_payer_name")
    df_2022 = df_2022.select("pin", "mailing_address", "property_address", "tax_payer_name")
    print(df_2006.head().collect())
    print(df_2022.head().collect())


    ptax_2006 = pl.scan_parquet(data_dir / "parcel_ptaxsim.parquet")
    ptax_2022 = pl.scan_parquet(data_dir / "parcel_ptaxsim_22.parquet")
    print(ptax_2006.collect_schema())
    print(ptax_2006.select(pl.len()).collect())
    print(ptax_2022.collect_schema())
    print(ptax_2022.select(pl.len()).collect())

    parcels_2022 = ptax_2022.join(df_2022, on="pin", how="inner")
    print(parcels_2022.head().collect())
    parcels_2006 = ptax_2006.join(df_2006, on="pin", how="inner")
    print(parcels_2006.head().collect())

    parcels_2006.sink_parquet(data_dir / "raw_2006_parcels.parquet")
    parcels_2022.sink_parquet(data_dir / "raw_2022_parcels.parquet")

    common_pins = parcels_2006.join(parcels_2022, on="pin", how="inner")
    print(common_pins.select(pl.len()).collect())
    common_pins_class = parcels_2006.join(parcels_2022, on=["pin", "class"], how="inner")
    print(common_pins_class.select(pl.len()).collect())
    

    ## WE only keep pins with the same class in both years for the combined analysis


    lim_ptax_2006_2006 = parcels_2006.join(common_pins_class.select("pin"), on="pin", how="inner")
    lim_ptax_2022_2022 = parcels_2022.join(common_pins_class.select("pin"), on="pin", how="inner")
    print(lim_ptax_2006_2006.select(pl.len()).collect())
    print(lim_ptax_2022_2022.select(pl.len()).collect())

    lim_ptax_2006_2006.sink_parquet(data_dir / "lim_2006_2006_parcels.parquet")
    lim_ptax_2022_2022.sink_parquet(data_dir / "lim_2022_2022_parcels.parquet")

    lim_ptax_2006_2022 = lim_ptax_2006_2006.select(pl.exclude("property_address")).join(
        lim_ptax_2022_2022.select("pin", "property_address"), on="pin", how="inner"
    )
    lim_ptax_2022_2006 = lim_ptax_2022_2022.select(pl.exclude("property_address")).join(
        lim_ptax_2006_2006.select("pin", "property_address"), on="pin", how="inner"
    )
    print(lim_ptax_2006_2022.head().collect())
    print(lim_ptax_2022_2006.head().collect())

    lim_ptax_2006_2022.sink_parquet(data_dir / "lim_2006_2022_parcels.parquet")
    lim_ptax_2022_2006.sink_parquet(data_dir / "lim_2022_2006_parcels.parquet")


    print("Total pins and class combos in each dataset:")
    print("raw 2006:")
    print(parcels_2006.select(pl.len()).collect())
    print(class_combos(parcels_2006).collect())
    print("raw 2022:")
    print(parcels_2022.select(pl.len()).collect())
    print(class_combos(parcels_2022).collect())
    print("lim--")
    print("2006:")
    print(lim_ptax_2006_2006.select(pl.len()).collect())
    print(class_combos(lim_ptax_2006_2006).collect())
    print("2022:")
    print(lim_ptax_2022_2022.select(pl.len()).collect())
    print(class_combos(lim_ptax_2022_2022).collect())
    print("2006 -> 2022:")
    print(lim_ptax_2006_2022.select(pl.len()).collect())
    print(class_combos(lim_ptax_2006_2022).collect())
    print("2022 -> 2006:")  
    print(lim_ptax_2022_2006.select(pl.len()).collect())
    print(class_combos(lim_ptax_2022_2006).collect())

    

