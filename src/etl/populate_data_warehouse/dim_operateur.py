import pandas as pd

def populate_dim_operateur(db_clean, db_warehouse):
    agency = db_clean.get_data_from_table("agencies")  # ✅ "agencies" not "agency"
    print(f"Agency bruts : {len(agency)}")

    df = pd.DataFrame({
        "agency_id":       agency["agency_id"].astype(str).str.strip(),
        "agency_name":     agency["agency_name"],
        "agency_url":      agency["agency_url"],
        "agency_timezone": agency["agency_timezone"],
        "agency_lang":     agency["agency_lang"] if "agency_lang" in agency.columns else None,
    }).dropna(subset=["agency_id"]).drop_duplicates(subset=["agency_id"], keep="first")

    print(f"Lignes dim_operateur à upsert : {len(df)}")
    db_warehouse.upsert(df, "dim_operateur", conflict_columns=["agency_id"], schema="tpre612_data_warehouse")
    print("dim_operateur OK")
