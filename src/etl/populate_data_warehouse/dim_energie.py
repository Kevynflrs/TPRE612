import pandas as pd

def populate_dim_energie(db_clean, db_warehouse):
    energy = db_clean.get_data_from_table("train_traffic_source_energy")
    print(f"train_traffic_source_energy bruts : {len(energy)}")

    df = pd.DataFrame({
        "geo":          energy["geo"],
        "vehicle":      energy["vehicle"],
        "energy_type":  energy["mot_nrg"],
        "year":         pd.to_numeric(energy["TIME_PERIOD"].astype(str).str.strip(), errors="coerce").astype("Int64"),
        "energy_value": pd.to_numeric(energy["obs_value"].astype(str).str.strip(), errors="coerce"),
    }).dropna(subset=["energy_value"])

    df = df.drop_duplicates(subset=["geo", "vehicle", "energy_type", "year"]).reset_index(drop=True)
    df["year"] = df["year"].astype(int)

    print(f"Lignes dim_energie à upsert : {len(df)}")

    db_warehouse.upsert(
        df=df,
        table_name="dim_energie",
        conflict_columns=["geo", "vehicle", "energy_type", "year"],
        schema="tpre612_data_warehouse"
    )
    print("dim_energie OK")