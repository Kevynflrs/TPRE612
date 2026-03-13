import pandas as pd

def populate_dim_gare(db_clean, db_warehouse):
    gares = db_clean.get_data_from_table("gares_europeennes")
    stops = db_clean.get_data_from_table("stops")
    print(f"Gares europeennes: {len(gares)}, Stops: {len(stops)}")

    df_gares = pd.DataFrame({
        "name":            gares["name"],
        "city":            gares["name"],
        "country":         gares["country"],
        "latitude":        pd.to_numeric(gares["latitude"].astype(str).str.strip(), errors="coerce"),
        "longitude":       pd.to_numeric(gares["longitude"].astype(str).str.strip(), errors="coerce"),
        "is_main_station": gares["is_main_station"] == "t",
    }).dropna(subset=["name", "latitude", "longitude"])

    df_stops = pd.DataFrame({
        "name":            stops["stop_name"],
        "city":            stops["stop_cityname"].fillna(stops["stop_name"]),
        "country":         stops["stop_country"],
        "latitude":        pd.to_numeric(stops["stop_lat"].astype(str).str.strip(), errors="coerce"),
        "longitude":       pd.to_numeric(stops["stop_lon"].astype(str).str.strip(), errors="coerce"),
        "is_main_station": False,
    }).dropna(subset=["name", "latitude", "longitude"])

    df = (
        pd.concat([df_gares, df_stops], ignore_index=True)
        .drop_duplicates(subset=["name", "country"], keep="first")
        .reset_index(drop=True)
    )

    print(f"Lignes dim_gare à upsert : {len(df)}")
    db_warehouse.upsert(df, "dim_gare", conflict_columns=["name", "country"], schema="tpre612_data_warehouse")
    print("dim_gare OK")