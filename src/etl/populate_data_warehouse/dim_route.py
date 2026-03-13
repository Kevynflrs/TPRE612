import pandas as pd


def populate_dim_route(db_clean, db_warehouse):
    routes = db_clean.get_data_from_table("routes")
    print(f"Routes brutes : {len(routes)}")

    valid_agencies = set(db_warehouse.get_data_from_table("dim_operateur")["agency_id"].tolist())
    print(f"Agences valides : {len(valid_agencies)}")

    df = pd.DataFrame({
        "route_id":        pd.to_numeric(routes["route_id"].astype(str).str.strip(), errors="coerce").astype("Int64"),
        "agency_id":       routes["agency_id"].astype(str).str.strip(),
        "route_long_name": routes["route_long_name"],
        "origin":          routes["origin_trip_0"] if "origin_trip_0" in routes.columns else None,
        "destination":     routes["destination_trip_0"] if "destination_trip_0" in routes.columns else None,
        "countries":       routes["countries"] if "countries" in routes.columns else None,
    })

    df = (
        df.dropna(subset=["route_id", "agency_id"])
        .loc[lambda x: x["agency_id"].isin(valid_agencies)]
        .drop_duplicates(subset=["route_id"], keep="first")
        .reset_index(drop=True)
    )
    df["route_id"] = df["route_id"].astype(int)

    print(f"Lignes dim_route à upsert : {len(df)}")
    db_warehouse.upsert(df, "dim_route", conflict_columns=["route_id"], schema="tpre612_data_warehouse")
    print("dim_route OK")
