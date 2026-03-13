import pandas as pd


def populate_fact_trajet_train(db_clean, db_warehouse):
    # Load all needed source tables
    trips = db_clean.get_data_from_table("trips")
    vod = db_clean.get_data_from_table("view_ontd_details")
    pt = db_clean.get_data_from_table("passenger_transported")
    print(f"Trips: {len(trips)}, VOD: {len(vod)}, Passengers: {len(pt)}")

    # Load warehouse dimensions
    dim_route = db_warehouse.get_data_from_table("dim_route")
    dim_operateur = db_warehouse.get_data_from_table("dim_operateur")
    dim_gare = db_warehouse.get_data_from_table("dim_gare")
    dim_date = db_warehouse.get_data_from_table("dim_date")

    # ---- Prepare trips ----
    trips["route_id_int"] = pd.to_numeric(trips["route_id"].astype(
        str).str.strip(), errors="coerce").astype("Int64")
    trips["country_first"] = trips["countries"].astype(
        str).str.split(",").str[0].str.strip()
    trips["country_last"] = trips["countries"].astype(
        str).str.split(",").str[-1].str.strip()

    # Filter: trip_id not null, route_id in dim_route, agency_id in dim_operateur
    valid_routes = set(dim_route["route_id"].astype(int))
    valid_agencies = set(dim_operateur["agency_id"].astype(str))

    trips = trips[
        trips["trip_id"].notna() &
        trips["route_id_int"].notna() &
        trips["route_id_int"].astype(int).isin(valid_routes) &
        trips["agency_id"].astype(str).isin(valid_agencies)
    ].copy()
    print(f"Trips après filtres FK : {len(trips)}")

    # ---- Prepare passengers: most recent year per geo ----
    pt["obs_value_float"] = pd.to_numeric(
        pt["obs_value"].astype(str).str.strip(), errors="coerce")
    pt = pt.dropna(subset=["obs_value_float"])
    pt["TIME_PERIOD_int"] = pd.to_numeric(
        pt["TIME_PERIOD"].astype(str).str.strip(), errors="coerce")
    pt_latest = (
        pt.sort_values("TIME_PERIOD_int", ascending=False)
        .drop_duplicates(subset=["geo"], keep="first")
        [["geo", "obs_value_float"]]
        .rename(columns={"obs_value_float": "passengers"})
    )

    # ---- Prepare vod: one row per route_id ----
    vod["route_id_int"] = pd.to_numeric(vod["route_id"].astype(
        str).str.strip(), errors="coerce").astype("Int64")
    vod["start_date_0_parsed"] = pd.to_datetime(vod["start_date_0"].astype(str).str.strip(), errors="coerce", utc=True).dt.date
    vod["end_date_0_parsed"]   = pd.to_datetime(vod["end_date_0"].astype(str).str.strip(), errors="coerce", utc=True).dt.date
    vod["average_speed_float"] = pd.to_numeric(vod["average_speed"].astype(str).str.strip(), errors="coerce")
    vod_dedup = vod.drop_duplicates(subset=["route_id_int"], keep="first")

    # ---- Prepare dim_date lookup ----
    dim_date["start_date"] = pd.to_datetime(dim_date["start_date"], errors="coerce", utc=True).dt.date
    dim_date["end_date"]   = pd.to_datetime(dim_date["end_date"], errors="coerce", utc=True).dt.date

    # ---- Prepare dim_gare lookup ----
    dim_gare["name"]    = dim_gare["name"].astype(str).str.strip()
    dim_gare["country"] = dim_gare["country"].astype(str).str.strip()

    # ---- Merge trips → vod ----
    df = trips.merge(
        vod_dedup[["route_id_int", "start_date_0_parsed", "end_date_0_parsed", "average_speed_float"]],
        left_on="route_id_int", right_on="route_id_int", how="left"
    )

    # ---- Merge → dim_date ----
    df = df.merge(
        dim_date[["date_id", "start_date", "end_date"]],
        left_on=["start_date_0_parsed", "end_date_0_parsed"],
        right_on=["start_date", "end_date"],
        how="left"
    )

    # ---- Merge → dim_gare departure ----
    df = df.merge(
        dim_gare[["gare_id", "name", "country"]].rename(columns={"gare_id": "gare_depart_id"}),
        left_on=["trip_origin", "country_first"],
        right_on=["name", "country"],
        how="left"
    )

    # ---- Merge → dim_gare arrival ----
    df = df.merge(
        dim_gare[["gare_id", "name", "country"]].rename(columns={"gare_id": "gare_arrivee_id"}),
        left_on=["trip_headsign", "country_last"],
        right_on=["name", "country"],
        how="left",
        suffixes=("_dep", "_arr")
    )

    # ---- Merge → passengers ----
    df = df.merge(pt_latest, left_on="country_first", right_on="geo", how="left")

    # ---- Measures ----
    df["distance_km"]   = pd.to_numeric(df["distance"].astype(str).str.strip(), errors="coerce")
    df["duree_minutes"] = pd.to_timedelta(df["duration"].astype(str).str.strip(), errors="coerce").dt.total_seconds() / 60
    df["emissions_co2"] = pd.to_numeric(df["emissions_co2e"].astype(str).str.strip(), errors="coerce")

    # ---- Build final df ----
    fact = pd.DataFrame({
        "train_id":       df["trip_id"],
        "route_id":       df["route_id_int"].astype(int),
        "operator_id":    df["agency_id"],
        "gare_depart_id": df["gare_depart_id"],
        "gare_arrivee_id":df["gare_arrivee_id"],
        "date_id":        df["date_id"],
        "distance_km":    df["distance_km"],
        "duree_minutes":  df["duree_minutes"],
        "emissions_co2":  df["emissions_co2"],
        "passengers":     df["passengers"],
        "average_speed":  df["average_speed_float"],
    })

    # Convert nullable int columns
    for col in ["gare_depart_id", "gare_arrivee_id", "date_id"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").astype("Int64")

    fact = fact.dropna(subset=["train_id"]).reset_index(drop=True)
    print(f"Lignes fact_trajet_train à insérer : {len(fact)}")

    # fact table is append-only — use plain insert (no conflict key)
    fact.to_sql(
        "fact_trajet_train",
        db_warehouse.engine,
        schema="tpre612_data_warehouse",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    print("fact_trajet_train OK")