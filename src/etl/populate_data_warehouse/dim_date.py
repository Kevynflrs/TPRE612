import pandas as pd

def populate_dim_date(db_clean, db_warehouse):
    vod = db_clean.get_data_from_table("view_ontd_details")
    print(f"view_ontd_details bruts : {len(vod)}")

    def to_bool(series):
        return pd.to_numeric(series.astype(str).str.strip(), errors="coerce").fillna(0).astype(int) == 1

    def to_date(series):
        return pd.to_datetime(series.astype(str).str.strip(), errors="coerce", utc=True).dt.date

    # Period 0
    df0 = pd.DataFrame({
        "start_date": to_date(vod["start_date_0"]),
        "end_date":   to_date(vod["end_date_0"]),
        "monday":     to_bool(vod["monday_0"]),
        "tuesday":    to_bool(vod["tuesday_0"]),
        "wednesday":  to_bool(vod["wednesday_0"]),
        "thursday":   to_bool(vod["thursday_0"]),
        "friday":     to_bool(vod["friday_0"]),
        "saturday":   to_bool(vod["saturday_0"]),
        "sunday":     to_bool(vod["sunday_0"]),
    }).dropna(subset=["start_date", "end_date"])

    # Period 1
    df1 = pd.DataFrame({
        "start_date": to_date(vod["start_date_1"]),
        "end_date":   to_date(vod["end_date_1"]),
        "monday":     to_bool(vod["monday_1"]),
        "tuesday":    to_bool(vod["tuesday_1"]),
        "wednesday":  to_bool(vod["wednesday_1"]),
        "thursday":   to_bool(vod["thursday_1"]),
        "friday":     to_bool(vod["friday_1"]),
        "saturday":   to_bool(vod["saturday_1"]),
        "sunday":     to_bool(vod["sunday_1"]),
    }).dropna(subset=["start_date", "end_date"])

    df = (
        pd.concat([df0, df1], ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    print(f"Lignes dim_date à upsert : {len(df)}")

    # dim_date uses SERIAL PK with no natural unique constraint —
    # add one first if you want upsert, otherwise plain insert is fine
    db_warehouse.upsert(
        df=df,
        table_name="dim_date",
        conflict_columns=["start_date", "end_date", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
        schema="tpre612_data_warehouse"
    )
    print("dim_date OK")