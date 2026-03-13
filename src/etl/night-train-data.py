import pandas as pd
import json
import os

BASE_PATH = "data/night-train-data/data/latest/"

def _load_json_to_dataframe(file_name):
    """Function to read and convert JSON into a Pandas dataframe."""
    file_path = os.path.join(BASE_PATH, file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Transformation into a DataFrame (the keys become the index, then the index is reset)
    df = pd.DataFrame.from_dict(data, orient='index')
    df.reset_index(drop=True, inplace=True)
    
    # Generic cleaning of empty columns if they exist
    if "" in df.columns:
        df.drop(columns=[""], inplace=True)
        
    df.replace("", pd.NA, inplace=True)
    df.dropna(how='all', inplace=True)
    df.fillna("", inplace=True)
    df.reset_index(drop=True, inplace=True)
        
    return df


def process_agencies(file_name="agencies.json"):
    return _load_json_to_dataframe(file_name)


def process_calendar_dates(file_name="calendar_dates.json"):
    df = _load_json_to_dataframe(file_name)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df


def process_calendar(file_name="calendar.json"):
    return _load_json_to_dataframe(file_name)


def process_classes(file_name="classes.json"):
    return _load_json_to_dataframe(file_name)


def process_routes(file_name="routes.json"):
    df = _load_json_to_dataframe(file_name)
    
    if 'route_short_name' in df.columns:
        df = df[df['route_short_name'] != " = "]
        
        df.reset_index(drop=True, inplace=True)
        
    return df


def process_stops(file_name="stops.json"):
    df = _load_json_to_dataframe(file_name)
    # Ensure that lat and lon are float
    for col in ['stop_lat', 'stop_lon']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def process_translations(file_name="translations.json"):
    return _load_json_to_dataframe(file_name)


def process_trip_stops(file_name="trip_stop.json"):
    df = _load_json_to_dataframe(file_name)
    # Converting hours to a time object
    for col in ['arrival_time', 'departure_time']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.time
            
    return df


def process_trips(file_name="trips.json"):
    df = _load_json_to_dataframe(file_name)
    # Cleaning hours and durations
    for col in ['origin_departure_time', 'destination_arrival_time', 'duration']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.time

    df['is_night_train'] = True
            
    return df


def process_view_ontd_cities(file_name="view_ontd_cities.json"):
    return _load_json_to_dataframe(file_name)


def process_view_ontd_details(file_name="view_ontd_details.json"):
    df = _load_json_to_dataframe(file_name)
    
    # Date conversion
    date_cols = ['start_date_0', 'end_date_0', 'start_date_1', 'end_date_1']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
    # Conversion of durations to Time object
    duration_cols = ['duration_0', 'duration_1']
    for col in duration_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.time    
    return df


def process_view_ontd_list(file_name="view_ontd_list.json"):
    # itinerary_long contains HTML (<br><br>), Pandas handles it as a regular string.
    return _load_json_to_dataframe(file_name)


def process_view_ontd_map(file_name="view_ontd_map.json"):
    df = _load_json_to_dataframe(file_name)
    
    time_columns = [
        'origin_departure_time_0',
        'destination_arrival_time_0',
        'origin_departure_time_1',
        'destination_arrival_time_1'
    ]
    
    for col in time_columns:
        if col in df.columns:
            # format='%H:%M' forces the exact reading "Hour:Minute"
            df[col] = pd.to_datetime(df[col], format='%H:%M', errors='coerce').dt.time
            
    return df


if __name__ == "__main__":
    
    # Dictionary mapping the logical name to its processing function
    etl_functions = {
        "agencies": process_agencies,
        "calendar_dates": process_calendar_dates,
        "calendar": process_calendar,
        "classes": process_classes,
        "routes": process_routes,
        "stops": process_stops,
        "translations": process_translations,
        "trip_stops": process_trip_stops,
        "trips": process_trips,
        "view_ontd_cities": process_view_ontd_cities,
        "view_ontd_details": process_view_ontd_details,
        "view_ontd_list": process_view_ontd_list,
        "view_ontd_map": process_view_ontd_map
    }
    
    # Dictionary that will store all our final dataframes in memory
    dataframes_collection = {}
    
    for name, func in etl_functions.items():
        print(f"Extraction et transformation de : {name}.json ...")
        try:
            # Calling the ETL function
            df = func()
            # Storage in the dictionary
            dataframes_collection[name] = df
            
            print(f"Succès : {df.shape[0]} lignes, {df.shape[1]} colonnes chargées.")
            
        except FileNotFoundError:
            print(f"Fichier {name}.json introuvable. Ignoré.")
        except Exception as e:
            print(f"Problème inattendu sur {name}.json : {str(e)}")
             
    df_routes = dataframes_collection.get("trips")
    # dataframes_collection["trips"].to_csv("view_ontqzdqzdd_details.csv", index=False, encoding="utf-8")
    if df_routes is not None:
        print(df_routes.head())