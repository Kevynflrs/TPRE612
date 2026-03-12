CREATE TABLE dim_operateur (
    agency_id TEXT PRIMARY KEY,
    agency_name TEXT,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT
);

CREATE TABLE dim_route (
    route_id INT PRIMARY KEY,
    agency_id TEXT REFERENCES dim_operateur(agency_id),
    route_long_name TEXT,
    origin TEXT,
    destination TEXT,
    countries TEXT
);

CREATE TABLE dim_train (
    trip_id TEXT PRIMARY KEY,
    route_id INT REFERENCES dim_route(route_id),
    trip_headsign TEXT,
    trip_origin TEXT,
    destination_arrival_time TIME,
    duration INTERVAL,
    distance FLOAT
);

CREATE TABLE dim_gare (
    gare_id SERIAL PRIMARY KEY,
    name TEXT,
    city TEXT,
    country TEXT,
    latitude FLOAT,
    longitude FLOAT,
    is_main_station BOOLEAN
);

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    start_date DATE,
    end_date DATE,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN
);

CREATE TABLE dim_energie (
    energy_id SERIAL PRIMARY KEY,
    geo TEXT,
    vehicle TEXT,
    energy_type TEXT,
    year INT,
    energy_value FLOAT
);

CREATE TABLE fact_trajet_train (
    fact_id SERIAL PRIMARY KEY,
    train_id TEXT REFERENCES dim_train(trip_id),
    route_id INT REFERENCES dim_route(route_id),
    operator_id TEXT REFERENCES dim_operateur(agency_id),
    gare_depart_id INT REFERENCES dim_gare(gare_id),
    gare_arrivee_id INT REFERENCES dim_gare(gare_id),
    date_id INT REFERENCES dim_date(date_id),
    distance_km FLOAT,
    duree_minutes FLOAT,
    emissions_co2 FLOAT,
    passengers FLOAT,
    average_speed FLOAT
);