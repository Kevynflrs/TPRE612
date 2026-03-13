CREATE TABLE tpre612_data_warehouse.dim_operateur (
    agency_id TEXT PRIMARY KEY,
    agency_name TEXT,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT
);

CREATE TABLE tpre612_data_warehouse.dim_route (
    route_id INT PRIMARY KEY,
    agency_id TEXT REFERENCES tpre612_data_warehouse.dim_operateur(agency_id),
    route_long_name TEXT,
    origin TEXT,
    destination TEXT,
    countries TEXT
);

CREATE TABLE tpre612_data_warehouse.dim_train (
    trip_id TEXT PRIMARY KEY,
    route_id INT REFERENCES tpre612_data_warehouse.dim_route(route_id),
    trip_headsign TEXT,
    trip_origin TEXT,
    destination_arrival_time TIME,
    duration INTERVAL,
    distance FLOAT,
    is_night_train BOOLEAN
);

CREATE TABLE tpre612_data_warehouse.dim_gare (
    gare_id SERIAL PRIMARY KEY,
    name TEXT,
    city TEXT,
    country TEXT,
    latitude FLOAT,
    longitude FLOAT,
    is_main_station BOOLEAN
);

CREATE TABLE tpre612_data_warehouse.dim_date (
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

CREATE TABLE tpre612_data_warehouse.dim_energie (
    energy_id SERIAL PRIMARY KEY,
    geo TEXT,
    vehicle TEXT,
    energy_type TEXT,
    year INT,
    energy_value FLOAT
);

CREATE TABLE tpre612_data_warehouse.fact_trajet_train (
    fact_id SERIAL PRIMARY KEY,
    train_id TEXT REFERENCES tpre612_data_warehouse.dim_train(trip_id),
    route_id INT REFERENCES tpre612_data_warehouse.dim_route(route_id),
    operator_id TEXT REFERENCES tpre612_data_warehouse.dim_operateur(agency_id),
    gare_depart_id INT REFERENCES tpre612_data_warehouse.dim_gare(gare_id),
    gare_arrivee_id INT REFERENCES tpre612_data_warehouse.dim_gare(gare_id),
    date_id INT REFERENCES tpre612_data_warehouse.dim_date(date_id),
    distance_km FLOAT,
    duree_minutes FLOAT,
    emissions_co2 FLOAT,
    passengers FLOAT,
    average_speed FLOAT,
    is_night_train BOOLEAN
);


-- dim_gare: needs unique on (name, country)
ALTER TABLE tpre612_data_warehouse.dim_gare
ADD CONSTRAINT uq_dim_gare_name_country UNIQUE (name, country);

-- dim_date: needs unique on the combination of fields
ALTER TABLE tpre612_data_warehouse.dim_date
ADD CONSTRAINT uq_dim_date UNIQUE (start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday);

-- dim_energie: needs unique on its natural key
ALTER TABLE tpre612_data_warehouse.dim_energie
ADD CONSTRAINT uq_dim_energie UNIQUE (geo, vehicle, energy_type, year);

