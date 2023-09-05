CREATE DATABASE loco_db;
\c loco_db;


CREATE TABLE IF NOT EXISTS cancel_code (
    cancel_code_id INT GENERATED ALWAYS AS IDENTITY,
    code text NOT NULL,
    reason text NOT NULL,
    PRIMARY KEY (cancel_code_id)
);

CREATE TABLE IF NOT EXISTS company (
    company_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    company_name TEXT NOT NULL,
    PRIMARY KEY (company_id)
);

CREATE TABLE IF NOT EXISTS station (
    station_id INT GENERATED ALWAYS AS IDENTITY,
    crs TEXT NOT NULL UNIQUE,
    station_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY (station_id)
);

CREATE TABLE IF NOT EXISTS service_type (
    service_type_id INT GENERATED ALWAYS AS IDENTITY,
    service_type_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY(service_type_id)
);
       
CREATE TABLE IF NOT EXISTS service_details (
    service_details_id INT GENERATED ALWAYS AS IDENTITY,
    company_id INT NOT NULL,
    -- service_uid TEXT,
    service_type_id INT NOT NULL,
    origin_station_id INT NOT NULL,
    destination_station_id INT NOT NULL,
    run_date TIMESTAMP NOT NULL,
    PRIMARY KEY (service_details_id)
);

CREATE TABLE IF NOT EXISTS delay_details (
    delay_details_id INT GENERATED ALWAYS AS IDENTITY,
    service_details_id INT,
    arrival_lateness SMALLINT
    scheduled_arrival TIMESTAMP,
    PRIMARY KEY (delay_details_id),
    FOREIGN KEY(service_details_id )
    REFERENCES service_details(service_details_id),
);

CREATE TABLE IF NOT EXISTS cancellation (
    cancellation_id INT GENERATED ALWAYS AS IDENTITY,
    service_details_id INT NOT NULL,
    -- station_id INT NOT NULL,
    cancellation_time TIMESTAMP,
    cancel_code_id INT NOT NULL,
    PRIMARY KEY (cancellation_id),
    FOREIGN KEY (service_details_id) REFERENCES service_details(service_details_id),
    FOREIGN KEY (cancel_code_id) REFERENCES cancel_code(cancel_code_id)
);