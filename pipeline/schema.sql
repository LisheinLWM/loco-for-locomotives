DROP DATABASE IF EXISTS loco_db;
CREATE DATABASE loco_db;

\c loco_db;

CREATE SCHEMA previous_day_data;
CREATE SCHEMA all_data;

SET search_path TO previous_day_data;

CREATE TABLE IF NOT EXISTS cancel_code (
    cancel_code_id INT GENERATED ALWAYS AS IDENTITY,
    code TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL,
    abbreviation TEXT,
    PRIMARY KEY (cancel_code_id)
);

CREATE TABLE IF NOT EXISTS company (
    company_id INT GENERATED ALWAYS AS IDENTITY,
    company_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY (company_id)
);

CREATE TABLE IF NOT EXISTS station (
    station_id INT GENERATED ALWAYS AS IDENTITY,
    crs TEXT NOT NULL UNIQUE,
    station_name TEXT NOT NULL,
    PRIMARY KEY (station_id)
);

CREATE TABLE IF NOT EXISTS service_type (
    service_type_id INT GENERATED ALWAYS AS IDENTITY,
    service_type_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY(service_type_id)
);

CREATE TABLE IF NOT EXISTS service_details (
    service_details_id INT GENERATED ALWAYS AS IDENTITY,
    service_uid TEXT NOT NULL,
    company_id INT NOT NULL,
    service_type_id INT NOT NULL,
    origin_station_id INT NOT NULL,
    destination_station_id INT NOT NULL,
    run_date TIMESTAMP NOT NULL,
    PRIMARY KEY (service_details_id),
    FOREIGN KEY (company_id) REFERENCES company(company_id),
    FOREIGN KEY (service_type_id) REFERENCES service_type(service_type_id),
    FOREIGN KEY (origin_station_id) REFERENCES station(station_id),
    FOREIGN KEY (destination_station_id) REFERENCES station(station_id)
);

CREATE TABLE IF NOT EXISTS delay_details (
    delay_details_id INT GENERATED ALWAYS AS IDENTITY,
    service_details_id INT NOT NULL,
    arrival_lateness SMALLINT,
    scheduled_arrival TIMESTAMP,
    PRIMARY KEY (delay_details_id),
    FOREIGN KEY(service_details_id) REFERENCES service_details(service_details_id)
);

CREATE TABLE IF NOT EXISTS cancellation (
    cancellation_id INT GENERATED ALWAYS AS IDENTITY,
    service_details_id INT NOT NULL,
    cancelled_station_id INT NOT NULL,
    reached_station_id INT NOT NULL,
    cancel_code_id INT NOT NULL,
    PRIMARY KEY (cancellation_id),
    FOREIGN KEY (service_details_id) REFERENCES service_details(service_details_id),
    FOREIGN KEY (cancel_code_id) REFERENCES cancel_code(cancel_code_id),
    FOREIGN KEY (cancelled_station_id) REFERENCES station(station_id),
    FOREIGN KEY (reached_station_id) REFERENCES station(station_id)   
);

INSERT INTO service_type (service_type_name)
VALUES ('bus'), ('train');