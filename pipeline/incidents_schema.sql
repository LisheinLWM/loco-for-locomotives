CREATE SCHEMA incident_data;

SET search_path TO incident_data;

CREATE TABLE IF NOT EXISTS operator (
    operator_id INT GENERATED ALWAYS AS IDENTITY,
    operator_code TEXT NOT NULL,
    operator_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY (operator_id)
);

CREATE TABLE IF NOT EXISTS priority (
    priority_id INT GENERATED ALWAYS AS IDENTITY,
    priority_code SMALLINT NOT NULL UNIQUE,
    PRIMARY KEY (priority_id)
);

CREATE TABLE IF NOT EXISTS incident (
    incident_id INT GENERATED ALWAYS AS IDENTITY,
    incident_num INT NOT NULL,
    incident_version INT NOT NULL UNIQUE,
    link TEXT NOT NULL,
    summary TEXT NOT NULL,
    priority_id INT NOT NULL,
    operator_id INT NOT NULL,
    is_planned BOOLEAN NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    PRIMARY KEY (incident_id),
    FOREIGN KEY (priority_id) REFERENCES priority(priority_id),
    FOREIGN KEY (operator_id) REFERENCES operator(operator_id)
);

CREATE TABLE IF NOT EXISTS route_affected (
    route_id INT GENERATED ALWAYS AS IDENTITY,
    route_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY (route_id)
);

CREATE TABLE IF NOT EXISTS incident_route_link (
    incident_route_link_id INT GENERATED ALWAYS AS IDENTITY,
    route_id INT NOT NULL,
    incident_id INT NOT NULL,
    PRIMARY KEY (incident_route_link_id),
    FOREIGN KEY (route_id) REFERENCES route_affected(route_id),
    FOREIGN KEY (incident_id) REFERENCES incident(incident_id)
);

CREATE TABLE IF NOT EXISTS incident_operator_link (
    incident_operator_link_id INT GENERATED ALWAYS AS IDENTITY,
    operator_id INT NOT NULL,
    incident_id INT NOT NULL UNIQUE,
    PRIMARY KEY (incident_operator_link_id),
    FOREIGN KEY (operator_id) REFERENCES operator(operator_id),
    FOREIGN KEY (incident_id) REFERENCES incident(incident_id)
);