CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE telemetry_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_time TIMESTAMP NOT NULL,
    serial_number VARCHAR(255) NOT NULL,
    reaction_latency REAL,
    movement_accuracy REAL,
    battery_level_percent INTEGER,
    battery_temp REAL
);

INSERT INTO telemetry_events (event_time, serial_number, reaction_latency, movement_accuracy, battery_level_percent, battery_temp)
VALUES
    (NOW() - INTERVAL '60 min', 'device0001', 92.0, 97.5, 90, 36.9),
    (NOW() - INTERVAL '30 min',  'device0001', 88.0, 96.8, 82, 37.2),
    (NOW() - INTERVAL '60 min', 'device0002', 82.0, 95.5, 60, 36.1),
    (NOW() - INTERVAL '30 min',  'device0002', 90.0, 94.8, 42, 37.1),
    (NOW() - INTERVAL '60 min', 'device0003', 82.0, 95.5, 80, 36.0),
    (NOW() - INTERVAL '30 min',  'device0003', 90.0, 94.8, 62, 37.0);