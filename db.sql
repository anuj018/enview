-- db.sql

-- Create the events table
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    is_driving_safe BOOLEAN,
    vehicle_id VARCHAR(255) NOT NULL,
    location_type VARCHAR(255) NOT NULL CHECK (location_type IN ('highway', 'city_center', 'commercial', 'residential'))
);

-- Create indexes on the events table
CREATE INDEX idx_events_timestamp ON events (timestamp);
CREATE INDEX idx_events_vehicle_id ON events (vehicle_id);

-- Create the alerts table
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    location_type VARCHAR(255) NOT NULL CHECK (location_type IN ('highway', 'city_center', 'commercial', 'residential'))
);

-- Create indexes on the alerts table
CREATE INDEX idx_alerts_timestamp ON alerts (timestamp);
CREATE INDEX idx_alerts_vehicle_id ON alerts (vehicle_id);
