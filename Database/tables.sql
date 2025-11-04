-- =====================================
-- = TABLES =
-- =====================================

-- City table
CREATE TABLE IF NOT EXISTS city (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

-- Weather data table
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    recorded_at TIMESTAMP NOT NULL,
    temperature_celsius DECIMAL(5,2),
    wind_speed_ms DECIMAL(5,2),
    weather_status weather_status_enum
);

-- Link table for city ↔ weather data
CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES city(id) ON DELETE CASCADE,
    weather_data_id INT NOT NULL REFERENCES weather_data(id) ON DELETE CASCADE
);

-- Daily report
CREATE TABLE IF NOT EXISTS daily_report (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    min_temp DECIMAL(5,2),
    max_temp DECIMAL(5,2),
    avg_temp DECIMAL(5,2),
    city_id INT NOT NULL REFERENCES city(id) ON DELETE CASCADE,
    UNIQUE (city_id, report_date)
);

-- Monthly average by weather status
CREATE TABLE IF NOT EXISTS monthly_avg_by_status (
    id SERIAL PRIMARY KEY,
    report_month DATE NOT NULL,
    weather_status VARCHAR(50) NOT NULL,
    avg_temp DECIMAL(5,2) NOT NULL,
    UNIQUE (report_month, weather_status)
);

-- Monthly extremes
CREATE TABLE IF NOT EXISTS monthly_extremes (
    id SERIAL PRIMARY KEY,
    report_month DATE NOT NULL UNIQUE,
    min_city_id INT NOT NULL REFERENCES city(id) ON DELETE CASCADE,
    min_temp DECIMAL(5,2),
    min_recorded_at TIMESTAMP,
    max_city_id INT NOT NULL REFERENCES city(id) ON DELETE CASCADE,
    max_temp DECIMAL(5,2),
    max_recorded_at TIMESTAMP
);
