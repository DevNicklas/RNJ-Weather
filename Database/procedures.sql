-- =====================================
-- = PROCEDURES =
-- =====================================

-- Insert weather data and link to city
CREATE OR REPLACE PROCEDURE add_weather_data(
    p_city VARCHAR,
    p_recorded_at TIMESTAMP,
    p_temp DECIMAL,
    p_wind DECIMAL,
    p_weather_status weather_status_enum,
    p_latitude DECIMAL(9,6) DEFAULT NULL,
    p_longitude DECIMAL(9,6) DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_city_id INT;
    v_weather_data_id INT;
BEGIN
    SELECT id INTO v_city_id FROM city WHERE name = p_city;
    IF v_city_id IS NULL THEN
        INSERT INTO city (name, country, latitude, longitude)
        VALUES (p_city, 'Denmark', p_latitude, p_longitude)
        RETURNING id INTO v_city_id;
    END IF;

    INSERT INTO weather_data (recorded_at, temperature_celsius, wind_speed_ms, weather_status)
    VALUES (p_recorded_at, p_temp, p_wind, p_weather_status)
    RETURNING id INTO v_weather_data_id;

    INSERT INTO weather (city_id, weather_data_id)
    VALUES (v_city_id, v_weather_data_id);
END;
$$;

-- Insert or update daily report
CREATE OR REPLACE PROCEDURE insert_daily_report(
    p_report_date DATE,
    p_min_temp NUMERIC,
    p_max_temp NUMERIC,
    p_avg_temp NUMERIC,
    p_city_id INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO daily_report (
        report_date, min_temp, max_temp, avg_temp, city_id
    )
    VALUES (
        p_report_date, p_min_temp, p_max_temp, p_avg_temp, p_city_id
    )
    ON CONFLICT (city_id, report_date)
    DO UPDATE SET
        min_temp = EXCLUDED.min_temp,
        max_temp = EXCLUDED.max_temp,
        avg_temp = EXCLUDED.avg_temp;

    RAISE NOTICE 'Inserted/updated daily report for city_id % on %', p_city_id, p_report_date;
END;
$$;

-- Insert or update monthly extremes
CREATE OR REPLACE PROCEDURE insert_monthly_extremes(
    p_report_month DATE,
    p_min_city_id INT,
    p_min_temp NUMERIC,
    p_min_recorded_at TIMESTAMP,
    p_max_city_id INT,
    p_max_temp NUMERIC,
    p_max_recorded_at TIMESTAMP
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO monthly_extremes (
        report_month,
        min_city_id, min_temp, min_recorded_at,
        max_city_id, max_temp, max_recorded_at
    )
    VALUES (
        p_report_month,
        p_min_city_id, p_min_temp, p_min_recorded_at,
        p_max_city_id, p_max_temp, p_max_recorded_at
    )
    ON CONFLICT (report_month)
    DO UPDATE SET
        min_city_id = EXCLUDED.min_city_id,
        min_temp = EXCLUDED.min_temp,
        min_recorded_at = EXCLUDED.min_recorded_at,
        max_city_id = EXCLUDED.max_city_id,
        max_temp = EXCLUDED.max_temp,
        max_recorded_at = EXCLUDED.max_recorded_at;

    RAISE NOTICE 'Monthly extremes stored or updated for %', p_report_month;
END;
$$;
