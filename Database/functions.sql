-- =====================================
-- = FUNCTIONS =
-- =====================================

-- Joined weather data
CREATE OR REPLACE FUNCTION get_weather_data()
RETURNS TABLE (
    city VARCHAR,
    country VARCHAR,
    recorded_at TIMESTAMP,
    temperature_celsius DECIMAL(5,2),
    wind_speed_ms DECIMAL(5,2),
    weather_status weather_status_enum
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.name AS city,
        c.country,
        wd.recorded_at,
        wd.temperature_celsius,
        wd.wind_speed_ms,
        wd.weather_status
    FROM weather w
    JOIN city c ON w.city_id = c.id
    JOIN weather_data wd ON w.weather_data_id = wd.id
    ORDER BY wd.recorded_at DESC;
END;
$$;


-- Daily reports
CREATE OR REPLACE FUNCTION get_daily_reports(p_date DATE DEFAULT NULL)
RETURNS TABLE (
    report_date DATE,
    city_id INT,
    city_name VARCHAR,
    min_temp DECIMAL(5,2),
    max_temp DECIMAL(5,2),
    avg_temp DECIMAL(5,2)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        dr.report_date,
        dr.city_id,
        c.name AS city_name,
        dr.min_temp,
        dr.max_temp,
        dr.avg_temp
    FROM daily_report dr
    JOIN city c ON dr.city_id = c.id
    WHERE p_date IS NULL OR dr.report_date = p_date
    ORDER BY dr.report_date DESC, c.name;
END;
$$;

-- Monthly report
CREATE OR REPLACE FUNCTION get_monthly_report(p_year INT, p_month INT)
RETURNS TABLE (
    report_month DATE,
    weather_status VARCHAR,
    avg_temp DECIMAL(5,2),
    min_city VARCHAR,
    min_temp DECIMAL(5,2),
    min_recorded_at TIMESTAMP,
    max_city VARCHAR,
    max_temp DECIMAL(5,2),
    max_recorded_at TIMESTAMP
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ma.report_month,
        ma.weather_status,
        ma.avg_temp,
        cmin.name AS min_city,
        me.min_temp,
        me.min_recorded_at,
        cmax.name AS max_city,
        me.max_temp,
        me.max_recorded_at
    FROM monthly_avg_by_status ma
    LEFT JOIN monthly_extremes me 
        ON ma.report_month = me.report_month
    LEFT JOIN city cmin ON me.min_city_id = cmin.id
    LEFT JOIN city cmax ON me.max_city_id = cmax.id
    WHERE EXTRACT(YEAR FROM ma.report_month) = p_year
      AND EXTRACT(MONTH FROM ma.report_month) = p_month
    ORDER BY ma.weather_status;
END;
$$;

-- Hourly weather (raw)
CREATE OR REPLACE FUNCTION get_hourly_weather(
    p_date DATE,
    p_city_id INT DEFAULT NULL
)
RETURNS TABLE (
    city_id INT,
    city_name VARCHAR,
    recorded_hour TIME,
    temperature_celsius DECIMAL(5,2),
    wind_speed_ms DECIMAL(5,2),
    weather_status weather_status_enum
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id AS city_id,
        c.name AS city_name,
        DATE_TRUNC('hour', wd.recorded_at)::TIME AS recorded_hour,
        wd.temperature_celsius,
        wd.wind_speed_ms,
        wd.weather_status
    FROM weather_data wd
    JOIN weather w ON wd.id = w.weather_data_id
    JOIN city c ON w.city_id = c.id
    WHERE DATE(wd.recorded_at) = p_date
      AND (p_city_id IS NULL OR c.id = p_city_id)
    ORDER BY c.name, recorded_hour;
END;
$$;

-- Hourly weather (averaged)
CREATE OR REPLACE FUNCTION get_hourly_weather_avg(
    p_date DATE,
    p_city_id INT DEFAULT NULL
)
RETURNS TABLE (
    city_id INT,
    city_name VARCHAR,
    recorded_hour TIME,
    avg_temp DECIMAL(5,2),
    avg_wind DECIMAL(5,2),
    dominant_status weather_status_enum
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH raw_data AS (
        SELECT *
        FROM get_hourly_weather(p_date, p_city_id)
    ),
    grouped AS (
        SELECT 
            r.city_id,
            r.city_name,
            r.recorded_hour,
            ROUND(AVG(r.temperature_celsius), 2) AS avg_temp,
            ROUND(AVG(r.wind_speed_ms), 2) AS avg_wind,
            (
                SELECT wd.weather_status
                FROM get_hourly_weather(p_date, p_city_id) wd
                WHERE wd.city_id = r.city_id
                  AND wd.recorded_hour = r.recorded_hour
                GROUP BY wd.weather_status
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS dominant_status
        FROM raw_data r
        GROUP BY r.city_id, r.city_name, r.recorded_hour
    )
    SELECT * FROM grouped
    ORDER BY city_name, recorded_hour;
END;
$$;
