-- =====================================
-- = ENUM TYPES =
-- =====================================

CREATE TYPE weather_status_enum AS ENUM (
    'Drizzle',
    'Rain',
    'Snow',
    'Freezing drizzle',
    'Freezing rain',
    'Graupel',
    'Hail'
);
