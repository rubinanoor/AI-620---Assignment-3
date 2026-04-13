
-- creating schema
DROP SCHEMA IF EXISTS synthetic_cars CASCADE;
CREATE SCHEMA synthetic_cars;
SET search_path TO synthetic_cars;


--  defining values for categorical featuress in reference tables 
CREATE TABLE cities (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO cities (name) VALUES
    ('Karachi'), ('Lahore'), ('Islamabad'), ('Rawalpindi'), ('Faisalabad'),
    ('Multan'), ('Peshawar'), ('Quetta'), ('Sialkot'), ('Gujranwala');

CREATE TABLE body_types (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO body_types (name) VALUES
    ('Sedan'), ('Hatchback'), ('SUV'), ('Crossover'), ('Compact SUV'),
    ('Van'), ('MPV'), ('Pickup'), ('Coupe');

CREATE TABLE makes (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO makes (name) VALUES
    ('Toyota'), ('Honda'), ('Suzuki'), ('Hyundai'), ('KIA'),
    ('Mitsubishi'), ('Nissan'), ('Daihatsu'), ('BMW'), ('Mercedes');

CREATE TABLE fuel_types (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO fuel_types (name) VALUES ('Petrol'), ('Diesel'), ('Hybrid'), ('CNG');

CREATE TABLE transmissions (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO transmissions (name) VALUES ('Manual'), ('Automatic');


-- clean synthetic dataset
CREATE TABLE clean_cars AS
WITH base AS (
    SELECT
        7000000 + gs AS addref,

        -- random city from reference table
        (SELECT name FROM cities
         ORDER BY random() LIMIT 1) AS city,

        (1991 + floor(random() * 32)::INT) AS year,

        (660 + floor(random() * 29)::INT * 100)   AS engine,

        (floor(random() * 500) * 500)::INT AS mileage,

        (SELECT name FROM body_types
         ORDER BY random() LIMIT 1)  AS body,

        (SELECT name FROM makes
         ORDER BY random() LIMIT 1) AS make,

        CASE
            WHEN random() < 0.70 THEN 'Petrol'
            WHEN random() < 0.85 THEN 'Diesel'
            WHEN random() < 0.95 THEN 'Hybrid'
            ELSE 'CNG'
        END AS fuel,

        CASE
            WHEN (1991 + floor(random() * 32)::INT) >= 2010
                 AND random() < 0.65 THEN 'Automatic'
            ELSE 'Manual'
        END  AS transmission,

        GREATEST(
            500000,
            (  (1991 + floor(random() * 32)::INT - 1990) * 80000
             + (660  + floor(random() * 29)::INT * 100)  * 400
             + floor(random() * 2000000)
            )
        )::BIGINT                                              AS price

    FROM generate_series(1, 50000) AS gs 
)
SELECT * FROM base;

ALTER TABLE clean_cars ADD PRIMARY KEY (addref);



-- corrupted synthetic dataset

CREATE TABLE corrupted_cars AS
SELECT * FROM clean_cars;  

-- corruption 1: Inject NULLs 
UPDATE corrupted_cars
SET city = NULL
WHERE random() < 0.05;

UPDATE corrupted_cars
SET fuel = NULL
WHERE random() < 0.05;

UPDATE corrupted_cars
SET engine = NULL
WHERE random() < 0.04;

UPDATE corrupted_cars
SET price = NULL
WHERE random() < 0.03;

UPDATE corrupted_cars
SET year = NULL
WHERE random() < 0.04;

UPDATE corrupted_cars
SET body = NULL
WHERE random() < 0.06;

-- corruption 2: out of range values 
UPDATE corrupted_cars
SET engine = CASE WHEN random() < 0.5 THEN 0 ELSE 99999 END
WHERE random() < 0.02;

-- corruption 3: data entry error
UPDATE corrupted_cars
SET mileage = -1 * mileage
WHERE random() < 0.02;

-- corruption 4: entry error
UPDATE corrupted_cars
SET year = 2030
WHERE random() < 0.015;

-- corruption 5: price not scraped
UPDATE corrupted_cars
SET price = 0
WHERE random() < 0.03;

-- corruption 6: Inconsistent categorical casing 
UPDATE corrupted_cars
SET transmission = UPPER(transmission)
WHERE random() < 0.04;

UPDATE corrupted_cars
SET fuel = LOWER(fuel)
WHERE random() < 0.04;

-- corruption 7: duplicate rows 
INSERT INTO corrupted_cars
SELECT
    addref + 90000000,  
    city, year, engine, mileage, body, make, fuel, transmission, price
FROM corrupted_cars
WHERE random() < 0.01   
LIMIT 500;

