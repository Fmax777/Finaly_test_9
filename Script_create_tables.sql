-- Создание таблицы airlines
CREATE TABLE airlines (
  iata_code VARCHAR(255),
  airline VARCHAR(255)
);

-- Создание таблицы airports
CREATE TABLE airports (
  iata_code VARCHAR(255),
  airport VARCHAR(255),
  city VARCHAR(255),
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION
);

-- Создание таблицы flights
CREATE TABLE flights_pak (
  flight_id SERIAL PRIMARY KEY, -- Добавлен суррогатный ключ для идентификации каждой записи
  date DATE,
  day_of_week VARCHAR(255),
  airline VARCHAR(255),
  flight_number INT,
  tail_number VARCHAR(255),
  origin_airport VARCHAR(255),
  destination_airport VARCHAR(255),
  departure_delay FLOAT,
  distance FLOAT,
  arrival_delay FLOAT,
  diverted INT,
  cancelled INT,
  cancellation_reason VARCHAR(255),
  air_system_delay FLOAT,
  security_delay FLOAT,
  airline_delay FLOAT,
  late_aircraft_delay FLOAT,
  weather_delay FLOAT,
  departure_hour INT,
  arrival_hour INT,
  is_long_haul VARCHAR(255),
  day_part VARCHAR(255)
);