CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE cities (
    id SERIAL PRIMARY KEY,
    country text not null,
    city text not null,
    accent_city text not null,
    region text not null,
    location geometry(Point, 4326) not null);

CREATE INDEX cities_location_idx
  ON cities
  USING GIST (location);