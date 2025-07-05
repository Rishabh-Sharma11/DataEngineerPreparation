CREATE EXTERNAL TABLE `lyft.lyft_rides`
(
  ride_id STRING,
  user_id STRING,
  driver_id STRING,
  start_location STRUCT<
    latitude FLOAT64,
    longitude FLOAT64
  >,
  end_location STRUCT<
    latitude FLOAT64,
    longitude FLOAT64
  >,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  distance_miles FLOAT64,
  price_usd FLOAT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://bigquery_projects/lyft/lyft_rides_*.json']
);

Select * from `lyft.lyft_rides` limit 5;