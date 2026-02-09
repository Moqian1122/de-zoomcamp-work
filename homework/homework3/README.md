In README.md I list all queries that I used for homework3.

```sql
CREATE OR REPLACE EXTERNAL TABLE `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024-06_ext`
(
  VendorID INTEGER OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
  tpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
  tpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
  passenger_count INTEGER OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
  trip_distance FLOAT64 OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
  RatecodeID INTEGER OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'),
  store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. TRUE = store and forward trip, FALSE = not a store and forward trip'),
  PULocationID INTEGER OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
  DOLocationID INTEGER OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
  payment_type INTEGER OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
  fare_amount FLOAT64 OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
  extra FLOAT64 OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
  mta_tax FLOAT64 OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
  tip_amount FLOAT64 OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
  tolls_amount FLOAT64 OPTIONS (description = 'Total amount of all tolls paid in trip.'),
  improvement_surcharge FLOAT64 OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
  total_amount FLOAT64 OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
  congestion_surcharge FLOAT64 OPTIONS (description = 'Congestion surcharge applied to trips in congested zones')
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://my-first-project-bucket01/yellow_tripdata_2024-06.parquet']
);

CREATE OR REPLACE TABLE `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024-06`
AS
SELECT
  MD5(CONCAT(
    COALESCE(CAST(VendorID AS STRING), ""),
    COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
    COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
    COALESCE(CAST(PULocationID AS STRING), ""),
    COALESCE(CAST(DOLocationID AS STRING), "")
  )) AS unique_row_id,
  "yellow_tripdata_2024-01.csv" AS filename,
  *
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024-06_ext`;

INSERT INTO `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`
SELECT * FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024-06`;

CREATE OR REPLACE TABLE `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`
(
  unique_row_id BYTES OPTIONS (description = 'Unique identifier'),
  filename STRING OPTIONS (description = 'Data source identifier'),
  VendorID INTEGER OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
  tpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
  tpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
  passenger_count INTEGER OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
  trip_distance FLOAT64 OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
  RatecodeID INTEGER OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'),
  store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. TRUE = store and forward trip, FALSE = not a store and forward trip'),
  PULocationID INTEGER OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
  DOLocationID INTEGER OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
  payment_type INTEGER OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
  fare_amount FLOAT64 OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
  extra FLOAT64 OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
  mta_tax FLOAT64 OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
  tip_amount FLOAT64 OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
  tolls_amount FLOAT64 OPTIONS (description = 'Total amount of all tolls paid in trip.'),
  improvement_surcharge FLOAT64 OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
  total_amount FLOAT64 OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
  congestion_surcharge FLOAT64 OPTIONS (description = 'Congestion surcharge applied to trips in congested zones')
);

SELECT COUNT(1)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`;

SELECT COUNT(1)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024-01_ext`;

CREATE OR REPLACE EXTERNAL TABLE `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024_ext`
(
  VendorID INTEGER OPTIONS (description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'),
  tpep_pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was engaged'),
  tpep_dropoff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the meter was disengaged'),
  passenger_count INTEGER OPTIONS (description = 'The number of passengers in the vehicle. This is a driver-entered value.'),
  trip_distance FLOAT64 OPTIONS (description = 'The elapsed trip distance in miles reported by the taximeter.'),
  RatecodeID INTEGER OPTIONS (description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'),
  store_and_fwd_flag STRING OPTIONS (description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. TRUE = store and forward trip, FALSE = not a store and forward trip'),
  PULocationID INTEGER OPTIONS (description = 'TLC Taxi Zone in which the taximeter was engaged'),
  DOLocationID INTEGER OPTIONS (description = 'TLC Taxi Zone in which the taximeter was disengaged'),
  payment_type INTEGER OPTIONS (description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'),
  fare_amount FLOAT64 OPTIONS (description = 'The time-and-distance fare calculated by the meter'),
  extra FLOAT64 OPTIONS (description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'),
  mta_tax FLOAT64 OPTIONS (description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'),
  tip_amount FLOAT64 OPTIONS (description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'),
  tolls_amount FLOAT64 OPTIONS (description = 'Total amount of all tolls paid in trip.'),
  improvement_surcharge FLOAT64 OPTIONS (description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'),
  total_amount FLOAT64 OPTIONS (description = 'The total amount charged to passengers. Does not include cash tips.'),
  congestion_surcharge FLOAT64 OPTIONS (description = 'Congestion surcharge applied to trips in congested zones')
)
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://my-first-project-bucket01/yellow_tripdata_2024-01.parquet',
    'gs://my-first-project-bucket01/yellow_tripdata_2024-02.parquet',
    'gs://my-first-project-bucket01/yellow_tripdata_2024-03.parquet',
    'gs://my-first-project-bucket01/yellow_tripdata_2024-04.parquet',
    'gs://my-first-project-bucket01/yellow_tripdata_2024-05.parquet',
    'gs://my-first-project-bucket01/yellow_tripdata_2024-06.parquet']
);

SELECT COUNT(1)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`;

SELECT DISTINCT COUNT(`PULocationID`)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024_ext`;

SELECT DISTINCT COUNT(`PULocationID`)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`;

SELECT `PULocationID`
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`;

SELECT `PULocationID`, `DOLocationID`
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`;

SELECT COUNT(1)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE 
  `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024_partitioned`
PARTITION BY
  DATE_TRUNC(tpep_dropoff_datetime, DAY)
CLUSTER BY
  VendorID
AS (
  SELECT *
  FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`
);

SELECT DISTINCT COUNT(VendorID)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT COUNT(VendorID)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT COUNT(*)
FROM `project-760c1f32-11db-4942-8e1.ny_taxi.yellow_tripdata_2024`;

```