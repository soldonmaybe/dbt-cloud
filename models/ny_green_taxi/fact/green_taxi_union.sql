{{ config(materialized='table') }}

with january as 
(
  select ROW_NUMBER() OVER (ORDER BY pickup_datetime) as tripID,* 
  from {{ ref('green_taxi_2021_01') }}
),
february as
(
  select ROW_NUMBER() OVER (ORDER BY pickup_datetime) as tripID,* 
  from {{ ref('green_taxi_2021_02') }}
),
march as
(
  select ROW_NUMBER() OVER (ORDER BY pickup_datetime) as tripID,* 
  from {{ ref('green_taxi_2021_03') }}
)

SELECT 
       tripID, 
       pickup_datetime, 
       dropoff_datetime, 
       CAST(vendorID AS INT64) as vendorID,
       CAST(RatecodeID AS INT64) as RatecodeID,
       CAST(PULocationID AS INT64) as PULocationID,
       CAST(DOLocationID AS INT64) as DOLocationID
FROM january
UNION ALL
SELECT 
       tripID, 
       pickup_datetime, 
       dropoff_datetime, 
       CAST(vendorID AS INT64) as vendorID,
       CAST(RatecodeID AS INT64) as RatecodeID,
       CAST(PULocationID AS INT64) as PULocationID,
       CAST(DOLocationID AS INT64) as DOLocationID
FROM february
UNION ALL
SELECT 
       tripID, 
       pickup_datetime, 
       dropoff_datetime, 
       CAST(vendorID AS INT64) as vendorID,
       CAST(RatecodeID AS INT64) as RatecodeID,
       CAST(PULocationID AS INT64) as PULocationID,
       CAST(DOLocationID AS INT64) as DOLocationID
FROM march
