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
       vendorid,
       ratecodeid,
       pickup_locationid,
       dropoff_locationid
FROM january
UNION ALL
SELECT 
       tripID, 
       pickup_datetime, 
       dropoff_datetime, 
       vendorid,
       ratecodeid,
       pickup_locationid,
       dropoff_locationid
FROM february
UNION ALL
SELECT 
       tripID, 
       pickup_datetime, 
       dropoff_datetime, 
       vendorid,
       ratecodeid,
       pickup_locationid,
       dropoff_locationid
FROM march
