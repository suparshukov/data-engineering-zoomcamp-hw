Week 3 Homework

1.
``` sql
-- Creating an external table referring to a gcs path
CREATE OR REPLACE EXTERNAL TABLE `smart-spark-412319.nytaxi.external_green_tripdata`
OPTIONS (
 format = 'Parquet',
 uris = ['gs://zoomcamp-mage-sp/nyc_green_taxi_data_2022.parquet']
);

SELECT COUNT(*) FROM `smart-spark-412319.nytaxi.external_green_tripdata`;

-- Creating a non partitioned table from the external table
CREATE OR REPLACE TABLE smart-spark-412319.nytaxi.green_tripdata_non_partitoned AS 
SELECT * FROM smart-spark-412319.nytaxi.external_green_tripdata;
```

2.
``` sql
SELECT COUNT(DISTINCT pu_location_id) 
FROM `smart-spark-412319.nytaxi.external_green_tripdata`;

SELECT COUNT(DISTINCT pu_location_id) 
FROM `smart-spark-412319.nytaxi.green_tripdata_non_partitoned`;
```
3.
``` sql
SELECT COUNT(*) 
FROM `smart-spark-412319.nytaxi.green_tripdata_non_partitoned` 
WHERE fare_amount = 0;
```
4.
``` sql
-- Creating a partition and cluster table 
CREATE OR REPLACE TABLE smart-spark-412319.nytaxi.green_tripdata_partitoned_clustered 
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY pu_location_id AS 
SELECT * FROM smart-spark-412319.nytaxi.external_green_tripdata;
```

``` sql
5.
SELECT DISTINCT pu_location_id 
FROM smart-spark-412319.nytaxi.green_tripdata_non_partitoned 
WHERE lpep_pickup_datetime >= '2022-06-01' and lpep_pickup_datetime < '2022-07-01';

SELECT DISTINCT pu_location_id 
FROM smart-spark-412319.nytaxi.green_tripdata_partitoned_clustered 
WHERE lpep_pickup_datetime >= '2022-06-01' and lpep_pickup_datetime < '2022-07-01';
```
8.
``` sql
SELECT count(*) 
FROM smart-spark-412319.nytaxi.green_tripdata_non_partitoned;
```