docker exec -it edvai_hadoop bash
su hadoop
hive

CREATE EXTERNAL TABLE tripdata.airport_trips (tpep_pickup_datetime date, airport_fee float, payment_type int, tolls_amount float, total_amount float)
    > COMMENT 'Airport Trips Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ','
    > LOCATION '/tables/external/airport_trips';

describe tripdata.airport_trips;

