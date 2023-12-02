docker exec -it edvai_hadoop bash
su hadoop
hive

CREATE TABLE tripdata.payments (vendorId int, tpep_pickup_datetime datetime, payment_type int, total_amount float)
    > COMMENT 'Payments Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

CREATE TABLE tripdata.payments (vendorId int, tpep_pickup_datetime date, payment_type int, total_amount float)
    > COMMENT 'Payments Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

CREATE TABLE tripdata.passengers (tpep_pickup_datetime date, passenger_count int, total_amount float)
    > COMMENT 'Passengers Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

CREATE TABLE tripdata.tolls (tpep_pickup_datetime date, passenger_count int, tolls_amount float, total_amount float)
    > COMMENT 'Tolls Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

CREATE TABLE tripdata.congestion (tpep_pickup_datetime date, passenger_count int, congestion_surcharge float, total_amount float)
    > COMMENT 'Congestion Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

CREATE TABLE tripdata.distance (tpep_pickup_datetime date, passenger_count int, trip_distance float, total_amount float)
    > COMMENT 'Distance Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

describe passengers;

describe distance;

select * from tripdata.payments limit 10;

select * from tripdata.passengers limit 10;

select * from tripdata.tolls limit 10;

select * from tripdata.congestion limit 10;

select * from tripdata.distance limit 10;

