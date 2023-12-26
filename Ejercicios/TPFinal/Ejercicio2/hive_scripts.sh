docker exec -it edvai_hadoop bash
su hadoop
hive

CREATE DATABASE car_rental_db;

CREATE TABLE car_rental_db.car_rental_analytics (fuelType string, rating integer, renterTripsTaken integer, reviewCount integer, city string, state_name string, owner_id integer, rate_daily integer, make string, model string, year integer)
    COMMENT 'Car Rental Analytics Table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
