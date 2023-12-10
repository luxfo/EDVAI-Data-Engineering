docker exec -it edvai_hadoop bash
su hadoop
hive

create database northwind_analytics;

CREATE TABLE northwind_analytics.products_sold (customer_id string, company_name string, products_sold int)
    > COMMENT 'Products Sold Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

CREATE TABLE northwind_analytics.products_sent (order_id int, shipped_date date, company_name string, phone string, unit_price_discount float, quantity int, total_price float)
    > COMMENT 'Products Sent Table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';

