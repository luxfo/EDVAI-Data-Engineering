docker exec -it edvai_hadoop bash
su hadoop

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--query "select c.customer_id, c.company_name, count(o.order_id) as products_sold from customers c inner join orders o on (o.customer_id = c.customer_id) where \$CONDITIONS group by c.customer_id, c.company_name order by count(o.order_id) desc" \
--m 1 \
--P \
--target-dir /home/hadoop/sqoop/ingest \
--as-parquetfile \
--delete-target-dir

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/sqoop/ingest/*.parquet /inputs/sqoop/ingest/clientes

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--query "select o.order_id, o.shipped_date, c.company_name, c.phone from orders o inner join customers c on (o.customer_id = c.customer_id) where \$CONDITIONS" \
--m 1 \
--P \
--target-dir /home/hadoop/sqoop/ingest \
--as-parquetfile \
--delete-target-dir

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/sqoop/ingest/*.parquet /inputs/sqoop/ingest/envios

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--query "select od.order_id, od.unit_price, od.quantity, od.discount from order_details od where \$CONDITIONS" \
--m 1 \
--P \
--target-dir /home/hadoop/sqoop/ingest \
--as-parquetfile \
--delete-target-dir

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/sqoop/ingest/*.parquet /inputs/sqoop/ingest/order_details

	 