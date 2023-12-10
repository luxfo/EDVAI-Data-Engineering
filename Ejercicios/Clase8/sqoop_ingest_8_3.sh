docker exec -it edvai_hadoop bash
su hadoop

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

	 