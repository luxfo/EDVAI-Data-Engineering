docker exec -it edvai_hadoop bash
su hadoop
sqoop import \
--connect jdbc:postgresql://172.17.0.2:5432/northwind \
--username postgres \
--table products \
--m 1 \
--P \
--target-dir home/hadoop/sqoop/ingest \
--as-parquetfile \
--where "units_in_stock > 20" \
--delete-target-dir
