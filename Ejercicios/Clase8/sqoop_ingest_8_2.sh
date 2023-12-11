docker exec -it edvai_hadoop bash
su hadoop

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--query "select o.order_id, o.shipped_date, c.company_name, c.phone from orders o inner join customers c on (o.customer_id = c.customer_id) where \$CONDITIONS" \
--m 1 \
--P \
--target-dir /home/hadoop/sqoop/ingest/envios \
--as-parquetfile \
--delete-target-dir


