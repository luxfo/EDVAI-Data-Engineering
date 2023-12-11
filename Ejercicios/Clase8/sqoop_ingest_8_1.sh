docker exec -it edvai_hadoop bash
su hadoop

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--query "select c.customer_id, c.company_name, count(o.order_id) as products_sold from customers c inner join orders o on (o.customer_id = c.customer_id) where \$CONDITIONS group by c.customer_id, c.company_name order by count(o.order_id) desc" \
--m 1 \
--P \
--target-dir /home/hadoop/sqoop/ingest/clientes \
--as-parquetfile \
--delete-target-dir

