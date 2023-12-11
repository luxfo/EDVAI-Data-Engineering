from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df_1 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/inputs/sqoop/ingest/envios/*.parquet")
df_2 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/inputs/sqoop/ingest/order_details/*.parquet")

df_1.createOrReplaceTempView("v_orders")
df_2.createOrReplaceTempView("v_order_details")

spark.sql("""
    insert into northwind_analytics.products_sent
    select  cast(o.order_id as int),
            cast(o.shipped_date as float),
            cast(o.company_name as string),
            cast(o.phone as string),
            cast((od.unit_price - od.discount) as float) as unit_price_discount,
            cast(od.quantity as int),
            cast(((od.unit_price - od.discount) * od.quantity) as float) as total_price
    from v_orders o
         inner join v_orders_details od on (o.order_id = od.order_id)
    where cast(od.discount as float) > 0
""")
