from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/inputs/sqoop/ingest/clientes/yellow_tripdata_2021-01.parquet")

df.createOrReplaceTempView("v_products_sold")

spark.sql("""
    insert into northwind_analytics.products_sold
    select  cast(customer_id as string),
            cast(company_name as string),
            cast(products_sold as integer)
    from v_products_sold
    where cast(products_sold as integer) > (select avg(products_sold)
                                            from v_products_sold)
""")