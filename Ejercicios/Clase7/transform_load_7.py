from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df_1 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/inputs/yellow_tripdata_2021-01.parquet")
df_2 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/inputs/yellow_tripdata_2021-02.parquet")

df = df_1.union(df_2)

df.createOrReplaceTempView("v_tripdata")

spark.sql("""
    insert into tripdata.airport_trips
    select  cast(tpep_pickup_datetime as date),
            cast(airport_fee as float),
            cast(payment_type as integer),
            cast(tolls_amount as float),
            cast(total_amount as float)
    from v_tripdata
    where cast(airport_fee as float) > 0
      and cast(payment_type as integer) = 2;
""")
