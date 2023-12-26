from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import round, lower
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df_1 = spark.read.option("header", "true").option("delimiter", ",").csv("hdfs://172.17.0.2:9000/home/hadoop/tp2/ingest/car_rental_data.csv")
df_1 = df_1.toDF(*(c.replace('.', '_') for c in df_1.columns))
df_1 = df_1.withColumn('rating', round(df_1.rating, 0))

df_2 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/home/hadoop/tp2/ingest/georef_usa_state.csv")
df_2 = df_2.toDF(*(c.replace(' ', '_') for c in df_2.columns))
df_2 = df_2.toDF(*(c.replace('-', '_') for c in df_2.columns))
df_2 = df_2.withColumnRenamed("United_States_Postal_Service_state_abbreviation", "US_PS_State_Abbr")

df = df_1.join(df_2, df_1.location_state == df_2.US_PS_State_Abbr, 'inner')
df = df.na.drop(subset=["rating"])
df = df.withColumn('fuelType', lower(df_1.fuelType))
df = df.filter(df.Official_Name_State != 'Texas')

df.createOrReplaceTempView("v_car_rental")

spark.sql("""
    insert into car_rental_db.car_rental_analytics
    select cast(fuelType as string),
           cast(rating as integer),
           cast(renterTripsTaken as integer),
           cast(reviewCount as integer),
           cast(location_city as string) as city,
           cast(Official_Name_State as string) as state_name,
           cast(owner_id as integer),
           cast(rate_daily as integer),
           cast(vehicle_make as string) as make,
           cast(vehicle_model as string) as model,
           cast(vehicle_year as integer) as year
    from v_car_rental
""")
