docker exec -it edvai_hadoop bash
su hadoop
pyspark
df = spark.read.option('header','true').csv('/inputs/yellow_tripdata_2021-01.csv')
df.printSchema()

df.select(df.VendorID.cast("int"), df.tpep_pickup_datetime.cast("date"), df.payment_type.cast("int"), df.total_amount.cast("float")).where(df.payment_type == 1).write.insertInto("tripdata.payments")

df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"), df.total_amount.cast("float")).where((df.passenger_count > 1) & (df.total_amount > 8)).write.insertInto('tripdata.passengers')

df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"), df.tolls_amount.cast("float"), df.total_amount.cast("float")).where((df.passenger_count > 1) & (df.tolls_amount > 0.1)).write.insertInto('tripdata.tolls')

df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"), df.congestion_surcharge.cast("float"), df.total_amount.cast("float")).where((df.tpep_pickup_datetime.cast("date") == '2021-01-18') & (df.congestion_surcharge > 0)).write.insertInto("tripdata.congestion")

df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"), df.trip_distance.cast("float"), df.total_amount.cast("float")).where((df.tpep_pickup_datetime.cast("date") == '2020-12-31') & (df.passenger_count == 1) & (df.trip_distance > 15)).write.insertInto("tripdata.distance")

