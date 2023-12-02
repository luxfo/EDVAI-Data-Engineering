docker exec -it edvai_hadoop bash
su hadoop
pyspark
df = spark.read.option('header','true').csv('/inputs/yellow_tripdata_2021-01.csv')
df.printSchema()
df.createOrReplaceTempView('v_yellow_tripdata')

result = spark.sql("select cast(vendorid as integer), cast(tpep_pickup_datetime as date), cast(total_amount as double) from v_yellow_tripdata where cast(total_amount as double) < 10")
result.show()

result = spark.sql("select cast(tpep_pickup_datetime as date), sum(cast(total_amount as double)) as sum_total_amount from v_yellow_tripdata group by cast(tpep_pickup_datetime as date) order by 2 desc")
result.show(10)

result = spark.sql("select cast(trip_distance as float), cast(total_amount as double) from v_yellow_tripdata where cast(trip_distance as float) > 10 order by 2 asc")
result.show()

result = spark.sql("select cast(trip_distance as float), cast(tpep_pickup_datetime as date) from v_yellow_tripdata where cast(passenger_count as integer) > 2 and cast(payment_type as integer) = 1")
result.show()

result = spark.sql("select cast(trip_distance as float), cast(tpep_pickup_datetime as date), cast(passenger_count as integer), cast(tip_amount as float) from v_yellow_tripdata where cast(trip_distance as float) > 10 order by cast(tip_amount as float) desc")
result.show()

result = spark.sql("select ratecodeid, sum(cast(total_amount as double)) as sum_total_amount, avg(cast(total_amount as double)) as avg_total_amount from v_yellow_tripdata where ratecodeid <> 6 group by ratecodeid")
result.show()
