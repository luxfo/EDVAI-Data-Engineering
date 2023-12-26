from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df_1 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/home/hadoop/tp1/ingest/informe_ministerio_2021.csv")
df_2 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/home/hadoop/tp1/ingest/informe_ministerio_2022.csv")

df = df_1.union(df_2)

df = df.drop("Calidad dato")
df = df.fillna(value=0, subset=["Pasajeros"])

df.createOrReplaceTempView("v_aeropuerto")

spark.sql("""
    insert into aviacion_civil.aeropuerto
    select cast(concat(substring(Fecha, 7, 4), '-', substring(Fecha, 1, 2), '-', substring(Fecha, 4, 2)) as date) as fecha,
           cast(`Hora UTC` as string) as horaUTC,
           cast(`Clase de Vuelo (todos los vuelos)` as string) as clase_vuelo,
           cast(`Clasificación Vuelo` as string) as clasificacion_vuelo,
           cast(`Tipo de Movimiento` as string) as tipo_movimiento,
           cast(`Aeropuerto` as string) as aeropuerto,
           cast(`Origen / Destino` as string) as origen_destino,
           cast(`Aerolinea Nombre` as string) as aerolinea_nombre,
           cast(`Aeronave` as string) as aeronave,
           cast(`Pasajeros` as integer) as pasajeros
    from v_aeropuerto
    where cast(`Clasificación Vuelo` as string) <> 'Internacional'
    and cast(concat(substring(Fecha, 7, 4), '-', substring(Fecha, 1, 2), '-', substring(Fecha, 4, 2)) as date) >= '2021-01-01'
    and cast(concat(substring(Fecha, 7, 4), '-', substring(Fecha, 1, 2), '-', substring(Fecha, 4, 2)) as date) <= '2022-06-30'
""")
