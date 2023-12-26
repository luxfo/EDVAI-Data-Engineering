from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/home/hadoop/tp1/ingest/aeropuertos_detalle.csv")
df = df.drop("inhab", "fir")
df = df.na.fill(value=0, subset=["distancia_ref"])

df.createOrReplaceTempView("v_aeropuerto_detalles")

spark.sql("""
    insert into aviacion_civil.aeropuerto_detalles
    select cast(local as string) as aeropuerto,
           cast(oaci as string) as oac,
           cast(iata as string) as iata,
           cast(tipo as float) as tipo,
           cast(denominacion as string) as denominacion,
           cast(coordenadas as string) as coordenadas,
           cast(latitud as string) as latitud,
           cast(longitud as string) as longitud,
           cast(elev as float) as elev,
           cast(uom_elev as string) as uom_elev,
           cast(ref as string) as ref,
           cast(distancia_ref as string) as distancia_ref,
           cast(direccion_ref as string) as direccion_ref,
           cast(condicion as string) as condicion,
           cast(region as string) as region,
           cast(uso as string) as uso,
           cast(trafico as string) as trafico,
           cast(sna as string) as sna,
           cast(concesionado as string) as concesionado,
           cast(provincia as string) as provincia
    from v_aeropuerto_detalles
""")