rm -f /home/hadoop/landing/*

wget -O /home/hadoop/landing/informe_ministerio_2021.csv "https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D"

wget -O /home/hadoop/landing/informe_ministerio_2022.csv "https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D"

wget -O /home/hadoop/landing/aeropuertos_detalle.csv "https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D"

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/informe_ministerio_2021.csv home/hadoop/tp1/ingest

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/informe_ministerio_2022.csv home/hadoop/tp1/ingest

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/aeropuertos_detalle.csv home/hadoop/tp1/ingest
