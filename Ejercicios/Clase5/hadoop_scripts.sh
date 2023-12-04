docker exec -it edvai_hadoop bash
su hadoop

wget -O /home/hadoop/landing/yellow_tripdata_2021-01.csv "https://edvaibucket.blob.core.windows.net/data-engineer-edvai/yellow_tripdata_2021-01.parquet?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D"

hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /inputs

