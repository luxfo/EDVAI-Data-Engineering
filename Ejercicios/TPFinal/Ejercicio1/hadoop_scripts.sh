docker exec -it edvai_hadoop bash
su hadoop
hdfs dfs -mkdir home/hadoop/tp1
hdfs dfs -mkdir home/hadoop/tp1/ingest
cd home/hadoop/scripts
nano tp1_ingest.sh
chmod 754 tp1_ingest.sh
nano tp1_transform_1.py
chmod 754 tp1_transform_1.py
nano tp1_transform_2.py
chmod 754 tp1_transform_2.py



