docker exec -it edvai_hadoop bash
su hadoop
hdfs dfs -mkdir home/hadoop/tp2
hdfs dfs -mkdir home/hadoop/tp2/ingest
cd home/hadoop/scripts
nano tp2_ingest.sh
chmod 754 tp2_ingest.sh
nano tp2_transform.py
chmod 754 tp2_transform.py

