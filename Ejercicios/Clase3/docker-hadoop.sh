docker exec -it edvai_hadoop bash
su hadoop
cd /home/hadoop/scripts/
nano landing.sh
chmod 754 landing.sh
./landing.sh
hdfs dfs -ls /ingest