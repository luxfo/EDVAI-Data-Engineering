docker exec -it edvai_hadoop bash
su hadoop
sqoop list-tables \
--connect jdbc:postgresql://172.17.0.2:5432/northwind \
--username postgres -P