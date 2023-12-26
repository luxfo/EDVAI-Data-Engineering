rm -f /home/hadoop/landing/*

wget -O /home/hadoop/landing/car_rental_data.csv "https://edvaibucket.blob.core.windows.net/data-engineer-edvai/CarRentalData.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D"

wget -O /home/hadoop/landing/georef_usa_state.csv "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-state/exports/csv?lang=en&timezone=America%2FArgentina%2FBuenos_Aires&use_labels=true&delimiter=%3B"

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/car_rental_data.csv home/hadoop/tp2/ingest

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/georef_usa_state.csv home/hadoop/tp2/ingest

