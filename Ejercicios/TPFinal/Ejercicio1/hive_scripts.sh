docker exec -it edvai_hadoop bash
su hadoop
hive

CREATE DATABASE aviacion_civil;

CREATE TABLE aviacion_civil.aeropuerto (fecha date, horaUTC string, clase_vuelo string, clasificacion_vuelo string, tipo_movimiento string, aeropuerto string, origen_destino string, aerolinea_nombre string, aeronave string, pasajeros integer)
    COMMENT 'Aeropuerto Table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

CREATE TABLE aviacion_civil.aeropuerto_detalles (aeropuerto string, oac string, iata string, tipo float, denominacion string, coordenadas string, latitud string, longitud string, elev float, uom_elev string, ref string, distancia_ref string, direccion_ref string, condicion string, region string, uso string, trafico string, sna string, concesionado string, provincia string)
    COMMENT 'Aeropuerto Detalles Table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';



