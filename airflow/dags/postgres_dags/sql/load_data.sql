COPY traffic_table(id, track_id, vehicle_type, traveled_d, avg_speed, lat, lon, speed, lon_acc, lat_acc, record_time)
FROM '../../../../data/clean_data.csv'
DELIMITER ','
CSV HEADER;

