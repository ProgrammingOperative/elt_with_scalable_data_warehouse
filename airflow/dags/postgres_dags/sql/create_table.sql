create table if not exists traffic_table
(
id BIGSERIAL PRIMARY KEY,
track_id INT NOT NULL,
vehicle_type VARCHAR(500) NOT NULL,
traveled_d VARCHAR(500) NOT NULL,
avg_speed FLOAT NOT NULL,
lat FLOAT NOT NULL,
lon FLOAT NOT NULL,
speed FLOAT NOT NULL,
lon_acc FLOAT NOT NULL,
lat_acc FLOAT NOT NULL,
record_time FLOAT NOT NULL
);