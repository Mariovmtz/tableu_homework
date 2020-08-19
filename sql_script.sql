---------------------------------------------------
/*CREATE TEMPORAL TABLE TO SAVE THE BIG CSV 13GB */
--------------------------------------------------
CREATE TABLE bycicle_data (
tripduration VARCHAR(100),
starttime VARCHAR(100),
stoptiome VARCHAR(100),
start_station_id VARCHAR(100),
start_station_name VARCHAR(255),
start_station_lat VARCHAR(100),
start_station_lon VARCHAR(100),
end_station_id VARCHAR(100),
end_station_name VARCHAR(255),
end_station_lat VARCHAR(100),
end_station_lon VARCHAR(100),
bike_id VARCHAR(100),
usertype VARCHAR(100),
birth_year VARCHAR(255),
gender VARCHAR(100),
fileName VARCHAR(100)
);
---------------------------------------------------
/* LOAD THE CSV INTO THE NEW TABLE */
----------------------------------------------------
COPY bycicle_data FROM '/home/mario/Documentos/consolidated_data.csv' HEADER CSV;
---------------------------------------------------
/* CREATE A UNIQUE STATIONS TABLE */
----------------------------------------------------
CREATE TABLE stations (
station_id INT,
station_name VARCHAR(100),
station_lat NUMERIC(15,12),
station_lon NUMERIC(15,12)
)
---------------------------------------------------
	/* INSERT UNIQUE RECORDS FROM MAIN TABLE for START and END stations */
----------------------------------------------------	
WITH CTE_1 AS (
SELECT start_station_id, start_station_name, start_station_lat, start_station_lon
FROM bycicle_data WHERE start_station_id  <> 'NULL' AND start_station_lat <> 'NULL' AND start_station_lon <> 'NULL'

UNION 

SELECT end_station_id, end_station_name, end_station_lat, end_station_lon
FROM bycicle_data  WHERE end_station_id  <> 'NULL'  AND end_station_lat <> 'NULL' AND end_station_lon <> 'NULL')

INSERT INTO stations
SELECT DISTINCT 
CAST(start_station_id AS INT),
start_station_name,
CAST(start_station_lat AS numeric(15,12)),
CAST(start_station_lon AS numeric(15,12))
FROM CTE_1
---------------------------------------------------
	/* CHECKING FOR DUPLICATES */
----------------------------------------------------
SELECT station_id, count(*) 
FROM stations
GROUP BY station_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) desc

SELECT * from stations WHERE station_id IN (521, 2003, 540, 519, 504, 517)
---------------------------------------------------
	/* AFTER REVIEWING DUPLICATES IT WAS FOUND THAT MANY RECORDS HAVE SLIGHTLY DIFFERENT NAME OR EVEN SLIGTHLY DIFFERENT LATITUDE AND LONGITUDE
	FOR example
	
		521 "8 Ave & W 31 St" 	40.750967350000 -73.994442080000
		521 "8 Ave & W 31 St N" 40.750967348716 -73.994442075491
		
	 REASONS FOR DIFFERENT COORDINATES AMONG CSV FILES -
	 	SOME CSV FILES HAVE LESS DECIMALS THAN OTHERS
		SOME STATIONS ACTUALLY SLIGHTLY MOVED FROM ONE LOCATION TO ANOTHER OR THEY DO EXIST IN DIFFERENT PARTS OF THE SAME STREET
		
	IN ORDER TO CREATE A UNIQUE CATALOGUE WE WILL MIN MAX LATITUDES AND LONGITUDES TO SHOW THE APPROXIMATE PLACE OF EVERY STATION IN THE MAP USING 4 DECIMALS THAT
	SHOULD BE ENOUGH PRECISION FOR A STREET
	
	ACCORING TO DEGREE PRECISION ARTICLE IN 
	https://rapidlasso.com/2019/05/06/how-many-decimal-digits-for-storing-longitude-latitude/
	
	*/
----------------------------------------------------
DROP TABLE stations;

CREATE TABLE stations (
station_id INT,
station_name VARCHAR(100),
station_lat NUMERIC(8,4),
station_lon NUMERIC(8,4)
);

WITH CTE_1 AS (
SELECT start_station_id, start_station_name, start_station_lat, start_station_lon
FROM bycicle_data WHERE start_station_id  <> 'NULL' AND start_station_lat <> 'NULL' AND start_station_lon <> 'NULL'

UNION ALL

SELECT end_station_id, end_station_name, end_station_lat, end_station_lon
FROM bycicle_data  WHERE end_station_id  <> 'NULL'  AND end_station_lat <> 'NULL' AND end_station_lon <> 'NULL')

INSERT INTO stations

SELECT
CAST(start_station_id AS INT),
MAX(start_station_name),
MAX(CAST(start_station_lat AS numeric(8,4))),
MIN(CAST(start_station_lon AS numeric(8,4)))
FROM CTE_1
GROUP BY CAST(start_station_id AS INT)

---------------------------------------------------
	/* CHECKING FOR DUPLICATES */
----------------------------------------------------
SELECT station_id, count(*) 
FROM stations
GROUP BY station_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) desc
---------------------------------------------------
	/* 
	CHECKING FOR OUTLIERS IN LATTITUDE AND LONGITUDE WITH Z-SCORE (latitude and longitude should not vary a lot because all of the points are in NY, mainly in Manhattan)
	
	FOUND OUTLIERS, STATIONS THAT DO NOT HAVE COORDINATES LATITUDE = 0 LONGITUDE = 0 (BY THE STATION NAME IT SEEMS THEY ARE DUMMY STATIONS OR RECORDS)
	   AND ALSO 2 STATIONS WITH COORDINATES IN CANADA?
	   
	   DELETING FROM OUR LIST THOSE OUTLIERS (Z SCORE BETWEEN -1.5 AND 1.5)
	
	*/
----------------------------------------------------
DROP TABLE STATIONS_FINAL

WITH CTE_AVERAGE AS (
SELECT AVG(station_lat) as lat_avg, AVG(station_lon) as lon_avg, STDDEV(station_lat) as lat_dev, STDDEV(station_lon) as lon_dev
FROM Stations
),

	CTE_FINAL AS (

SELECT stations.*,
(station_lat - lat_avg) / lat_dev AS z_score_lat,
(station_lon - lon_avg) / lon_dev AS z_score_lon
			
FROM stations CROSS JOIN CTE_AVERAGE)

SELECT *
INTO stations_final
FROM CTE_FINAL
WHERE z_score_lat BETWEEN -1.5 AND 1.5 AND z_score_lon BETWEEN -1.5 AND 1.5
---------------------------------------------------
	/* 
	DROPPING STATIONS TABLE 
	*/
----------------------------------------------------
DROP TABLE stations
---------------------------------------------------
	/* 
	CREATING DISTANCE TABLE FROM STATION TO STATION
	*/
----------------------------------------------------
DROP TABLE distance_station

CREATE TABLE distance_station (
from_station INT,
to_station INT,
distance_miles NUMERIC(15,5)
);
---------------------------------------------------
	/* 
	POPULATING distance_station with all possible stations combinations
	
	GETTING DISTANCE FROM STATIONS USING COORDINATES AND FORMULA TAKEN FROM
	https://www.movable-type.co.uk/scripts/latlong.html
	*/
----------------------------------------------------
INSERT INTO distance_station
SELECT  a.station_id, b.station_id,
3963.0 * ACOS( (SIN(RADIANS(a.station_lat)) * SIN(RADIANS(b.station_lat))) +
					 (COS(RADIANS(a.station_lat)) * COS(RADIANS(b.station_lat)) * COS(RADIANS(a.station_lon) -  RADIANS(b.station_lon))))
FROM stations_final as a CROSS JOIN stations_final as b
WHERE a.station_id <> b.station_id
ORDER BY a.station_id, b.station_id

CREATE INDEX ON distance_station (from_station);
CREATE INDEX ON distance_station (to_station);
---------------------------------------------------
	/* 
	CREATE FINAL FACT TABLE OF TRIPS
	*/
----------------------------------------------------
DROP TABLE bycicle_data_final

CREATE TABLE bycicle_data_final (
trip_duration_sec INT,
start_time TIMESTAMP,
end_time TIMESTAMP,
start_station_id INT,
end_station_id INT,
bike_id INT,
user_type VARCHAR(20),
birth_year INT,
gender INT
);

CREATE INDEX ON bycicle_data_final (start_time);
CREATE INDEX ON bycicle_data_final (user_type);
CREATE INDEX ON bycicle_data_final (birth_year);
CREATE INDEX ON bycicle_data_final (gender);
---------------------------------------------------
	/* 
	INSERTING RECORDS FROM PRETABLE
	*/
----------------------------------------------------
INSERT INTO bycicle_data_final
SELECT 
CAST(tripduration AS INT),
CASE WHEN LEFT(starttime,4) ~ '[^0-9]' THEN TO_TIMESTAMP(starttime, 'MM/DD/YYYY HH24:MI:SS.ffff')
				     ELSE TO_TIMESTAMP(starttime, 'YYYY-MM-DD HH24:MI:SS.ffff') END,
CASE WHEN LEFT(stoptiome,4) ~ '[^0-9]' THEN TO_TIMESTAMP(stoptiome, 'MM/DD/YYYY HH24:MI:SS.ffff')
				     ELSE TO_TIMESTAMP(stoptiome, 'YYYY-MM-DD HH24:MI:SS.ffff') END,
CAST(start_station_id AS INT),
CAST(end_station_id AS INT),
CAST(bike_id AS INT),
usertype,
CASE WHEN birth_year ~ '[^0-9]' OR birth_year IS  NULL THEN NULL ELSE CAST(birth_year AS INT) END,
CASE WHEN gender ~ '[^0-9]' OR gender IS NULL THEN NULL ELSE CAST(gender AS INT)  END
FROM bycicle_data

WHERE start_station_id <> 'NULL' and end_station_id <> 'NULL'

ORDER BY 
CASE WHEN LEFT(starttime,4) ~ '[^0-9]' THEN TO_TIMESTAMP(starttime, 'MM/DD/YYYY HH24:MI:SS.ffff')
				     ELSE TO_TIMESTAMP(starttime, 'YYYY-MM-DD HH24:MI:SS.ffff') END
					 				 
---------------------------------------------------
	/* 
	DELETING BYCICLE_DATA BECAUSE OCCUPIES A LOT OF SPACE IN SERVER: 22GIGS
	*/
----------------------------------------------------				 
DROP TABLE bycicle_data		 
---------------------------------------------------
	/* 
	TILL NOW WE HAVE 3 "NORMALIZED TABLES"
	
	 1 - bycicle_data_final - Facts about a single bycicle trip.
	 2 - distance_station - distances between stations.
	 3 - stations_final - simplifed catalogue of stations with locations.
	*/
----------------------------------------------------	
SELECT * FROM bycicle_data_final LIMIT 100;
SELECT * FROM distance_station LIMIT 100;
SELECT * FROM stations_final LIMIT 100;
--------------------------------------------------------
	/* 
	 ADDING ADDITIONAL FIELDS TO FINAL TABLE
	*/
----------------------------------------------------	
ALTER TABLE bycicle_data_final
ADD COLUMN id SERIAL PRIMARY KEY,
ADD COLUMN period  VARCHAR(6),
ADD COLUMN yr  VARCHAR(4),
ADD COLUMN age INT,
ADD COLUMN mnt  VARCHAR(2);


UPDATE bycicle_data_final
SET period = TO_CHAR(EXTRACT(YEAR FROM START_TIME), 'fm0000') || TO_CHAR(EXTRACT(MONTH FROM START_TIME),'fm00'),
	yr = LEFT(TO_CHAR(EXTRACT(YEAR FROM START_TIME), 'fm0000') || TO_CHAR(EXTRACT(MONTH FROM START_TIME),'fm00'),4),
    mnt = RIGHT(TO_CHAR(EXTRACT(YEAR FROM START_TIME), 'fm0000') || TO_CHAR(EXTRACT(MONTH FROM START_TIME),'fm00'),2)


UPDATE bycicle_data_final
SET age = CASE WHEN birth_year IS NOT NULL THEN EXTRACT(YEAR FROM START_TIME) - birth_year END
---------------------------------------------------------------
	/* 
		ADDITIONAL CLEANING
		
		REMOVE UNKNOWN USER_TYPE, UNKNOWN SEX
		
	*/
---------------------------------------------------------------
DELETE FROM bycicle_data_final WHERE user_type IS  NULL OR gender = 0
---------------------------------------------------------------
	/* 
		CHECKING trip duration outliers
		
		DELETING OUTLIERS (AROUND 42,000 RECORDS HAVING SECOND DURATIONS THAT ARE EQUAL TO MORE THAN 1 DAY)
		
		ALSO CLEANING NULL AGE AND > 80
		
	*/
---------------------------------------------------------------

WITH CTE_1 AS ( 			
SELECT
AVG(trip_duration_sec) as avg_trip_duration,
STDDEV(trip_duration_sec) as stdev_trip_duration 
FROM bycicle_data_final
)
SELECT id
INTO z_zcore_duration_temp
FROM bycicle_data_final CROSS JOIN CTE_1
WHERE (trip_duration_sec - avg_trip_duration) / stdev_trip_duration > 2.9

DELETE FROM bycicle_data_final WHERE id IN (SELECT id from z_zcore_duration_temp) OR  trip_duration_sec < 180
DELETE FROM bycicle_data_final WHERE age IS NULL OR AGE > 80 
---------------------------------------------------------------
	/* 
	 CREATING OUTPUT REPORTS TO POPULATE VISUALIZATIONS IN TABLEAU
	*/
----------------------------------------------------	
drop table Final_Count_Records_by_Month

WITH CTE_1 AS (
SELECT PERIOD,
CASE WHEN yr = '2013' and mnt IN ('06','07','08','09','10','11','12') OR yr = '2014' and mnt IN ('01','02','03','04','05') THEN '1st yr Jun 2013 - May 2014'
	 WHEN yr = '2014' and mnt IN ('06','07','08','09','10','11','12') OR yr = '2015' and mnt IN ('01','02','03','04','05')  THEN '2nd yr Jun 2014 - May 2015'
	 WHEN yr = '2015' and mnt IN ('06','07','08','09','10','11','12') OR yr = '2016' and mnt IN ('01','02','03','04','05')  THEN '3th yr Jun 2015 - May 2016'
	 WHEN yr = '2016' and mnt IN ('06','07','08','09','10','11','12') OR yr = '2017' and mnt IN ('01','02','03','04','05')  THEN '4th yr Jun 2016 - May 2017'
	 WHEN yr = '2017' and mnt IN ('06','07','08','09','10','11','12') OR yr = '2018' and mnt IN ('01','02','03','04','05')  THEN '5th yr Jun 2017 - May 2018'
	 WHEN yr = '2018' and mnt IN ('06','07','08','09','10','11','12') OR yr = '2019' and mnt IN ('01','02','03','04','05')  THEN '6th yr Jun 2018 - May 2019'
	 WHEN yr = '2019' and mnt IN ('06','07','08','09','10','11','12') OR yr = '2020' and mnt IN ('01','02','03','04','05') THEN '7th yr Jun 2019 - May 2020'
END as real_period,
user_type,
CASE WHEN gender = 1 THEN 'Male'
	 WHEN gender = 2 THEN 'Female'
END as gender
FROM bycicle_data_final)

SELECT PERIOD, REAL_PERIOD, user_type, gender, COUNT(*) AS NO_TRIPS
INTO Final_Count_Records_by_Month
FROM CTE_1
WHERE REAL_PERIOD IS NOT NULL
GROUP BY PERIOD, REAL_PERIOD, user_type, gender
ORDER BY PERIOD, REAL_PERIOD, user_type, gender
-----------------------------------------------------------------------------------------------------------
SELECT
CASE WHEN  MNT IN ('03','04','05') THEN 'Spring'
	 WHEN  MNT IN ('06', '07', '08') then 'Summer'
	 WHEN  MNT IN ('09','10', '11') then 'Autumn'
	 WHEN  MNT IN ('12','01','02') THEN 'Winter'
END AS Season,
	 EXTRACT(HOUR FROM Start_Time) as Hour,
	 COUNT(*) as NO_TRIPS 
INTO Final_Count_Records_Season_Hour
FROM bycicle_data_final
GROUP BY
CASE WHEN  MNT IN ('03','04','05') THEN 'Spring'
	 WHEN  MNT IN ('06', '07', '08') then 'Summer'
	 WHEN  MNT IN ('09','10', '11') then 'Autumn'
	 WHEN  MNT IN ('12','01','02') THEN 'Winter'
END,
EXTRACT(HOUR FROM Start_Time)
ORDER BY Season, Hour
-----------------------------------------------------------------------------------------------------------
drop table Final_No_Trips_Starting;

SELECT Period, 
start_station_id,  COUNT(*) as No_trips
INTO Final_No_Trips_Starting
FROM bycicle_data_final as a
WHERE start_time BETWEEN '2019-06-01' AND '2020-05-31'
GROUP BY Period, start_station_id

ORDER BY No_trips desc
-------------------------------------------------
SELECT Period, 
end_station_id,  COUNT(*) as No_trips
INTO Final_No_Trips_Ending
FROM bycicle_data_final as a
WHERE start_time BETWEEN '2019-06-01' AND '2020-05-31'
GROUP BY Period, end_station_id

ORDER BY No_trips desc
----------------------------------------------------------------------------------------------------------

SELECT AGE, AVG(trip_duration_sec / 60)
INTO Final_age_trip_duration
FROM bycicle_data_final
GROUP BY AGE
-----------------------------------------------------------------------------------------------------------
SELECT bike_id, AVG(distance_miles) avg_miles, SUM(distance_miles) total_aprox_miles, COUNT(*) as no_trips
INTO Final_bike_trips
FROM bycicle_data_final as a
INNER JOIN distance_station as b ON  a.start_station_id = b.from_station AND a.end_station_id = b.to_station
WHERE start_time > '2019-12-31'
GROUP BY bike_id
-----------------------------------------------------------------------------------------------------------
SELECT * FROM Final_Count_Records_by_Month
SELECT * FROM Final_Count_Records_Season_Hour ORDER BY Season, no_trips desc
SELECT * FROM Final_No_Trips_Starting
SELECT * FROM Final_No_Trips_Ending
SELECT * FROM Final_age_trip_duration
SELECT * FROM Final_bike_trips ORDER BY total_aprox_miles desc
-----------------------------------------------------------------------------------------------------------
	/* 
	 EXPORTING CSVS 
	*/
----------------------------------------------------	
COPY (SELECT * FROM Final_Count_Records_by_Month) TO '/home/mario/Documentos/Final_Count_Records_by_Month.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM Final_Count_Records_Season_Hour) TO '/home/mario/Documentos/Final_Count_Records_Season_Hour.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM Final_No_Trips_Starting) TO '/home/mario/Documentos/Final_No_Trips_Starting.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM Final_No_Trips_Ending) TO '/home/mario/Documentos/Final_No_Trips_Ending.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM Final_age_trip_duration) TO '/home/mario/Documentos/Final_age_trip_duration.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM Final_bike_trips) TO '/home/mario/Documentos/Final_bike_trips.csv' DELIMITER ',' CSV HEADER;


