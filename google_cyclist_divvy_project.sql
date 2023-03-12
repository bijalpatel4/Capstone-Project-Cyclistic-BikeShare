/*
This is the SQL script used to import, compile and analyse the Cyclistic Case 
Study for the Capstone Project of the Google Data Analytics Professional Certificate.

The database used is MYSQL Database.
The steps followed:
1. Download the individual CSV documents
2. Import each into separate tables, regularising data types
3. Combine all the data into one table
4. Inspect data for anomalies
5. Identify and exclude data with anomalies
6. Create queries for data visualisations
The code below starts at step 3.
*/
SELECT count(*) FROM divvy_tripdata.divvy_tripdata;


 Alter Table  divvy_tripdata
 Drop column ride_length;
 
ALTER TABLE divvy_tripdata
 Drop column day_of_week;
 
Alter Table divvy_tripdata
Add ride_length_min INT;

ALTER TABLE divvy_tripdata
ADD exclude VARCHAR(50);
 
ALTER TABLE divvy_tripdata
ADD day_of_week VARCHAR(20);

ALTER TABLE divvy_tripdata
DROP COLUMN exculde;
 
UPDATE divvy_tripdata
set ride_length_min = Timestampdiff(minute,started_at,ended_at);

UPDATE divvy_tripdata
SET day_of_week = dayname(started_at);

/*Inspect data for anomalies*/

SELECT DISTINCT member_casual
FROM divvy_tripdata;

SELECT MIN(end_lat),MAX(end_lat),MIN(end_lng), MAX(end_lng), MIN(start_lng), MAX(start_lng),MIN(start_lat),
MAX(start_lat)
FROM divvy_tripdata;

SELECT end_station_id, end_station_name, COUNT(1)
FROM divvy_tripdata
GROUP BY end_station_id,end_station_name;

SELECT rideable_type, COUNT(1) 
FROM divvy_tripdata
GROUP BY rideable_type;

SELECT ride_id,COUNT(1)
FROM divvy_tripdata
GROUP BY ride_id
HAVING COUNT(1)>1;


SELECT *
FROM divvy_tripdata
WHERE started_At is NULL and ended_at is NULL;

SELECT *
FROM divvy_tripdata
WHERE started_At is NULL or ended_at is NULL;

SELECT *
FROM divvy_tripdata
WHERE exclude is NULL and (start_lng is NULL or end_lat is NULL);

SELECT *
FROM divvy_tripdata
WHERE exclude is null
and member_casual is null;

/*--- identify and exclude data anomalies----*/
/*----exclude where start time is less then end time */

SELECt count(1)
FROM divvy_tripdata
WHERE started_at >= ended_at;

UPDATE divvy_tripdata
set exclude = 'Y'
WHERE started_at >= ended_at;

/*---exclude where ride length is less then */

SELECT *
FROM divvy_tripdata
ORDER BY ride_length_min asc ;

UPDATE divvy_tripdata
SET exclude = 'Y'
WHERE ride_length_min <= 0;

SELECT *
FROM divvy_tripdata
WHERE exclude is NULL
ORDER BY ride_length_min asc;

SELECT ride_length_min, count(1)
FROM divvy_tripdata
WHERE exclude is NULL
GROUP BY ride_length_min;

/* Checking on start and end station name */

SELECT COUNT(1)
FROM divvy_tripdata
WHERE start_station_name is null and end_station_name is null;

SELECT *
FROM divvy_tripdata
WHERE start_station_name is NULL OR start_station_name = "";

UPDATE divvy_tripdata
SET exclude = 'Y'
WHERE start_station_name is null or start_station_name = "" or end_station_name is null
or end_station_name = "";

SELECT *
FROM divvy_tripdata
WHERE start_station_id = "" or end_station_id ="";

SELECT start_station_name, count(1)
from divvy_tripdata
WHERE exclude is null
GROUP BY start_station_name;

SELECT *
FROM divvy_tripdata
WHERE exclude is NULL
and (UPPER(start_station_name) LIKE '%BASE%WAREHOUSE%' or
UPPER(end_station_name) LIKE '%BASE%WAREHOUSE%');

UPDATE divvy_tripdata
SET exclude ='Y'
WHERE exclude is NULL
and (UPPER(start_station_name) LIKE '%BASE%WAREHOUSE%' or
UPPER(end_station_name) LIKE '%BASE%WAREHOUSE%');

SELECT COUNT(1)
FROM divvy_tripdata
WHERE exclude is NULL;

SELECT d.*,row_number() over() as rn
FROM divvy_tripdata d
WHERE exclude is NULL;
/* Data Visulization */

/*Total Numbers of Member & Casuals Riders Type*/

With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)

SELECT member_casual, COUNT(rn) AS Number_of_members
FROM dt
GROUP BY member_casual;


/*No. of Rider Type Per Day of the Week*/ 
With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)

SELECT member_casual,day_of_week, count(rn)
FROM dt
GrOUP BY member_casual,day_of_week;          
         
/*Average ride duration by rider type per day of the week*/
With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)

SELECT member_casual,day_of_week, AVG(ride_length_min) as Avg_ride_length_min
FROM dt
GrOUP BY member_casual,day_of_week;  


 
/* Number of Rider type per year */ 
With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)
SELECT member_casual,date_format(started_at, '%b-%Y') AS per_month_year,COUNT(rn) AS Number_of_members
FROM dt
GROUP BY member_casual,date_format(started_at, '%b-%Y'); 

With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)
SELECT member_casual,date_format(started_at, '%d') AS per_day_year,COUNT(rn) AS Number_of_members
FROM dt
GROUP BY member_casual,date_format(started_at, '%d'); 

      
/*Comparision of bike type by rider type */      
With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)
 SELECT member_casual,rideable_type, count(rn)
 FROM dt
 GROUP BY member_casual,rideable_type;
 
 
 With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)
 SELECT day_of_week,member_casual,ride_length_min,count(rn)
 FROM dt
  where ride_length_min >= 60
 GROUP BY day_of_week,member_casual,ride_length_min;


 /*Top 30 member casual riders */ 
 With dt AS (SELECT row_number() over() as rn,d.*
            FROM divvy_tripdata d
            WHERE exclude is NULL)
SELECT i.*
FROM(            
	SELECT member_casual, start_station_name,end_station_name,num,rank() over (partition by member_casual order by n.num desc) AS num_rank
	FROM(
	SELECT member_casual, start_station_name,end_station_name, COUNT(1) AS num
	FROM dt
	GROUP BY member_casual, start_station_name,end_station_name
	) as n
) as i 
WHERE i.num_rank <= 30;          


 




