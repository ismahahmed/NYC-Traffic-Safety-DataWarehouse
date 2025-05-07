-- CREATING FACT TABLE AND INSERTING DATA
/*
CREATE TABLE crash_fact (
    crash_fact_id SERIAL PRIMARY KEY,
    street_dim_id INTEGER,
    date_dim_id TEXT,
    time_dim_id TEXT,
    month_year_id TEXT,
    vehicle_type_1_id INTEGER,
    vehicle_type_2_id INTEGER,
    vehicle_type_3_id INTEGER,
    vehicle_type_4_id INTEGER,
    vehicle_type_5_id INTEGER,
    contributing_factor_1_id INTEGER,
    contributing_factor_2_id INTEGER,
    contributing_factor_3_id INTEGER,
    contributing_factor_4_id INTEGER,
    contributing_factor_5_id INTEGER,
    total_injured INTEGER,
    total_killed INTEGER,
    pedestrians_injured INTEGER,
    pedestrians_killed INTEGER,
    cyclists_injured INTEGER,
    cyclists_killed INTEGER,
    motorists_injured INTEGER,
    motorists_killed INTEGER,
    FOREIGN KEY (street_dim_id) REFERENCES streetdim(street_dim_id),
    FOREIGN KEY (date_dim_id) REFERENCES datedim(date_dim_id),
    FOREIGN KEY (time_dim_id) REFERENCES timedim(time_dim_id),
    FOREIGN KEY (month_year_id) REFERENCES monthyeardim(month_year_id),
    FOREIGN KEY (vehicle_type_1_id) REFERENCES vehicletypedim(vehicletype_dim_id),
    FOREIGN KEY (vehicle_type_2_id) REFERENCES vehicletypedim(vehicletype_dim_id),
    FOREIGN KEY (vehicle_type_3_id) REFERENCES vehicletypedim(vehicletype_dim_id),
    FOREIGN KEY (vehicle_type_4_id) REFERENCES vehicletypedim(vehicletype_dim_id),
    FOREIGN KEY (vehicle_type_5_id) REFERENCES vehicletypedim(vehicletype_dim_id),
    FOREIGN KEY (contributing_factor_1_id) REFERENCES contributingfactor_dim(contributing_factor_id),
    FOREIGN KEY (contributing_factor_2_id) REFERENCES contributingfactor_dim(contributing_factor_id),
    FOREIGN KEY (contributing_factor_3_id) REFERENCES contributingfactor_dim(contributing_factor_id),
    FOREIGN KEY (contributing_factor_4_id) REFERENCES contributingfactor_dim(contributing_factor_id),
    FOREIGN KEY (contributing_factor_5_id) REFERENCES contributingfactor_dim(contributing_factor_id));


SELECT * FROM crash_fact;


INSERT INTO crash_fact (
    street_dim_id,
    date_dim_id,
    time_dim_id,
    month_year_id,
    vehicle_type_1_id,
    vehicle_type_2_id,
    vehicle_type_3_id,
    vehicle_type_4_id,
    vehicle_type_5_id,
    contributing_factor_1_id,
    contributing_factor_2_id,
    contributing_factor_3_id,
    contributing_factor_4_id,
    contributing_factor_5_id,
    total_injured,
    total_killed,
    pedestrians_injured,
    pedestrians_killed,
    cyclists_injured,
    cyclists_killed,
    motorists_injured,
    motorists_killed
)
SELECT 
    streetdim.street_dim_id,
    datedim.date_dim_id,
    timedim.time_dim_id,
    monthyeardim.month_year_id,
    v1dim.vehicletype_dim_id AS vehicletype1_id,
    v2dim.vehicletype_dim_id AS vehicletype2_id,
    v3dim.vehicletype_dim_id AS vehicletype3_id,
    v4dim.vehicletype_dim_id AS vehicletype4_id,
    v5dim.vehicletype_dim_id AS vehicletype5_id,    
    cf1dim.contributing_factor_id AS contributingfactor1_id,
    cf2dim.contributing_factor_id AS contributingfactor2_id,
    cf3dim.contributing_factor_id AS contributingfactor3_id,
    cf4dim.contributing_factor_id AS contributingfactor4_id,
    cf5dim.contributing_factor_id AS contributingfactor5_id,   
    crashes."NUMBER OF PERSONS INJURED"     AS total_injured,
    crashes."NUMBER OF PERSONS KILLED"      AS total_killed,
    crashes."NUMBER OF PEDESTRIANS INJURED" AS pedestrians_injured,
    crashes."NUMBER OF PEDESTRIANS KILLED"  AS pedestrians_killed,
    crashes."NUMBER OF CYCLIST INJURED"     AS cyclists_injured,
    crashes."NUMBER OF CYCLIST KILLED"      AS cyclists_killed,
    crashes."NUMBER OF MOTORIST INJURED"    AS motorists_injured,
    crashes."NUMBER OF MOTORIST KILLED"     AS motorists_killed
FROM crashes 
JOIN streetdim 
    ON COALESCE(crashes."STREET", '') = COALESCE(streetdim.streetname, '')  
    AND COALESCE(crashes."BOROUGH", '') = COALESCE(streetdim.borough, '')
    AND COALESCE(crashes."ZIP CODE", '') = COALESCE(streetdim.zipcode, '')
JOIN datedim
    ON CAST(crashes."CRASH_DATETIME" AS date) = datedim."date"
JOIN timedim 
    ON CAST(crashes."CRASH_DATETIME" AS time) = timedim."time"
JOIN monthyeardim 
    ON CAST(crashes."MONTH_CRASH_DATETIME" AS TEXT) = monthyeardim.MONTH
    AND CAST(crashes."YEAR_CRASH_DATETIME" AS INT) = monthyeardim.YEAR
JOIN vehicletypedim v1dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 1", 'UNKNOWN') = v1dim.vehiclename
JOIN vehicletypedim v2dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 2", 'UNKNOWN') = v2dim.vehiclename
JOIN vehicletypedim v3dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 3", 'UNKNOWN') = v3dim.vehiclename
JOIN vehicletypedim v4dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 4", 'UNKNOWN') = v4dim.vehiclename
JOIN vehicletypedim v5dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 5", 'UNKNOWN') = v5dim.vehiclename
JOIN contributingfactor_dim cf1dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 1" = cf1dim.contributing_factor
JOIN contributingfactor_dim cf2dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 2" = cf2dim.contributing_factor
JOIN contributingfactor_dim cf3dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 3" = cf3dim.contributing_factor
JOIN contributingfactor_dim cf4dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 4" = cf4dim.contributing_factor
JOIN contributingfactor_dim cf5dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 5" = cf5dim.contributing_factor;
    

*/

-- checks
SELECT count(*) FROM crashes;
SELECT count(*) FROM crash_fact;

SELECT * FROM crash_fact;

-- QUERIES

-- total crashes by year and month 
SELECT 
    monthyeardim.month AS month,
    monthyeardim.year AS year,
    COUNT(crash_fact_id) AS total_crashes
FROM Crash_Fact
JOIN monthyeardim 
    ON Crash_Fact.month_year_id = monthyeardim.month_year_id
GROUP BY monthyeardim.month, monthyeardim.year
ORDER BY year, month;

-- total crashes by year month and contributing factor
SELECT 
	cf.contributing_factor,
    monthyeardim.month AS month,
    monthyeardim.year AS year,
    COUNT(crash_fact_id) AS total_crashes
FROM Crash_Fact
JOIN monthyeardim 
    ON Crash_Fact.month_year_id = monthyeardim.month_year_id
JOIN contributingfactor_dim cf 
    ON Crash_Fact.contributing_factor_1_id = cf.contributing_factor_id
    OR Crash_Fact.contributing_factor_2_id = cf.contributing_factor_id
    OR Crash_Fact.contributing_factor_3_id = cf.contributing_factor_id
    OR Crash_Fact.contributing_factor_4_id = cf.contributing_factor_id
    OR Crash_Fact.contributing_factor_5_id = cf.contributing_factor_id
GROUP BY monthyeardim.month, monthyeardim.YEAR, cf.contributing_factor
ORDER BY year, MONTH, cf.contributing_factor;

-- Business question
-- highest contributing factor each month
WITH ranked_factors AS (
    SELECT 
        cf.contributing_factor,
        monthyeardim.month AS month,
        monthyeardim.year AS year,
        COUNT(crash_fact_id) AS total_crashes,
        ROW_NUMBER() OVER (
            PARTITION BY monthyeardim.year, monthyeardim.month
            ORDER BY COUNT(crash_fact_id) DESC
        ) AS rn
    FROM Crash_Fact
    JOIN monthyeardim 
        ON Crash_Fact.month_year_id = monthyeardim.month_year_id
    JOIN contributingfactor_dim cf 
        ON Crash_Fact.contributing_factor_1_id = cf.contributing_factor_id
        OR Crash_Fact.contributing_factor_2_id = cf.contributing_factor_id
        OR Crash_Fact.contributing_factor_3_id = cf.contributing_factor_id
        OR Crash_Fact.contributing_factor_4_id = cf.contributing_factor_id
        OR Crash_Fact.contributing_factor_5_id = cf.contributing_factor_id
    WHERE cf.contributing_factor NOT IN  ('NO CONTRIBUTING FACTOR DETAILS', 'UNSPECIFIED')
    GROUP BY monthyeardim.month, monthyeardim.year, cf.contributing_factor
)
SELECT 
    contributing_factor,
    month,
    year,
    total_crashes
FROM ranked_factors
WHERE rn = 1
ORDER BY year, month;





-- Crashes associated with each contributing factor
-- since a crash can have multiple cfs, we need to make sure each
-- cf is counted seperately 
SELECT 
    cf.contributing_factor,  -- the contributing factor description
    COUNT(*) AS total_crashes
FROM crash_fact cfact
JOIN contributingfactor_dim cf 
    ON cfact.contributing_factor_1_id = cf.contributing_factor_id
    OR cfact.contributing_factor_2_id = cf.contributing_factor_id
    OR cfact.contributing_factor_3_id = cf.contributing_factor_id
    OR cfact.contributing_factor_4_id = cf.contributing_factor_id
    OR cfact.contributing_factor_5_id = cf.contributing_factor_id
GROUP BY cf.contributing_factor
ORDER BY total_crashes DESC;


-- Total number of crashes by contributing factor for Sedan vehicle type
SELECT 
    cf.contributing_factor,
    COUNT(*) AS total_crashes
FROM crash_fact cfact
JOIN contributingfactor_dim cf 
    ON cfact.contributing_factor_1_id = cf.contributing_factor_id
    OR cfact.contributing_factor_2_id = cf.contributing_factor_id
    OR cfact.contributing_factor_3_id = cf.contributing_factor_id
    OR cfact.contributing_factor_4_id = cf.contributing_factor_id
    OR cfact.contributing_factor_5_id = cf.contributing_factor_id
JOIN vehicletypedim vt
    ON cfact.vehicle_type_1_id = vt.vehicletype_dim_id  
    OR cfact.vehicle_type_2_id = vt.vehicletype_dim_id
    OR cfact.vehicle_type_3_id = vt.vehicletype_dim_id
    OR cfact.vehicle_type_4_id = vt.vehicletype_dim_id
    OR cfact.vehicle_type_5_id = vt.vehicletype_dim_id
WHERE vt.vehiclename = 'SEDAN'  
GROUP BY cf.contributing_factor
ORDER BY total_crashes DESC;






SELECT 
    streetdim.street_dim_id,
    datedim.date_dim_id,
    timedim.time_dim_id,
    monthyeardim.month_year_id,
    v1dim.vehicletype_dim_id AS vehicletype1_id,
    v2dim.vehicletype_dim_id AS vehicletype2_id,
    v3dim.vehicletype_dim_id AS vehicletype3_id,
    v4dim.vehicletype_dim_id AS vehicletype4_id,
    v5dim.vehicletype_dim_id AS vehicletype5_id,    
    cf1dim.contributing_factor_id AS contributingfactor1_id,
    cf2dim.contributing_factor_id AS contributingfactor2_id,
    cf3dim.contributing_factor_id AS contributingfactor3_id,
    cf4dim.contributing_factor_id AS contributingfactor4_id,
    cf5dim.contributing_factor_id AS contributingfactor5_id,   
    crashes."NUMBER OF PERSONS INJURED"     AS total_injured,
    crashes."NUMBER OF PERSONS KILLED"      AS total_killed,
    crashes."NUMBER OF PEDESTRIANS INJURED" AS pedestrians_injured,
    crashes."NUMBER OF PEDESTRIANS KILLED"  AS pedestrians_killed,
    crashes."NUMBER OF CYCLIST INJURED"     AS cyclists_injured,
    crashes."NUMBER OF CYCLIST KILLED"      AS cyclists_killed,
    crashes."NUMBER OF MOTORIST INJURED"    AS motorists_injured,
    crashes."NUMBER OF MOTORIST KILLED"     AS motorists_killed
FROM crashes 
JOIN streetdim 
    ON COALESCE(crashes."STREET", '') = COALESCE(streetdim.streetname, '')  
    AND COALESCE(crashes."BOROUGH", '') = COALESCE(streetdim.borough, '')
    AND COALESCE(crashes."ZIP CODE", '') = COALESCE(streetdim.zipcode, '')
JOIN datedim
    ON CAST(crashes."CRASH_DATETIME" AS date) = datedim."date"
JOIN timedim 
    ON CAST(crashes."CRASH_DATETIME" AS time) = timedim."time"
JOIN monthyeardim 
    ON CAST(crashes."MONTH_CRASH_DATETIME" AS TEXT) = monthyeardim.MONTH
    AND CAST(crashes."YEAR_CRASH_DATETIME" AS INT) = monthyeardim.YEAR
JOIN vehicletypedim v1dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 1", 'UNKNOWN') = v1dim.vehiclename
JOIN vehicletypedim v2dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 2", 'UNKNOWN') = v2dim.vehiclename
JOIN vehicletypedim v3dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 3", 'UNKNOWN') = v3dim.vehiclename
JOIN vehicletypedim v4dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 4", 'UNKNOWN') = v4dim.vehiclename
JOIN vehicletypedim v5dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 5", 'UNKNOWN') = v5dim.vehiclename
JOIN contributingfactor_dim cf1dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 1" = cf1dim.contributing_factor
JOIN contributingfactor_dim cf2dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 2" = cf2dim.contributing_factor
JOIN contributingfactor_dim cf3dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 3" = cf3dim.contributing_factor
JOIN contributingfactor_dim cf4dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 4" = cf4dim.contributing_factor
JOIN contributingfactor_dim cf5dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 5" = cf5dim.contributing_factor;
   
SELECT * FROM crashes LIMIT 10000;

SELECT 
    streetdim.street_dim_id,
    datedim.date_dim_id,
    timedim.time_dim_id,
    monthyeardim.month_year_id,
    v1dim.vehicletype_dim_id AS vehicletype1_id,
    v2dim.vehicletype_dim_id AS vehicletype2_id,
    v3dim.vehicletype_dim_id AS vehicletype3_id,
    v4dim.vehicletype_dim_id AS vehicletype4_id,
    v5dim.vehicletype_dim_id AS vehicletype5_id,    
    cf1dim.contributing_factor_id AS contributingfactor1_id,
    cf2dim.contributing_factor_id AS contributingfactor2_id,
    cf3dim.contributing_factor_id AS contributingfactor3_id,
    cf4dim.contributing_factor_id AS contributingfactor4_id,
    cf5dim.contributing_factor_id AS contributingfactor5_id,   
    crashes."NUMBER OF PERSONS INJURED"     AS total_injured,
    crashes."NUMBER OF PERSONS KILLED"      AS total_killed,
    crashes."NUMBER OF PEDESTRIANS INJURED" AS pedestrians_injured,
    crashes."NUMBER OF PEDESTRIANS KILLED"  AS pedestrians_killed,
    crashes."NUMBER OF CYCLIST INJURED"     AS cyclists_injured,
    crashes."NUMBER OF CYCLIST KILLED"      AS cyclists_killed,
    crashes."NUMBER OF MOTORIST INJURED"    AS motorists_injured,
    crashes."NUMBER OF MOTORIST KILLED"     AS motorists_killed
FROM crashes 
JOIN streetdim 
    ON COALESCE(crashes."STREET", '') = COALESCE(streetdim.streetname, '')  
    AND COALESCE(crashes."BOROUGH", '') = COALESCE(streetdim.borough, '')
    AND COALESCE(crashes."ZIP CODE", '') = COALESCE(streetdim.zipcode, '')
JOIN datedim
    ON CAST(crashes."CRASH_DATETIME" AS date) = datedim."date"
JOIN timedim 
    ON CAST(crashes."CRASH_DATETIME" AS time) = timedim."time"
JOIN monthyeardim 
    ON CAST(crashes."MONTH_CRASH_DATETIME" AS TEXT) = monthyeardim.MONTH
    AND CAST(crashes."YEAR_CRASH_DATETIME" AS INT) = monthyeardim.YEAR
JOIN vehicletypedim v1dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 1", 'UNKNOWN') = v1dim.vehiclename
JOIN vehicletypedim v2dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 2", 'UNKNOWN') = v2dim.vehiclename
JOIN vehicletypedim v3dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 3", 'UNKNOWN') = v3dim.vehiclename
JOIN vehicletypedim v4dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 4", 'UNKNOWN') = v4dim.vehiclename
JOIN vehicletypedim v5dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 5", 'UNKNOWN') = v5dim.vehiclename
JOIN contributingfactor_dim cf1dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 1" = cf1dim.contributing_factor
JOIN contributingfactor_dim cf2dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 2" = cf2dim.contributing_factor
JOIN contributingfactor_dim cf3dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 3" = cf3dim.contributing_factor
JOIN contributingfactor_dim cf4dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 4" = cf4dim.contributing_factor
JOIN contributingfactor_dim cf5dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 5" = cf5dim.contributing_factor;



SELECT 
    streetdim.street_dim_id,
    datedim.date_dim_id,
    timedim.time_dim_id,
    monthyeardim.month_year_id,
    v1dim.vehicletype_dim_id AS vehicletype1_id,
    v2dim.vehicletype_dim_id AS vehicletype2_id,
    v3dim.vehicletype_dim_id AS vehicletype3_id,
    v4dim.vehicletype_dim_id AS vehicletype4_id,
    v5dim.vehicletype_dim_id AS vehicletype5_id,    
    cf1dim.contributing_factor_id AS contributingfactor1_id,
    cf2dim.contributing_factor_id AS contributingfactor2_id,
    cf3dim.contributing_factor_id AS contributingfactor3_id,
    cf4dim.contributing_factor_id AS contributingfactor4_id,
    cf5dim.contributing_factor_id AS contributingfactor5_id,   
    crashes."NUMBER OF PERSONS INJURED" AS total_injured,
    crashes."NUMBER OF PERSONS KILLED" AS total_killed,
    crashes."NUMBER OF PEDESTRIANS INJURED" AS pedestrians_injured,
    crashes."NUMBER OF PEDESTRIANS KILLED" AS pedestrians_killed,
    crashes."NUMBER OF CYCLIST INJURED" AS cyclists_injured,
    crashes."NUMBER OF CYCLIST KILLED" AS cyclists_killed,
    crashes."NUMBER OF MOTORIST INJURED" AS motorists_injured,
    crashes."NUMBER OF MOTORIST KILLED" AS motorists_killed
FROM crashes
JOIN streetdim 
    ON COALESCE(crashes."STREET", '') = COALESCE(streetdim.streetname, '')  
    AND COALESCE(crashes."BOROUGH", '') = COALESCE(streetdim.borough, '')
    AND COALESCE(crashes."ZIP CODE", '') = COALESCE(streetdim.zipcode, '')
JOIN datedim
    ON CAST(crashes."CRASH_DATETIME" AS date) = datedim."date"
JOIN timedim 
    ON CAST(crashes."CRASH_DATETIME" AS time) = timedim."time"
JOIN monthyeardim 
    ON CAST(crashes."MONTH_CRASH_DATETIME" AS TEXT) = monthyeardim.MONTH
    AND CAST(crashes."YEAR_CRASH_DATETIME" AS INT) = monthyeardim.YEAR
JOIN vehicletypedim v1dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 1", 'UNKNOWN') = v1dim.vehiclename
JOIN vehicletypedim v2dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 2", 'UNKNOWN') = v2dim.vehiclename
JOIN vehicletypedim v3dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 3", 'UNKNOWN') = v3dim.vehiclename
JOIN vehicletypedim v4dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 4", 'UNKNOWN') = v4dim.vehiclename
JOIN vehicletypedim v5dim 
    ON COALESCE(crashes."VEHICLE TYPE CODE 5", 'UNKNOWN') = v5dim.vehiclename
JOIN contributingfactor_dim cf1dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 1" = cf1dim.contributing_factor
JOIN contributingfactor_dim cf2dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 2" = cf2dim.contributing_factor
JOIN contributingfactor_dim cf3dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 3" = cf3dim.contributing_factor
JOIN contributingfactor_dim cf4dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 4" = cf4dim.contributing_factor
JOIN contributingfactor_dim cf5dim 
    ON crashes."CONTRIBUTING FACTOR VEHICLE 5" = cf5dim.contributing_factor;

SELECT * FROM crashes;
