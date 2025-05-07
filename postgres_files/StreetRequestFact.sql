-- CREATE TABLE
/*
CREATE TABLE Street_Request_Crash_Fact (
	Street_Request_Crash_Fact SERIAL PRIMARY KEY,
	requestdimid Integer,
	requestdate_dimid text,
	requesttime_dimid text,
	street_dimid Integer,
	crashdate_dimid text,
	crashtime_dimid text,
	crash_id text, -- degenerative dimension
	approved_speed_reducers Int,
	denied_speed_reducers Int,

    FOREIGN KEY (requestdimid) REFERENCES requestdim(requestdimid),
    FOREIGN KEY (requestdate_dimid) REFERENCES datedim(date_dim_id),
    FOREIGN KEY (requesttime_dimid) REFERENCES timedim(time_dim_id),
    FOREIGN KEY (street_dimid) REFERENCES streetdim(street_dim_id),
    FOREIGN KEY (crashdate_dimid) REFERENCES datedim(date_dim_id),
    FOREIGN KEY (crashtime_dimid) REFERENCES timedim(time_dim_id)
);
*/

-- View Fact Table
SELECT * FROM Street_Request_Crash_Fact;


-- INSERTING DATA INTO FACT TABLE 
/*

-- Step 1: Creating temp table that will hold agg of requests
-- also gets fk's from required dim tables
CREATE TEMPORARY TABLE temp_request_aggregates AS
SELECT 
	requestdim.requestdimid,
    datedim.date_dim_id AS requestdate_id,
    timedim.time_dim_id AS requesttime_id,
    streetdim.street_dim_id AS requeststreet_id,
    requests."RequestId" AS requestid,
    CAST(requests."DateAdded" AS DATE) AS requestdate, 
    TO_CHAR(requests."DateAdded"::timestamp, 'HH24:MI:SS') AS requesttime,  
    COALESCE(requests."Street", '') AS street,
    COALESCE(requests."ZIP CODE", '') AS zipcode,
    COALESCE(requests."Borough", '') AS borough,
    COUNT(CASE WHEN requests."StatusCategory" LIKE 'Approved%' THEN 1 END) AS "Approved_Speed_Reducers",  
    COUNT(CASE WHEN requests."StatusCategory" = 'Denied' THEN 1 END) AS "Denied_Speed_Reducers"
FROM requests
JOIN streetdim 
    ON requests."Street" = streetdim.streetname  
    AND requests."Borough" = streetdim.borough
    AND requests."ZIP CODE" = streetdim.zipcode
JOIN datedim 
    ON requests."DateAdded"::date = datedim.date  
JOIN timedim 
    ON CAST(requests."DateAdded" AS TIME) = timedim.time
JOIN requestdim
	ON requests."RequestId" = requestdim.requestid
WHERE requestdim.current_flag = True
GROUP BY 
	requestdim.requestdimid,
    datedim.date_dim_id,
    timedim.time_dim_id,
    streetdim.street_dim_id,
    requests."RequestId", 
    CAST(requests."DateAdded" AS DATE),
    TO_CHAR(requests."DateAdded"::timestamp, 'HH24:MI:SS'),
    COALESCE(requests."Street", ''),
    COALESCE(requests."ZIP CODE", ''),
    COALESCE(requests."Borough", '');

SELECT * FROM temp_request_aggregates;

-- Step 2: Joining crashes table with requests agg
CREATE TEMPORARY TABLE temp_requestcrash_aggregates AS
SELECT 
	temp_request_aggregates.*, 
    CAST(crashes."CRASH_DATETIME" AS DATE) AS crashdate, 
    TO_CHAR(crashes."CRASH_DATETIME"::timestamp, 'HH24:MI:SS') AS crashtime,
    crashes."COLLISION_ID" AS "crash_id",
    datedim.date_dim_id AS crashdate_id,
    timedim.time_dim_id AS crashtime_id
FROM temp_request_aggregates
JOIN crashes 
	ON temp_request_aggregates."street" = crashes."STREET"
	AND temp_request_aggregates."borough" = crashes."BOROUGH"
	AND temp_request_aggregates."zipcode" = crashes."ZIP CODE"
JOIN datedim 
    ON CAST(crashes."CRASH_DATETIME" AS DATE) = datedim.date  
JOIN timedim 
    ON crashes."CRASH_DATETIME"::time = timedim.time;

-- View data
SELECT * FROM temp_requestcrash_aggregates LIMIT 100;

-- Insert Data
/*
INSERT INTO Street_Request_Crash_Fact (
    requestdimid, 
    requestdate_dimid, 
    requesttime_dimid, 
    street_dimid, 
    crashdate_dimid, 
    crashtime_dimid, 
    crash_id, 
    approved_speed_reducers, 
    denied_speed_reducers
)
SELECT 
    requestdimid,
    requestdate_id AS requestdate_dimid,
    requesttime_id AS requesttime_dimid,
    requeststreet_id AS street_dimid,
    crashdate_id AS crashdate_dimid,
    crashtime_id AS crashtime_dimid,
    crash_id AS crash_id,  -- degenerative dimension
    "Approved_Speed_Reducers" AS approved_speed_reducers,
    "Denied_Speed_Reducers" AS denied_speed_reducers
FROM temp_requestcrash_aggregates;
*/
*/

-- Fact Table Data
SELECT * FROM Street_Request_Crash_Fact; 

-- ANALYTICAL QUERY:
-- Are speed reducer requests more likely to be approved in locations 
-- with a high number of past collisions, as measured by the approval rate (%)

CREATE TEMPORARY TABLE street_summary_by_year AS
WITH facttableinfo AS ( -- crash + request events, joins dim tables
    SELECT 
        Street_Request_Crash_Fact.*,
        RequestDim.requestid,
        RequestDateDim.date AS requestdate,
        RequestTimeDim.time AS requesttime,
        StreetDim.streetname AS street,
        StreetDim.zipcode AS zipcode,
        StreetDim.borough AS borough,
        CrashDateDim.date AS crashdate,
        CrashTimeDim.time AS crashtime
    FROM Street_Request_Crash_Fact
    JOIN RequestDim
        ON Street_Request_Crash_Fact.requestdimid = RequestDim.requestdimid
    JOIN datedim AS RequestDateDim  
        ON Street_Request_Crash_Fact.requestdate_dimid = RequestDateDim.date_dim_id
    JOIN timedim AS RequestTimeDim  
        ON Street_Request_Crash_Fact.requesttime_dimid = RequestTimeDim.time_dim_id
    JOIN datedim AS CrashDateDim  
        ON Street_Request_Crash_Fact.crashdate_dimid = CrashDateDim.date_dim_id
    JOIN timedim AS CrashTimeDim  
        ON Street_Request_Crash_Fact.crashtime_dimid = CrashTimeDim.time_dim_id
    JOIN streetdim AS StreetDim  
        ON Street_Request_Crash_Fact.street_dimid = StreetDim.street_dim_id
),
request_distinct AS ( -- cte gets DISTINCT requests, extracting YEAR AND keeping summaries
    SELECT DISTINCT 
        requestid, 
        EXTRACT(YEAR FROM requestdate) AS request_year,
        street, 
        zipcode, 
        borough, 
        approved_speed_reducers, 
        denied_speed_reducers
    FROM facttableinfo
),
requests_agg AS ( -- aggregates the request_distinct cte DATA TO GET total approved/denied BY location AND year
    SELECT 
        street, 
        zipcode, 
        borough, 
        request_year,
        SUM(approved_speed_reducers) AS approved_speed_reducers,
        SUM(denied_speed_reducers) AS denied_speed_reducers
    FROM request_distinct 
    GROUP BY request_year, street, zipcode, borough
),
crashes_distinct AS ( -- cte gets DISTINCT crash events BY the id
    SELECT DISTINCT 
        EXTRACT(YEAR FROM crashdate) AS crash_year,
        crash_id,
        street, 
        zipcode,
        borough
    FROM facttableinfo
),
crashes_agg AS ( -- aggregates the crashes_distinct cte TO GET total crashes per LOCATION AND year
    SELECT 
        crash_year,
        street, 
        zipcode, 
        borough, 
        COUNT(crash_id) AS total_crashes
    FROM crashes_distinct 
    GROUP BY crash_year, street, zipcode, borough
)
SELECT -- joining the requests agg DATA AND crashes agg DATA 
    r.request_year AS year, 
    r.street, 
    r.zipcode, 
    r.borough, 
    r.approved_speed_reducers, 
    r.denied_speed_reducers, 
    c.total_crashes
FROM requests_agg r
JOIN crashes_agg c 
    ON r.street = c.street 
    AND r.zipcode = c.zipcode 
    AND r.borough = c.borough
    AND r.request_year = c.crash_year
WHERE r.request_year != 2012
ORDER BY YEAR;


SELECT * FROM street_summary_by_year;

-- QUESTION: 
--Are speed reducer requests more likely to be approved in locations with a 
--high number of past collisions, as measured by the approval rate (%)

-- First lets get the Approval Rate (%) of Requests by Year and Borough
SELECT 
	YEAR,
	borough,
	SUM(approved_speed_reducers) AS approved_count,
	SUM(denied_speed_reducers) AS denied_count,
	SUM(total_crashes) AS total_crashes,
	ROUND(100.0 * SUM(approved_speed_reducers) / NULLIF(SUM(approved_speed_reducers) + 
	SUM(denied_speed_reducers), 0), 2) AS approval_rate
FROM 
	street_summary_by_year
GROUP BY 
	YEAR, borough
ORDER BY 
	YEAR, borough;

-- Second, lets get a better understanding of the distribution of our data
SELECT 
	MIN(total_crashes) AS min_crashes,
	MAX(total_crashes) AS max_crashes,
	AVG(total_crashes) AS avg_crashes,
	PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_crashes) AS p25,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_crashes) AS median,
	PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_crashes) AS p75
FROM (
	SELECT 
		YEAR,
		borough,
		SUM(total_crashes) AS total_crashes
	FROM 
		street_summary_by_year
	GROUP BY 
		YEAR, borough
) sub;

-- Finally, using the measures found above, lets get the approval rate by crash level and borough
WITH crash_data AS (
	SELECT 
		YEAR,
		borough,
		SUM(approved_speed_reducers) AS approved_count,
		SUM(denied_speed_reducers) AS denied_count,
		SUM(total_crashes) AS total_crashes,
		ROUND(100.0 * SUM(approved_speed_reducers) / 
		NULLIF(SUM(approved_speed_reducers) + SUM(denied_speed_reducers), 0), 2) AS approval_rate
	FROM street_summary_by_year
	GROUP BY YEAR, borough
),
crash_bins AS (
	SELECT *,
	CASE 
		WHEN total_crashes < 384 THEN 'Low'
		WHEN total_crashes BETWEEN 384 AND 919 THEN 'Medium-Low'
		WHEN total_crashes BETWEEN 920 AND 2384 THEN 'Medium-High'
		ELSE 'High'
	END AS crash_level FROM crash_data)
SELECT 
	borough,
	crash_level,
	COUNT(*) AS num_records,
	ROUND(AVG(approval_rate), 2) AS avg_approval_rate
FROM crash_bins
GROUP BY borough, crash_level
ORDER BY borough,
	CASE 
	WHEN crash_level = 'Low' THEN 1
	WHEN crash_level = 'Medium-Low' THEN 2
	WHEN crash_level = 'Medium-High' THEN 3
	WHEN crash_level = 'High' THEN 4
	END;

-- reshaping
WITH crash_data AS (
	SELECT 
		YEAR,
		borough,
		SUM(approved_speed_reducers) AS approved_count,
		SUM(denied_speed_reducers) AS denied_count,
		SUM(total_crashes) AS total_crashes,
		ROUND(100.0 * SUM(approved_speed_reducers) / 
		NULLIF(SUM(approved_speed_reducers) + SUM(denied_speed_reducers), 0), 2) AS approval_rate
	FROM street_summary_by_year GROUP BY YEAR, borough
),
crash_bins AS (
	SELECT *,
		CASE 
			WHEN total_crashes < 384 THEN 'Low'
			WHEN total_crashes BETWEEN 384 AND 919 THEN 'Medium_Low'
			WHEN total_crashes BETWEEN 920 AND 2384 THEN 'Medium_High'
			ELSE 'High'
		END AS crash_level FROM crash_data
) SELECT 
	borough,
	ROUND(AVG(CASE WHEN crash_level = 'Low' THEN approval_rate END), 2) AS low_crash_approvalrate,
	ROUND(AVG(CASE WHEN crash_level = 'Medium_Low' THEN approval_rate END), 2) AS mediumcrash_approvalrate,
	ROUND(AVG(CASE WHEN crash_level = 'Medium_High' THEN approval_rate END), 2) AS mediumhighcrash_approvalrate,
	ROUND(AVG(CASE WHEN crash_level = 'High' THEN approval_rate END), 2) AS highcrash_approvalrate
FROM crash_bins GROUP BY borough ORDER BY borough;



/*
DROP TABLE temp_request_aggregates;
DROP TABLE temp_requestcrash_aggregates;
DROP TABLE facttableinfo;
*/


/*
SELECT * 
FROM requests 
WHERE requests."Street" = 'OCEAN PARKWAY'
  AND requests."ZIP CODE" = '11230'
  AND requests."Borough" = 'BROOKLYN';
*/

