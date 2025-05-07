-- CREATE TABLE
/*
CREATE TABLE crash_request_borough_month_fact (
    crash_fact_id SERIAL PRIMARY KEY, 
    borough_dim_id INTEGER, 
    month_year_id TEXT, 
    total_crashes INTEGER,
    total_injured INTEGER, 
    total_killed INTEGER,
    injury_rate_percentage NUMERIC,
    killed_rate_percentage NUMERIC,
    request_in_review INTEGER, 
    approved_requests INTEGER,
    denied_requests INTEGER,
    FOREIGN KEY (borough_dim_id) REFERENCES boroughdim(borough_dim_id),
    FOREIGN KEY (month_year_id) REFERENCES monthyeardim(month_year_id) 
);

-- INFO FOR CRASH ON MONTHYEAR BASIS
CREATE TEMPORARY TABLE crash_monthyear_agg AS
SELECT 
  boroughdim.borough_dim_id,
  monthyeardim.month_year_id,
  COUNT(crashes."COLLISION_ID") AS total_crashes,
  SUM(crashes."NUMBER OF PERSONS INJURED") AS total_injured,
  SUM(crashes."NUMBER OF PERSONS KILLED") AS total_killed,
  SUM(crashes."NUMBER OF PERSONS INJURED") / COUNT(crashes."COLLISION_ID") * 100 AS injury_rate_percentage,
  SUM(crashes."NUMBER OF PERSONS KILLED") / COUNT(crashes."COLLISION_ID") * 100 AS killed_rate_percentage
FROM crashes
JOIN boroughdim 
	ON crashes."BOROUGH" = boroughdim."borough"
JOIN monthyeardim 
    ON CAST(crashes."MONTH_CRASH_DATETIME" AS TEXT) = monthyeardim.MONTH
    AND CAST(crashes."YEAR_CRASH_DATETIME" AS INT) = monthyeardim.YEAR
GROUP BY 
  crashes."MONTH_CRASH_DATETIME", 
  crashes."YEAR_CRASH_DATETIME", 
  crashes."BOROUGH", 
  boroughdim.borough_dim_id, 
  monthyeardim.month_year_id
ORDER BY borough_dim_id, month_year_id;

     
CREATE TEMPORARY TABLE request_monthyear_agg AS
SELECT 
  boroughdim.borough_dim_id,
  monthyeardim.month_year_id,
  COUNT(CASE WHEN requests."StatusCategory" = 'Request_In_Review' THEN 1 END) AS "Request_In_Review",
  COUNT(CASE WHEN requests."StatusCategory" LIKE 'Approved%' THEN 1 END) AS "Approved",  
  COUNT(CASE WHEN requests."StatusCategory" = 'Denied' THEN 1 END) AS "Denied"
FROM requests
JOIN boroughdim 
    ON requests."Borough" = boroughdim."borough"
JOIN monthyeardim 
    ON CAST(requests."Month_DateTime_DateAdded" AS TEXT) = monthyeardim.MONTH
    AND CAST(requests."Year_DateTime_DateAdded" AS INT) = monthyeardim.YEAR
JOIN requestdim rd
    ON requests."RequestId" = rd."requestid" -- Join on RequestId between requests and requestsdim
WHERE rd.current_flag = TRUE  -- Filter for only true flags in the requestsdim table
GROUP BY boroughdim.borough_dim_id, monthyeardim.month_year_id
ORDER BY borough_dim_id, month_year_id;

select * FROM crash_monthyear_agg;
select * FROM request_monthyear_agg;

INSERT INTO crash_request_borough_month_fact (
    borough_dim_id,
    month_year_id,
    total_crashes,
    total_injured,
    total_killed,
    request_in_review,
    approved_requests,
    denied_requests,
    injury_rate_percentage,
    killed_rate_percentage
)
SELECT 
    cm.borough_dim_id,
    cm.month_year_id,
    cm.total_crashes,
    cm.total_injured,
    cm.total_killed,
    rm."Request_In_Review",
    rm."Approved",
    rm."Denied",
    (cm.total_injured / cm.total_crashes) * 100 AS injury_rate_percentage,  -- Injury rate calculation
    (cm.total_killed / cm.total_crashes) * 100 AS killed_rate_percentage   -- Killed rate calculation
FROM crash_monthyear_agg cm
JOIN request_monthyear_agg rm 
    ON cm.borough_dim_id = rm.borough_dim_id
    AND cm.month_year_id = rm.month_year_id
ORDER BY cm.borough_dim_id, cm.month_year_id;

--DROP TABLE IF EXISTS crash_monthyear_agg;
--DROP TABLE IF EXISTS request_monthyear_agg;
*/



SELECT * FROM crash_request_borough_month_fact;

--  Monthly Request Trends for Each Borough
SELECT 
    monthyeardim.month,  
    monthyeardim.YEAR,
    boroughdim.borough,  
    SUM(crash_request_borough_month_fact.request_in_review) AS total_in_review,
    SUM(crash_request_borough_month_fact.approved_requests) AS total_approved,
    SUM(crash_request_borough_month_fact.denied_requests) AS total_denied
FROM crash_request_borough_month_fact 
JOIN monthyeardim ON crash_request_borough_month_fact.month_year_id = monthyeardim.month_year_id
JOIN boroughdim ON crash_request_borough_month_fact.borough_dim_id = boroughdim.borough_dim_id
GROUP BY monthyeardim.month, monthyeardim.YEAR,boroughdim.borough
ORDER BY boroughdim.borough, monthyeardim.YEAR, monthyeardim.month;







