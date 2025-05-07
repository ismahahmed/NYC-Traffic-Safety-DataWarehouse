-- Create RequestFact
/*

CREATE TABLE RequestFact (
    RequestFactId SERIAL PRIMARY KEY,     
    Request_DimId INTEGER,                     
    MonthYearSubmitted_DimID TEXT,                    
    Borough_DimID Integer,                                                  
    total_approved INTEGER,             
    total_denied INTEGER,
    total_waiting INTEGER,
    total_requests INTEGER,

    FOREIGN KEY (Request_DimId) REFERENCES requestdim(requestdimid),
    FOREIGN KEY (MonthYearSubmitted_DimID) REFERENCES monthyeardim(month_year_id),
    FOREIGN KEY (Borough_DimID) REFERENCES boroughdim(borough_dim_id)
);

	
WITH cte AS (
    SELECT
        "RequestId",
        CAST("Month_DateTime_DateAdded" AS text) AS month,  
        CAST("Year_DateTime_DateAdded" AS INT4) AS "year",  
        "Borough" AS borough,
        COUNT(CASE WHEN "StatusCategory" = 'Request_In_Review' THEN 1 END) AS "Request_In_Review",
        COUNT(CASE WHEN "StatusCategory" LIKE 'Approved%' THEN 1 END) AS "Approved",  
        COUNT(CASE WHEN "StatusCategory" = 'Denied' THEN 1 END) AS "Denied"
    FROM requests
    GROUP BY month, "year", borough, "RequestId"
)
INSERT INTO RequestFact (
    Request_DimId,
    MonthYearSubmitted_DimID,
    Borough_DimID,
    total_approved,
    total_denied,
    total_waiting,
    total_requests
)
SELECT 
    rd.requestdimid,                      
    md.month_year_id,                    
    bd.borough_dim_id,                   
    cte."Approved",
    cte."Denied",
    cte."Request_In_Review",
    (cte."Approved" + cte."Denied" + cte."Request_In_Review") AS total_requests
FROM cte
JOIN monthyeardim md 
    ON cte.month = md."month"  
    AND cte.year = md."year"
JOIN boroughdim bd
    ON cte.borough = bd.borough
JOIN requestdim rd
    ON cte."RequestId" = rd."requestid"
WHERE rd.current_flag = True;

*/

SELECT * FROM requestfact; -- 55993

SELECT count(*) FROM requestfact; -- 55993
SELECT count(*) FROM requests; -- 55993

-- Approval rating broken down by borough and year
SELECT 
    bd."borough" AS borough,
    md."year" AS year,
    SUM(rf.total_approved) AS total_approved,
    SUM(rf.total_requests) AS total_requests,
    ROUND((SUM(rf.total_approved)::decimal / NULLIF(SUM(rf.total_requests), 0)) * 100, 2) AS approval_rate_percentage
FROM RequestFact rf
JOIN boroughdim bd ON rf.Borough_DimID = bd.borough_dim_id
JOIN monthyeardim md ON rf.MonthYearSubmitted_DimID = md.month_year_id
GROUP BY bd."borough", md."year"
ORDER BY bd."borough", md."year";

-- Approval rating broken down by borough and year -roll up to see subtotals
SELECT 
    COALESCE(bd."borough", 'ALL') AS borough,
    COALESCE(md."year", -1) AS year,
    SUM(rf.total_approved) AS total_approved,
    SUM(rf.total_requests) AS total_requests,
    ROUND((SUM(rf.total_approved)::decimal / NULLIF(SUM(rf.total_requests), 0)) * 100, 2) AS approval_rate_percentage
FROM RequestFact rf
JOIN boroughdim bd ON rf.Borough_DimID = bd.borough_dim_id
JOIN monthyeardim md ON rf.MonthYearSubmitted_DimID = md.month_year_id
GROUP BY ROLLUP(bd."borough", md."year")
ORDER BY 
    GROUPING(bd."borough"), 
    bd."borough",
    md."year";

-- Top 3 boroughs with the highest approval rating by year
WITH cte AS (
    SELECT 
        COALESCE(bd."borough", 'ALL') AS borough,
        COALESCE(md."year", -1) AS year,
        SUM(rf.total_approved) AS total_approved,
        SUM(rf.total_requests) AS total_requests,
        ROUND(
            (SUM(rf.total_approved)::decimal / NULLIF(SUM(rf.total_requests), 0)) * 100, 2
        ) AS approval_rate_percentage,
        ROW_NUMBER() OVER (
            PARTITION BY md."year"
            ORDER BY 
                (SUM(rf.total_approved)::decimal / NULLIF(SUM(rf.total_requests), 0)) DESC
        ) AS rank
    FROM RequestFact rf
    JOIN boroughdim bd ON rf.Borough_DimID = bd.borough_dim_id
    JOIN monthyeardim md ON rf.MonthYearSubmitted_DimID = md.month_year_id
    GROUP BY ROLLUP(bd."borough", md."year")
)
SELECT *
FROM cte
WHERE rank <= 3
  AND borough != 'ALL'  -- Exclude rollup values
  AND year != -1        -- Exclude ROLLUP values
ORDER BY year, rank;


-- BUSINESS QUESTION
-- Top Borough with the highest approval rating each year
WITH cte AS (
    SELECT 
        COALESCE(bd."borough", 'ALL') AS borough,
        COALESCE(md."year", -1) AS year,
        SUM(rf.total_approved) AS total_approved,
        SUM(rf.total_requests) AS total_requests,
        ROUND(
            (SUM(rf.total_approved)::decimal / NULLIF(SUM(rf.total_requests), 0)) * 100, 2
        ) AS approval_rate_percentage,
        ROW_NUMBER() OVER (
            PARTITION BY md."year"
            ORDER BY 
                (SUM(rf.total_approved)::decimal / NULLIF(SUM(rf.total_requests), 0)) DESC
        ) AS rank
    FROM RequestFact rf
    JOIN boroughdim bd ON rf.Borough_DimID = bd.borough_dim_id
    JOIN monthyeardim md ON rf.MonthYearSubmitted_DimID = md.month_year_id
    GROUP BY ROLLUP(bd."borough", md."year")
)
SELECT *
FROM cte
WHERE rank <= 1
  AND borough != 'ALL'  
  AND year != -1        
ORDER BY year, rank;


-- Which neighborhoods submit the most speed reducer requests, 
-- and how does the approval rate vary by neighborhood over time?

SELECT 
    boroughdim.borough,  
    monthyeardim.month,    
    monthyeardim.year,          
    SUM(rf.total_requests) AS total_requests,
    SUM(rf.total_approved) AS total_approved,
    SUM(rf.total_denied) AS total_denied,
    SUM(rf.total_approved) * 100.0 / NULLIF(SUM(rf.total_requests), 0) AS approval_rate
FROM 
    RequestFact rf
JOIN 
    boroughdim ON rf.Borough_DimID = boroughdim.borough_dim_id  -- Join to get borough name
JOIN 
    monthyeardim ON rf.MonthYearSubmitted_DimID = monthyeardim.month_year_id  -- Join to get month/year info
GROUP BY 
    boroughdim.borough, monthyeardim.month, monthyeardim.year
ORDER BY 
    boroughdim.borough, monthyeardim.year, monthyeardim.month;




