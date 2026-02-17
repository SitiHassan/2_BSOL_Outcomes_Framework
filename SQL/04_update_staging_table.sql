/*=================================================================================================
 Siti Hassan
 Last updated: 2025-11-07
 Purpose(s):
 1- To merge latest datasets from multiple sources (SQL, API, SharePoint, Other)
 2- To cleanse dataset by removing unknown IMD & ethnicity and replace 1-21 ethnicity codes with 41-49
 3- To update denominators for census-required indicators (including those without events/numerators)
    - DASR (value_type_code = 4): needs full 5-year age bands within metadata min/max age codes
    - Crude rate (value_type_code = 3): NO 5-year splits, use metadata age band as-is
 4- To insert the final dataset into a staging table for processing
=================================================================================================*/
USE EAT_Reporting_BSOL;

/*=================================================================================================
Update 1-21 with 41-49 ethnicity coding for SQL data
=================================================================================================*/
DROP TABLE IF EXISTS #sql_data

SELECT * INTO #sql_data 
FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_SQL_Data]


UPDATE a
SET a.ethnicity_code = b.ethnicity_code_OF
FROM #sql_data AS a
JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Ethnicity] AS b
  ON a.ethnicity_code = b.ethnicity_code
WHERE a.ethnicity_code BETWEEN 1 AND 21 ; --646168


/*=================================================================================================
 Merge datasets from all sources with normalised data types
=================================================================================================*/
DROP TABLE IF EXISTS #all;

SELECT *
INTO #all
FROM (
    -- SQL
    SELECT 
        CAST(indicator_id AS INT) AS indicator_id,
        start_date,
        end_date,
        SUM(numerator) AS numerator,
        SUM(denominator) AS denominator,
        NULL AS indicator_value, 
        NULL AS lower_ci95,           
        NULL AS upper_ci95,           
        CAST(imd_code AS INT) AS imd_code,
        CAST(aggregation_id AS INT) AS aggregation_id,
        CAST(age_group_code AS INT) AS age_group_code,
        CAST(sex_code AS INT) AS sex_code,
        CAST(ethnicity_code AS INT) AS ethnicity_code,
        creation_date,
        value_type_code,
        source_code
    FROM #sql_data
	GROUP BY CAST(indicator_id AS INT), -- group up data as we've updated ethnicity coding earlier so data needs aggregating
        start_date,
        end_date,
        CAST(imd_code AS INT),
        CAST(aggregation_id AS INT),
        CAST(age_group_code AS INT),
        CAST(sex_code AS INT),
        CAST(ethnicity_code AS INT),
        creation_date,
        value_type_code,
        source_code
	

    UNION ALL

    -- API
    SELECT 
        CAST(indicator_id AS INT) AS indicator_id,
        start_date,
        end_date,
        numerator,
        denominator,
        indicator_value,
        lower_ci95,
        upper_ci95,
        CAST(imd_code AS INT) AS imd_code,
        CAST(aggregation_id AS INT) AS aggregation_id,
        CAST(age_group_code AS INT) AS age_group_code,
        CAST(sex_code AS INT) AS sex_code,
        CAST(ethnicity_code AS INT) AS ethnicity_code,
        creation_date,
        value_type_code,
        source_code
    FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_API_Data]

    UNION ALL

    -- SHAREPOINT
    SELECT 
        CAST(indicator_id AS INT) AS indicator_id,
        start_date,
        end_date,
        numerator,
        denominator,
        indicator_value,
        lower_ci95,
        upper_ci95,
        CAST(imd_code AS INT) AS imd_code,
        CAST(aggregation_id AS INT) AS aggregation_id,
        CAST(age_group_code AS INT) AS age_group_code,
        CAST(sex_code AS INT) AS sex_code,
        CAST(ethnicity_code AS INT) AS ethnicity_code,
        creation_date,
        value_type_code,
        source_code
    FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Sharepoint_Data]

    UNION ALL

    -- OTHER
    SELECT 
        CAST(indicator_id AS INT) AS indicator_id,
        start_date,
        end_date,
        numerator,
        denominator,
        indicator_value,
        lower_ci95,
        upper_ci95,
        CAST(imd_code AS INT) AS imd_code,
        CAST(aggregation_id AS INT) AS aggregation_id,
        CAST(age_group_code AS INT) AS age_group_code,
        CAST(sex_code AS INT) AS sex_code,
        CAST(ethnicity_code AS INT) AS ethnicity_code,
        creation_date,
        value_type_code,
        source_code
    FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Other_Data]
) AS t;  --1065759


/*=================================================================================================
 Remove unknown IMD and ethnicity (code = -99)
=================================================================================================*/
DELETE FROM #all
WHERE imd_code = -99 OR ethnicity_code = -99; --50120

/*=================================================================================================
 Keep only the latest created data per source + indicator
=================================================================================================*/
DROP TABLE IF EXISTS #max_date;

SELECT
  indicator_id,
  source_code,
  MAX(creation_date) AS creation_date
INTO #max_date
FROM #all
GROUP BY indicator_id, source_code; --95

DROP TABLE IF EXISTS #latest_all;

SELECT  a.indicator_id,
        a.start_date,
        a.end_date,
        a.numerator,
        a.denominator,
        a.indicator_value,
        a.lower_ci95,
        a.upper_ci95,
        a.imd_code,
        a.aggregation_id,
        a.age_group_code,
        a.sex_code,
        a.ethnicity_code,
        a.creation_date,
        a.value_type_code,
        a.source_code
INTO #latest_all
FROM #all AS a
INNER JOIN #max_date AS b
  ON a.indicator_id  = b.indicator_id
 AND a.source_code   = b.source_code
 AND a.creation_date = b.creation_date --991,636


/*=================================================================================================
 Identify SQL indicators that need Census denominator, split by type
=================================================================================================*/
DROP TABLE IF EXISTS #census_dasr;
DROP TABLE IF EXISTS #census_crude;
DROP TABLE IF EXISTS #require_census;

-- DASR (Directly age-standardised rate) = value_type_code = 4
SELECT DISTINCT indicator_id
INTO #census_dasr
FROM #latest_all
WHERE source_code = 1 --SQL
  AND denominator IS NULL -- require census
  AND value_type_code = 4; -- 17

-- Crude rate = value_type_code = 3
SELECT DISTINCT indicator_id
INTO #census_crude
FROM #latest_all
WHERE source_code = 1 --SQL
  AND denominator IS NULL -- require census
  AND value_type_code = 3; -- 6

-- Combined list of all census-required SQL indicators
SELECT indicator_id
INTO #require_census
FROM #census_dasr
UNION
SELECT indicator_id
FROM #census_crude; -- 23 total


/*=================================================================================================
 Build population table (all age codes, 41–49 ethnicity codes)
=================================================================================================*/
DROP TABLE IF EXISTS #pop;

SELECT
    aggregation_id,
    imd_code,
    age_code,
    sex_code,
    e.ethnicity_code_OF AS ethnicity_code, --41-49 ethnicity codes
    SUM(observation) AS denominator
INTO #pop
FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Population] AS p
JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Ethnicity] AS e
  ON p.ethnicity_code = e.ethnicity_code -- 1-21 ethnicity codes
GROUP BY
    aggregation_id,
    imd_code,
    age_code,
    sex_code,
    e.ethnicity_code_OF; --359100

/*=================================================================================================
 Build period set per DASR census indicator
=================================================================================================*/
DROP TABLE IF EXISTS #periods_dasr;

SELECT
    s.indicator_id,
    s.start_date,
    s.end_date
INTO #periods_dasr
FROM #latest_all AS s
WHERE s.source_code = 1
  AND s.indicator_id IN (SELECT indicator_id FROM #census_dasr)
GROUP BY s.indicator_id, s.start_date, s.end_date; --196

/*=================================================================================================
 Extract metadata per DASR indicator
=================================================================================================*/
DROP TABLE IF EXISTS #indicator_meta_dasr;

SELECT
    s.indicator_id,
    MAX(s.value_type_code) AS value_type_code,
    MAX(s.source_code)     AS source_code,
    MAX(s.creation_date)   AS creation_date
INTO #indicator_meta_dasr
FROM #latest_all AS s
WHERE s.source_code = 1
  AND s.indicator_id IN (SELECT indicator_id FROM #census_dasr)
GROUP BY s.indicator_id; --17

/*=================================================================================================
 Sex splits per DASR indicator
=================================================================================================*/
DROP TABLE IF EXISTS #indicator_sex_dasr;

SELECT DISTINCT
    indicator_id,
    sex_code
INTO #indicator_sex_dasr
FROM #latest_all
WHERE source_code = 1
  AND indicator_id IN (SELECT indicator_id FROM #census_dasr); --17

--SELECT indicator_id, count(sex_code) FROM #indicator_sex_dasr
--group by indicator_id
--having count(sex_code) > 1
/*=================================================================================================
Age ranges per census-required indicator from metadata for dasr
=================================================================================================*/
-- dasr indicators can have 'all ages' or specific age ranges like 65+
-- so we need to filter the pop grid by these age ranges to ensure only relevant pop is included

DROP TABLE IF EXISTS #indicator_age;

SELECT
    m.indicator_id,
    m.value_type_code,
    m.min_age_code,
    m.max_age_code
INTO #indicator_age
FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Metadata] AS m
JOIN #census_dasr AS c
  ON m.indicator_id = c.indicator_id
WHERE m.value_type_code = 4     

--SELECT * FROM #indicator_age --17
/*=================================================================================================
 Build DASR full frame: (indicator x period) × population 5-year bands
   - Uses age ranges from metadata (min_age_code / max_age_code)
   - Restricted to DASR indicators (value_type_code = 4)
=================================================================================================*/
-- This creates combinations of indicator id x start date x end date x aggregation id x imd code x age code x sex code x ethnicity code
-- And the total denominator for each combination which is used as the total population cohort for DASR

DROP TABLE IF EXISTS #pop_grid_dasr;

SELECT
    p.indicator_id,
    p.start_date,
    p.end_date,
    g.aggregation_id,
    g.imd_code,
    g.age_code,
    g.sex_code,
    g.ethnicity_code,
    g.denominator AS pop_denominator
INTO #pop_grid_dasr
FROM #periods_dasr AS p                 -- DASR census indicators only
JOIN #indicator_sex_dasr AS s           -- Limits the sex combination to one code only
  ON p.indicator_id = s.indicator_id
JOIN #indicator_age AS a                -- Ensure right age ranges are used
  ON p.indicator_id   = a.indicator_id
 AND a.value_type_code = 4             -- DASR only
JOIN #pop AS g
  ON g.sex_code = s.sex_code
 AND g.age_code BETWEEN a.min_age_code AND a.max_age_code -- restrict the 5-year bands to only those relevant for the indicator
 AND g.age_code BETWEEN 1 AND 18;      -- enforce 5-year bands for DASR --15,315,900

 --SELECT distinct age_code  FROM #pop_grid_dasr
 --where indicator_id = 10 --1-18 age codes (all ages)
 --order by age_code

 -- SELECT distinct age_code  FROM #pop_grid_dasr
 --where indicator_id = 24 -- 8-18 age codes (35+)
 --order by age_code

/*=================================================================================================
 Join DASR population grid with SQL events (to add missing 5-year bands)
=================================================================================================*/
DROP TABLE IF EXISTS #census_dasr_cohort;

SELECT
    g.indicator_id,
    g.start_date,
    g.end_date,
    COALESCE(b.numerator, 0) AS numerator,   -- fill missing numerators with 0
    g.pop_denominator,
    b.denominator AS data_denominator,
    COALESCE(b.denominator, g.pop_denominator) AS final_denominator,
    b.indicator_value,
    b.lower_ci95,
    b.upper_ci95,
    g.imd_code,
    g.aggregation_id,
    g.age_code,
    g.sex_code,
    g.ethnicity_code,
    m.creation_date,
    m.value_type_code,
    m.source_code
INTO #census_dasr_cohort
FROM #pop_grid_dasr AS g
LEFT JOIN #latest_all AS b
  ON g.indicator_id    = b.indicator_id
 AND g.start_date      = b.start_date
 AND g.end_date        = b.end_date
 AND g.aggregation_id  = b.aggregation_id
 AND g.imd_code        = b.imd_code
 AND g.age_code        = b.age_group_code
 AND g.sex_code        = b.sex_code
 AND g.ethnicity_code  = b.ethnicity_code
 AND b.source_code     = 1        -- only SQL data
 AND b.value_type_code = 4        -- only DASR
LEFT JOIN #indicator_meta_dasr AS m
  ON g.indicator_id = m.indicator_id; --15,315,900

--select top 1000 * from #census_dasr_cohort 
--where indicator_id = 10 and aggregation_id = 151 and ethnicity_code = 41
--order by 1, end_date, age_code, imd_code
                                 
-- checking duplicates
--  SELECT
--    indicator_id,
--    start_date,
--    end_date,
--    aggregation_id,
--    imd_code,
--    age_group_code,
--    sex_code,
--    ethnicity_code,
--    source_code,
--    COUNT(*) AS cnt
--FROM #latest_all
--WHERE source_code = 1
--  AND indicator_id IN (SELECT indicator_id FROM #census_dasr)
--GROUP BY
--    indicator_id,
--    start_date,
--    end_date,
--    aggregation_id,
--    imd_code,
--    age_group_code,
--    sex_code,
--    ethnicity_code,
--    source_code
--HAVING COUNT(*) > 1;

--select * from #latest_all
--where indicator_id = 10 and
--    start_date = '2020-04-01' and 
--    end_date = '2021-03-31' and
--    aggregation_id = 138 and
--    imd_code = 1 and
--    age_group_code = 3 and
--    sex_code = 999 and
--    ethnicity_code = 43 and
--    source_code =1 -- there are no duplicates
/*=================================================================================================
 Crude census indicators – use metadata age band directly (no 5-year split)
   - One row per existing SQL record (no extra age rows)
   - Denominator = census population summed over min_age_code..max_age_code
=================================================================================================*/
DROP TABLE IF EXISTS #crude_census_cohort;

select a.indicator_id,
    a.start_date,
    a.end_date,
    COALESCE(a.numerator, 0) AS numerator,   -- fill missing numerators with 0
    p.denominator AS pop_denominator,
    a.denominator AS data_denominator,
    COALESCE(a.denominator, p.denominator) AS final_denominator,
    a.indicator_value,
    a.lower_ci95,
    a.upper_ci95,
    a.imd_code,
    a.aggregation_id,
    a.age_group_code,
    a.sex_code,
    a.ethnicity_code,
    a.creation_date,
    a.value_type_code,
    a.source_code
into #crude_census_cohort
from #latest_all a
left join #pop p
on a.aggregation_id = p.aggregation_id
and a.imd_code = p.imd_code
and a.age_group_code = p.age_code
and a.sex_code = p.sex_code
and a.ethnicity_code = p.ethnicity_code
where a.source_code   = 1 -- SQL only
and indicator_id in (select indicator_id from #census_crude)
and a.denominator is null --36495

--select * from #crude_census_cohort

--SELECT
--    b.indicator_id,
--    b.start_date,
--    b.end_date,
--    COALESCE(b.numerator, 0) AS numerator,
--    pop_sum.denominator      AS final_denominator,
--    b.indicator_value,
--    b.lower_ci95,
--    b.upper_ci95,
--    b.imd_code,
--    b.aggregation_id,
--    b.age_group_code,
--    b.sex_code,
--    b.ethnicity_code,
--    b.creation_date,
--    b.value_type_code,
--    b.source_code
--INTO #crude_census_cohort
--FROM #latest_all AS b
--JOIN #indicator_age AS a
--  ON b.indicator_id    = a.indicator_id
-- AND a.value_type_code = 3              -- crude rate
--JOIN (
--    SELECT
--        aggregation_id,
--        imd_code,
--        sex_code,
--        ethnicity_code,
--        -- we keep age_code so we can group by later if needed
--        age_code,
--        denominator
--    FROM #pop
--) AS p
--  ON p.aggregation_id = b.aggregation_id
-- AND p.imd_code       = b.imd_code
-- AND p.sex_code       = b.sex_code
-- AND p.ethnicity_code = b.ethnicity_code
-- AND p.age_code BETWEEN a.min_age_code AND a.max_age_code
---- aggregate population over age range defined in metadata
--CROSS APPLY (
--    SELECT SUM(p2.denominator) AS denominator
--    FROM #pop AS p2
--    WHERE p2.aggregation_id = b.aggregation_id
--      AND p2.imd_code       = b.imd_code
--      AND p2.sex_code       = b.sex_code
--      AND p2.ethnicity_code = b.ethnicity_code
--      AND p2.age_code BETWEEN a.min_age_code AND a.max_age_code
--) AS pop_sum
--WHERE b.source_code   = 1
--  AND b.indicator_id IN (SELECT indicator_id FROM #census_crude);

/*=================================================================================================
 Combine census indicators (DASR + crude) with other data sources
=================================================================================================*/
DROP TABLE IF EXISTS #full_all;

-- DASR census indicators (5-year age bands)
SELECT
    indicator_id,
    start_date,
    end_date,
    numerator,
    final_denominator AS denominator,
    indicator_value,
    lower_ci95,
    upper_ci95,
    imd_code,
    aggregation_id,
    age_code AS age_group_code,
    sex_code,
    ethnicity_code,
    creation_date,
    value_type_code,
    source_code
INTO #full_all
FROM #census_dasr_cohort

UNION ALL

-- Crude census indicators 
SELECT
    indicator_id,
    start_date,
    end_date,
    numerator,
    final_denominator AS denominator,
    indicator_value,
    lower_ci95,
    upper_ci95,
    imd_code,
    aggregation_id,
    age_group_code,
    sex_code,
    ethnicity_code,
    creation_date,
    value_type_code,
    source_code
FROM #crude_census_cohort

UNION ALL

-- Non-SQL sources + SQL indicators that don't require census denominators
SELECT
    s.indicator_id,
    s.start_date,
    s.end_date,
    s.numerator,
    s.denominator,
    s.indicator_value,
    s.lower_ci95,
    s.upper_ci95,
    s.imd_code,
    s.aggregation_id,
    s.age_group_code,
    s.sex_code,
    s.ethnicity_code,
    s.creation_date,
    s.value_type_code,
    s.source_code
FROM #latest_all AS s
WHERE s.source_code <> 1
   OR s.indicator_id NOT IN (SELECT indicator_id FROM #require_census); --15,575,503 


--SELECT sum(numerator) as n, sum(denominator) as d  
--FROM #full_all
--WHERE aggregation_id = 151 and indicator_id = 49
--and end_date = '2025-03-31' and imd_code = 999 and ethnicity_code = 999 -- d = 1381256

--SELECT sum(numerator) as n, sum(denominator) as d  
--FROM #full_all
--WHERE aggregation_id = 151 and indicator_id = 50
--and end_date = '2025-03-31' and imd_code = 999 and ethnicity_code = 999 --1381256

--SELECT sum(numerator) as n, sum(denominator) as d  
--FROM #full_all
--WHERE aggregation_id = 151 and indicator_id = 26 
--and end_date = '2025-03-31' and imd_code = 999 and ethnicity_code = 999  --1381256
/*=================================================================================================
Delete denominator = 0 and numerator = 0 for census required indicators
=================================================================================================*/

-- Remove impossible combinations (no population and no events)
DELETE FROM #full_all
WHERE source_code = 1                           -- SQL census indicators only
  AND indicator_id IN (SELECT indicator_id FROM #require_census)
  AND denominator = 0
  AND numerator = 0; --9,754,657

--SELECT COUNT(*) FROM #full_all --5,820,846

-- DQ
--  SELECT sum(numerator) as n, sum(denominator) as d  
--FROM #full_all
--WHERE aggregation_id = 151 and indicator_id = 49
--and end_date = '2025-03-31' and imd_code = 999 and ethnicity_code = 999 -- d = 1381256

--SELECT sum(numerator) as n, sum(denominator) as d  
--FROM #full_all
--WHERE aggregation_id = 151 and indicator_id = 50
--and end_date = '2025-03-31' and imd_code = 999 and ethnicity_code = 999 --1381256

--SELECT sum(numerator) as n, sum(denominator) as d  
--FROM #full_all
--WHERE aggregation_id = 151 and indicator_id = 26 
--and end_date = '2025-03-31' and imd_code = 999 and ethnicity_code = 999  --1381256


/*=================================================================================================
 Load into staging 
=================================================================================================*/

-- Disable the FK that references the staging table to enable truncating the table 
ALTER TABLE EAT_Reporting_BSOL.Development.BSOLBI_0033_Dummy_Table
DROP CONSTRAINT FK_DummyTable6;

-- Truncate the staging table (quicker than DELETE FROM)
TRUNCATE TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data];

-- Re-enable the FK constraint to prevent table drops
ALTER TABLE EAT_Reporting_BSOL.Development.BSOLBI_0033_Dummy_Table
ADD CONSTRAINT FK_DummyTable6
FOREIGN KEY (Indicator_Staging_ID)
REFERENCES [OF].[OF2_Indicator_Staging_Data](PK_ID);

-- Insert data into staging table
INSERT INTO [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data] (
       [indicator_id]
      ,[start_date]
      ,[end_date]
      ,[numerator]
      ,[denominator]
      ,[indicator_value]
      ,[lower_ci95]
      ,[upper_ci95]
      ,[imd_code]
      ,[aggregation_id]
      ,[age_group_code]
      ,[sex_code]
      ,[ethnicity_code]
      ,[creation_date]
      ,[value_type_code]
      ,[source_code]
      ,[insertion_date_time]
)
SELECT [indicator_id]
      ,[start_date]
      ,[end_date]
      ,COALESCE(numerator, 0) AS numerator
      ,denominator
      ,[indicator_value]
      ,[lower_ci95]
      ,[upper_ci95]
      ,[imd_code]
      ,[aggregation_id]
      ,[age_group_code]
      ,[sex_code]
      ,[ethnicity_code]
      ,[creation_date]
      ,[value_type_code]
      ,[source_code]
      ,GETDATE() AS [insertion_date_time]
FROM #full_all;

