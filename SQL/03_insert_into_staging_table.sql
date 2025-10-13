/*=================================================================================================
 This script combines data from SQL, API, SharePoint, and Other tables into a Staging data table
=================================================================================================*/

-- Update the denominator column in the SQL data table first
DROP TABLE IF EXISTS #sql_data
SELECT * INTO #sql_data
FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_SQL_Data] 

UPDATE T1
SET T1.denominator = T2.observation
FROM #sql_data T1
LEFT JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Population] T2
	ON T1.aggregation_id = T2.aggregation_id
	AND T1.age_group_code = T2.age_code
	AND T1.ethnicity_code = T2.ethnicity_code
	AND T1.imd_code = T2.imd_code
	AND T1.sex_code = T2.sex_code
WHERE T1.denominator IS NULL 

--select distinct indicator_id from #sql_data where denominator is null and ethnicity_code not in (-99, 17, 999)

-- All data
DROP TABLE IF EXISTS #all
SELECT *
INTO #all
FROM (
    SELECT * FROM #sql_data
    UNION ALL
    SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_API_Data]
    UNION ALL
    SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Sharepoint_Data]
    UNION ALL
    SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Other_Data]
) AS t; 

-- Max creation date per indicator id and source_code
DROP TABLE IF EXISTS #max_date
SELECT
  indicator_id,
  source_code,
  MAX(creation_date) AS creation_date
INTO #max_date
FROM #all
GROUP BY indicator_id, source_code; 

-- Latest data
DROP TABLE IF EXISTS #latest_data
SELECT a.*
INTO #latest_data
FROM #all AS a
JOIN #max_date AS b
  ON a.indicator_id  = b.indicator_id
 AND a.source_code   = b.source_code
 AND a.creation_date = b.creation_date;

-- Load staging
DELETE FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]

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
	  , GETDATE() AS [insertion_date_time]
FROM #latest_data

--SELECT DISTINCT indicator_id FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data] 
--ORDER BY 1
