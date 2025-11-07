/*=================================================================================================
 Siti Hassan
 Last updated: 2025-11-07
 Purpose(s):
 1- To create age metadata per indicator
 such as any age ranges (min and max age codes) for directly age standardised indicators (dasr) and single age codes
 for non dasr indicators

 - this will be used to filter the census population needed per dasr indicator 
=================================================================================================*/



DROP TABLE IF EXISTS #metadata_age;

SELECT 
      a.indicator_id
    , b.age_group_label
    , b.age_code AS single_age_code
    , b.min_age
    , b.max_age
    , CAST(NULL AS INT) AS min_age_code
    , CAST(NULL AS INT) AS max_age_code
    , a.value_type
    , CAST(NULL AS INT) AS value_type_code
    , a.status_code
INTO #metadata_age
FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Metadata] a
LEFT JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Group] b
    ON a.age = b.age_group_label
WHERE indicator_id IS NOT NULL
GROUP BY 
      a.indicator_id
    , age
    , b.age_group_label
    , b.age_code
    , b.min_age
    , b.max_age
    , a.value_type
    , a.status_code;

-- Update value type code
UPDATE a
SET a.value_type_code = b.value_type_code
FROM #metadata_age a
LEFT JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Value_Type] b
    ON a.value_type = b.value_type;

-- All ages dasr (1-18)
UPDATE #metadata_age
SET min_age_code = 1, 
    max_age_code = 18 -- Full 1-18 5-year age bands
WHERE single_age_code = 999 
  AND value_type_code = 4; -- All Ages DASR

-- Update the min age code for dasr 
UPDATE a
SET min_age_code = b.age_code
FROM #metadata_age a
LEFT JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Group] b
    ON a.min_age = b.min_age -- Join on min age
WHERE a.single_age_code <> 999 
  AND b.age_type = '5-year age group'
  AND value_type_code = 4;

-- Update the max age for dasr (18)
UPDATE a
SET max_age_code = 18
FROM #metadata_age a
WHERE a.max_age = 120 
  AND a.single_age_code <> 999 
  AND value_type_code = 4;

-- Update the other max age
UPDATE a
SET max_age_code = b.age_code
FROM #metadata_age a
LEFT JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Group] b
    ON a.max_age = b.max_age -- Join on max age
WHERE a.single_age_code <> 999 
  AND b.age_type = '5-year age group'
  AND value_type_code = 4;

-- Update the age for crude, same as age code column as no 5 year age bands required
UPDATE #metadata_age
SET min_age_code = single_age_code,
    max_age_code = single_age_code
WHERE value_type_code <> 4;


--CREATE TABLE [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Metadata] (
--      indicator_id    INT
--    , age_group_label VARCHAR(50)
--    , single_age_code INT
--    , min_age         INT
--    , max_age         INT
--    , min_age_code    INT
--    , max_age_code    INT
--    , value_type      VARCHAR(50)
--    , value_type_code INT
--    , status_code     INT
--);
TRUNCATE TABLE [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Metadata]

INSERT INTO [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Metadata] (
      indicator_id
    , age_group_label
    , single_age_code
    , min_age
    , max_age
    , min_age_code
    , max_age_code
    , value_type
    , value_type_code
    , status_code
)
SELECT 
      indicator_id
    , age_group_label
    , single_age_code
    , min_age
    , max_age
    , min_age_code
    , max_age_code
    , value_type
    , value_type_code
    , status_code
FROM #metadata_age;


--SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Metadata] 
