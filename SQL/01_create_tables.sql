  	
/*=================================================================================================
  Table Creations			
=================================================================================================*/	

-- Raw data tables

  -- Table 1

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Indicator_SQL_Data] (
	
         [indicator_id] INT
	   , [start_date] DATE
	   , [end_date] DATE
	   , [numerator] FLOAT
	   , [denominator] FLOAT
	   , [indicator_value] FLOAT
	   , [lower_ci95] FLOAT
	   , [upper_ci95] FLOAT
	   , [imd_code] INT
	   , [aggregation_id] INT
	   , [age_group_code] INT
	   , [sex_code] INT
	   , [ethnicity_code] INT
	   , [creation_date] DATETIME
	   , [value_type_code] INT
	   , [source_code] INT
	   )

;


  -- Table 2

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Indicator_API_Data] (
	
         [indicator_id] INT
	   , [start_date] DATE
	   , [end_date] DATE
	   , [numerator] FLOAT
	   , [denominator] FLOAT
	   , [indicator_value] FLOAT
	   , [lower_ci95] FLOAT
	   , [upper_ci95] FLOAT
	   , [imd_code] INT
	   , [aggregation_id] INT
	   , [age_group_code] INT
	   , [sex_code] INT
	   , [ethnicity_code] INT
	   , [creation_date] DATETIME
	   , [value_type_code] INT
	   , [source_code] INT
	   )


  -- Table 3

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Sharepoint_Data] (
	
         [indicator_id] INT
	   , [start_date] DATE
	   , [end_date] DATE
	   , [numerator] FLOAT
	   , [denominator] FLOAT
	   , [indicator_value] FLOAT
	   , [lower_ci95] FLOAT
	   , [upper_ci95] FLOAT
	   , [imd_code] INT
	   , [aggregation_id] INT
	   , [age_group_code] INT
	   , [sex_code] INT
	   , [ethnicity_code] INT
	   , [creation_date] DATETIME
	   , [value_type_code] INT
	   , [source_code] INT
	   )

  -- Table 4

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Other_Data] (
	
         [indicator_id] INT
	   , [start_date] DATE
	   , [end_date] DATE
	   , [numerator] FLOAT
	   , [denominator] FLOAT
	   , [indicator_value] FLOAT
	   , [lower_ci95] FLOAT
	   , [upper_ci95] FLOAT
	   , [imd_code] INT
	   , [aggregation_id] INT
	   , [age_group_code] INT
	   , [sex_code] INT
	   , [ethnicity_code] INT
	   , [creation_date] DATETIME
	   , [value_type_code] INT
	   , [source_code] INT
	   )

-- Staging table

  -- Table 5
          
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data] (
	
         [indicator_id] INT
	   , [start_date] DATE
	   , [end_date] DATE
	   , [numerator] FLOAT
	   , [denominator] FLOAT
	   , [indicator_value] FLOAT
	   , [lower_ci95] FLOAT
	   , [upper_ci95] FLOAT
	   , [imd_code] INT
	   , [aggregation_id] INT
	   , [age_group_code] INT
	   , [sex_code] INT
	   , [ethnicity_code] INT
	   , [creation_date] DATETIME
	   , [value_type_code] INT
	   , [source_code] INT
	   , [insertion_date_time] DATETIME
	   )


--- Final processed indicator data table
  -- Table 6
     CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Processed_Data] (
         [indicator_id] INT
	   , [start_date] DATE
	   , [end_date] DATE
	   , [numerator] FLOAT
	   , [denominator] FLOAT
	   , [indicator_value] FLOAT
	   , [lower_ci95] FLOAT
	   , [upper_ci95] FLOAT
	   , [imd_code] INT
	   , [aggregation_id] INT
	   , [age_group_code] INT
	   , [sex_code] INT
	   , [ethnicity_code] INT
	   , [creation_date] DATETIME
	   , [value_type_code] INT
	   , [source_code] INT
	   , [insertion_date_time] DATETIME
	   , [time_period_type] VARCHAR(50)
	   , [combination_id] INT
	   )

-- Reference tables

	-- Sex

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Sex] (
         [sex_code] INT
	   , [sex] VARCHAR(10)
	   )

    -- Age Group
		
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Group] (
         [age_code] INT
	   , [age_type] VARCHAR(100)
	   , [min_age] INT
	   , [max_age] INT
	   , [age_group] VARCHAR(100)
	   , [age_unit] VARCHAR(10)
	   )

	      
	-- IMD
	
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_IMD] (
         [imd_code] INT
	   , [imd_quintile] VARCHAR(10)
	   , [imd_quintile_desc] VARCHAR(25)
	   )


	-- Ethnicity

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Ethnicity] (
         [ethnicity_code] INT
	   , [nhs_code] VARCHAR(10)
	   , [nhs_code_definitions] VARCHAR(100)
	   , [census_ethnic_group] VARCHAR(10)
	   , [definitions] VARCHAR(100)
	   , [ethnicity_code_main] INT
	   , [main5_code] VARCHAR(10)
	   , [main5] VARCHAR(100)
	   , [cvd_prevent_grouping] VARCHAR(50)
	   , [ethnicity_code_OF] INT
	   , [OF_code] VARCHAR(10)
	   , [OF_grouping11] VARCHAR(50)
	   
	   )

	-- Value Type
	
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Value_Type] (
         [value_type_code] INT
	   , [value_type] VARCHAR(100)
	   )


	-- Source
	
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Source] (
         [source_code] INT
	   , [source] VARCHAR(12)
	   )


	-- Polarity
	
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Polarity] (
         [polarity_code] INT
	   , [polarity] VARCHAR(10)
	   , [polarity_desc] VARCHAR(50)
	   )

	---- Geography
	
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Geography] (
		 [geography_id] INT
	   , [aggregation_type] VARCHAR(50)
       , [aggregation_code] VARCHAR(10)
	   , [aggregation_label] VARCHAR(100)
	   )

	
	---- Domain
	
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Domain] (
		 [domain_code] INT
       , [domain_label] VARCHAR(25)
	   )

	---- Status

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Status] (
		 [status_code] INT
       , [domain_label] VARCHAR(25)
	   )

	--- Combination
   CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Combination] (
		 [combination_id] INT
       , [split_description] VARCHAR(100)
	   )

	  	---- Indicator list

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Indicator_List] (
		 [indicator_id] INT
       , [domain_code] INT
	   , [reference_id] VARCHAR(25)
	   , [icb_indicator_title] VARCHAR(250)
	   , [indicator_label] VARCHAR(250)
	   , [status_code] INT
	   , [old_label] VARCHAR(250)
	   , [new_label] VARCHAR(250)
	   )


	-- Ward to IMD lookup
     CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Ward_To_IMD] (
		 [ward_code] VARCHAR(30)
       , [score] NUMERIC
	   , [quintile] INT
	   )

	--- Initial population table using Census
     CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Initial_Population] (
		 [population_id] INT
       , [aggregation_id] INT
	   , [geography_hierarchy] VARCHAR(50)
	   , [aggregation_type] VARCHAR(50)
	   , [icb] VARCHAR(25)
	   , [la_code] VARCHAR(50)
	   , [la_name] VARCHAR(50)
	   , [locality] VARCHAR(50)
	   , [ward_pcn_code] VARCHAR(50)
	   , [ward_pcn_name] VARCHAR(250)
	   , [aggregation_name] VARCHAR(250)
	   , [imd] VARCHAR(10)
	   , [age_group] VARCHAR(50)
	   , [start_age] INT
	   , [end_age] INT
	   , [nhs_code] VARCHAR(10)
	   , [nhs_code_definitions] VARCHAR(100)
	   , [sex_code] INT
	   , [sex] VARCHAR(10)
	   , [observation] INT
	   )


	  
	  	--- Geography table with hierarchies
     CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Geography_Translation] (
         [aggregation_id] INT
	   , [geography_hierarchy] VARCHAR(50)
	   , [aggregation_type] VARCHAR(50)
	   , [icb] VARCHAR(25)
	   , [parent_code] VARCHAR(50)
	   , [parent_name] VARCHAR(50)
	   , [locality] VARCHAR(50)
	   , [area_code] VARCHAR(50)
	   , [area_name] VARCHAR(250)
	   , [aggregation_name] VARCHAR(250)
	   )




 -- Adding in Primary Key fields to each table which will link to a dummy table as foreign keys to prevent tables being dropped

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Sharepoint_Data]
   ADD PK_ID INT IDENTITY (1,1);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Sharepoint_Data]
   ADD CONSTRAINT PK_Dummy PRIMARY KEY (PK_ID);


   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_SQL_Data]
   ADD PK_ID INT IDENTITY (1,1);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_SQL_Data]
   ADD CONSTRAINT PK_Dummy_SQL PRIMARY KEY (PK_ID);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_API_Data]
   ADD PK_ID INT IDENTITY (1,1);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_API_Data]
   ADD CONSTRAINT PK_Dummy_API PRIMARY KEY (PK_ID);


   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Other_Data]
   ADD PK_ID INT IDENTITY (1,1);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Other_Data]
   ADD CONSTRAINT PK_Dummy_Other PRIMARY KEY (PK_ID);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Processed_Data]
   ADD PK_ID INT IDENTITY (1,1);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Processed_Data]
   ADD CONSTRAINT PK_Dummy_Processed PRIMARY KEY (PK_ID);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]
   ADD PK_ID INT IDENTITY (1,1);

   ALTER TABLE [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]
   ADD CONSTRAINT PK_Dummy_Staging PRIMARY KEY (PK_ID);


 -- Dummy Table which references these Primary Keys as Foreign Keys

 CREATE TABLE EAT_Reporting_BSOL.Development.BSOLBI_0033_Dummy_Table (
    Dummy_ID INT IDENTITY (1,1) PRIMARY KEY,
	Sharepoint_ID INT NOT NULL,
	SQL_ID INT NOT NULL,
	API_ID INT NOT NULL,
	Indicator_Other_ID INT NOT NULL,
	Indicator_Processed_ID INT NOT NULL,
	Indicator_Staging_ID INT NOT NULL

	CONSTRAINT FK_DummyTable1
	FOREIGN KEY (Sharepoint_ID)
	REFERENCES [OF].[OF2_Indicator_Sharepoint_Data](PK_ID), 

	CONSTRAINT FK_DummyTable2
	FOREIGN KEY (SQL_ID) 
	REFERENCES [OF].[OF2_Indicator_SQL_Data](PK_ID),

	CONSTRAINT FK_DummyTable3
	FOREIGN KEY (API_ID) 
	REFERENCES [OF].[OF2_Indicator_API_Data](PK_ID),

	CONSTRAINT FK_DummyTable4
	FOREIGN KEY (Indicator_Other_ID)
	REFERENCES [OF].[OF2_Indicator_Other_Data](PK_ID),

	CONSTRAINT FK_DummyTable5
	FOREIGN KEY (Indicator_Processed_ID)
	REFERENCES [OF].[OF2_Indicator_Processed_Data](PK_ID),

	CONSTRAINT FK_DummyTable6
	FOREIGN KEY (Indicator_Staging_ID)
	REFERENCES [OF].[OF2_Indicator_Staging_Data](PK_ID)
	);
	
