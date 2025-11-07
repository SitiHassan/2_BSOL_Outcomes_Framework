--script to clean and add ids to the different fields for the population table 


--select * from EAT_Reporting_BSOL.[OF].[OF2_Reference_IMD]
--select * from EAT_Reporting_BSOL.[OF].[OF2_Reference_Ethnicity]
--select* from EAT_Reporting_BSOL.[OF].[OF2_Reference_Sex]
--select * from EAT_Reporting_BSOL.[OF].[OF2_Reference_Age_Group]

 --SELECT *  FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Group]

drop table if exists [EAT_Reporting_BSOL].[OF].[OF2_Reference_Population]


--drop table #aa
USE EAT_Reporting_BSOL
select distinct 
				a1.aggregation_id,
				a1.geography_hierachy,
				a1.aggregation_type,
				a1.icb, a1.la_code,
				a1.la_name,
				a1.locality,
				a1.ward_pcn_code,
				a1.ward_pcn_name,
				a1.aggregation_name,
				a1.imd,
				case when a1.imd='ALL' then '999'
				else a2.imd_code end as imd_code,
				--a2.imd_code,
				--a2.imd_quintile,
				--cast(a1.imd as varchar(1)),
				a1.age_group,
				a1.start_age,
				a1.end_age,
				case when a1.age_group='ALL' then '999'
				else a5.age_code end as age_code,
				--a5.age_code,
				--a1.nhs_code,
				a1.nhs_code_definitions,
				case when a1.nhs_code='ALL' then '999'
				else a3.ethnicity_code end as ethnicity_code,
				--a3.ethnicity_code,
				a1.sex,
				--a1.sex_code,
				case when  a1.sex_code=3 then '999'
				else a4.sex_code end as sex_code,
				--a4.sex_code,
				a1.observation
into [EAT_Reporting_BSOL].[OF].[OF2_Reference_Population]
from  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Initial_Population] a1
left join [OF].[OF2_Reference_IMD] a2 on cast(a1.imd as varchar(1))=a2.imd_quintile
left join [OF].[OF2_Reference_Ethnicity] a3 on  a1.nhs_code=a3.nhs_code 
left join [OF].[OF2_Reference_Sex] a4 on a1.sex=a4.sex
left join [OF].[OF2_Reference_Age_Group] a5 on a1.start_age=a5.min_age and a1.end_age=a5.max_age

--testing
--see if there is some that don't match
select distinct  a.start_date, a.numerator
,a.aggregation_id, b.aggregation_name, a.age_group_code,b.age_group, a.sex_code,b.sex
,a.ethnicity_code ,b.observation
from [EAT_Reporting_BSOL].[OF].OF2_Indicator_SQL_Data a
left join [EAT_Reporting_BSOL].[OF].[OF2_Reference_Population] b on a.aggregation_id=b.aggregation_id

AND a.age_group_code = b.age_code
	AND a.ethnicity_code = b.ethnicity_code
	AND a.imd_code = b.imd_code
	AND a.sex_code = b.sex_code
where indicator_id =87 and observation is null 
and a.ethnicity_code not in ('999','-99','17')


--add population_id
ALTER TABLE[EAT_Reporting_BSOL].[OF].[OF2_Reference_Population]
Add population_id Int Identity(1, 1)


select top 10000* from [EAT_Reporting_BSOL].[OF].[OF2_Reference_Population]
order by population_id