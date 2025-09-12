from fingertips_import import get_fingertips_indicators 
from datetime import datetime
import sqlalchemy as sql
import pyodbc
import pandas as pd
import time

# get area codes from db
#conn = "mssql+pyodbc://MLCSU-BI-SQL/EAT_Reporting_BSOL?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes"

#engine = sql.create_engine('mssql+pyodbc://@' + 'MLCSU-BI-SQL' + '/' + 'EAT_REPORTING_BSOL' + '?trusted_connection=Yes&driver=ODBC+Driver+17+for+SQL+Server')

#engine = sql.create_engine(conn)
 
nongp_query = """
SELECT [aggregation_id]
      ,[aggregation_type]
      ,[aggregation_code]
      ,[aggregation_label]
  FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Geography]
  Where aggregation_type not in ('Locality (resident)', 'Locality (registered)', 'Ward', 'Constituency', 'Unknown', 'All')
"""


gp_query = """
select GPPracticeCode_Original, GPPracticeName_Original from 
EAT_Reporting_BSOL.Reference.BSOL_ICS_PracticeMapped 
where
ICS_2223 = 'BSOL'
"""


#area_lookup = pd.read_sql_query(nongp_query, engine)
area_lookup = pd.read_csv("./data/orgs_lkp.csv")


area_codes = area_lookup['aggregation_code'].unique().tolist()


#gp_lookup = pd.read_sql_query(gp_query, engine)
gp_lookup = pd.read_csv("./data/gps.csv")


gp_codes = gp_lookup['GPPracticeCode_Original'].unique().tolist()

combined_codes = area_codes + gp_codes

#areas = list_area_types()

# Example indicator IDs
#ids = [90362, 11001, "bad_id"]

# Fist line is CSU tables from v1, second and third is BCC pipeline from v1
of_25 = [811, 20101, 20401, 30307, 41203, 90619, 91340, 91743, 92254, 92517, 92601,
92781, 93085, 93088, 93183, 93605, 93675, 91041, 22001, 90631, 93438, 93454, 93790]

# LA

# GP

# PCN

# ICB





# 7 GP, 204 PCN, 221 ICB
area_ids = [7, 204, 221]

# Fetch data
start_time = time.time()
data = get_fingertips_indicators(of_25, area_codes=combined_codes)
end_time = time.time()

print(f"Download took {end_time - start_time:.2f} seconds")

# INdex file with date for test
today_date = datetime.now().strftime('%Y%m%d')

data.to_csv(f'./data/test_data_{today_date}.csv', index=False)