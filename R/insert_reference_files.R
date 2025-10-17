################################################################################
# This script writes reference files to the respective SQL tables
################################################################################

library(tidyverse)
library(DBI)
library(odbc)
library(readxl)
library(janitor)
library(IMD)
library(PHEindicatormethods)

#1. The path to the parent work directory ----------------------------------------
directory_path = "//mlcsu-bi-fs/csugroupdata$/Commissioning Intelligence And Strategy/BSOLCCG/Reports/02_Routine/BSOLBI_0033_Outcome_Framework_Rebuild"

#2. Path to reference file
reference_file_path <- file.path(directory_path, "Reference", "Lookups.xlsx")

# Read specific sheets into separate variables ---------------------------------
sex_lookup               <- read_xlsx(reference_file_path, sheet = "sex")
age_lookup               <- read_xlsx(reference_file_path, sheet = "age")
imd_lookup               <- read_xlsx(reference_file_path, sheet = "imd")
ethnicity_lookup         <- read_xlsx(reference_file_path, sheet = "ethnicity")
polarity_lookup          <- read_xlsx(reference_file_path, sheet = "polarity")
valuetype_lookup         <- read_xlsx(reference_file_path, sheet = "value_type")
source_lookup            <- read_xlsx(reference_file_path, sheet = "source")
domain_lookup            <- read_xlsx(reference_file_path, sheet = "domain")
status_lookup            <- read_xlsx(reference_file_path, sheet = "status")
indicator_list_lookup    <- read_xlsx(reference_file_path, sheet = "indicator_list")
geography_lookup         <- read_xlsx(reference_file_path, sheet = "geography")

# Initial population table

# Using Census
population_reference_path <- file.path(directory_path, "Reference", "census_ethnicity_imd_age_full_population.csv")

population_reference_file <- read_csv(population_reference_path)

population_reference_file <- clean_names(population_reference_file, case = "snake")

# Metadata

metadata <- read_xlsx("data/metadata/metadata.xlsx")

metadata <- janitor::clean_names(metadata, case = "snake") |>
  select(-in_destination_table_internal_use_only)

#3. Establish SQL connection -----------------------------------------------------
sql_connection <-
  dbConnect(
    odbc(),
    Driver = "SQL Server",
    Server = "MLCSU-BI-SQL",
    Database = "EAT_Reporting_BSOL",
    Trusted_Connection = "True"
  )

#4. Write tables -----------------------------------------------------------------
#1 Sex -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Sex"),
  sex_lookup,
  overwrite = TRUE
)

#2 Age -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Age_Group"),
  age_lookup,
  overwrite = TRUE
)

#3 IMD -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_IMD"),
  imd_lookup,
  overwrite = TRUE
)

#4 Ethnicity -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Ethnicity"),
  ethnicity_lookup,
  overwrite = TRUE
)

#5 Polarity -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Polarity"),
  polarity_lookup,
  overwrite = TRUE
)

#6 Value Type -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Value_Type"),
  valuetype_lookup,
  overwrite = TRUE
)

#7 Source -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Source"),
  source_lookup,
  overwrite = TRUE
)

#8 Geography -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Geography"),
  geography_lookup,
  overwrite = TRUE
)

#9 Domain -------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Domain"),
  domain_lookup,
  overwrite = TRUE
)

#10 Status ------------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Status"),
  status_lookup,
  overwrite = TRUE
)

#11 Indicator list ---------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Indicator_List"),
  indicator_list_lookup,
  overwrite = TRUE
)

#12 Ward to IMD lookup -----------

ward_imd_lookup <- IMD::imd_england_ward %>%
  phe_quantile(Score, nquantiles = 5L, invert = TRUE) %>%
  select(ward_code, Score, quantile) %>%
  rename(score = Score, quintile = quantile)

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Ward_To_IMD"),
  ward_imd_lookup,
  overwrite = TRUE
)

#13. Census population table -----------

dbWriteTable(
  sql_connection,
  Id(schema = "OF", table = "OF2_Reference_Initial_Population"),
  population_reference_file,
  overwrite = TRUE
)

#14. Metadata -------------


dbWriteTable(sql_connection,
             name = Id(schema = "OF", table = "OF2_Reference_Metadata"),
             value = metadata,
             overwrite = TRUE)
