library(tidyverse)
library(readxl)
library(DBI)
library(odbc)
library(PHEindicatormethods)
library(tibble)

# 1 Read metadata --------------------------------------------------------------
metadata <- read_xlsx("data/metadata.xlsx")

# 2) Database connection -------------------------------------------------------
sql_connection <- dbConnect(
  odbc(),
  Driver   = "SQL Server",
  Server   = "MLCSU-BI-SQL",
  Database = "EAT_Reporting_BSOL",
  Trusted_Connection = "True"
)

# 3) Pull staging data ---------------------------------------------------------
staging_data <- dbGetQuery(
  sql_connection,
  "SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]"
)

# 4) Define a runner that sources functions and executes the ETL ----
run_all <- function(staging_data, metadata = NULL, use_metadata = TRUE) {

  # Run insert_sharepoint_data.R
  source(file.path("R/insert_sharepoint_data.R"))

  # Source function files
  source(file.path("R/util.R")) # define calculate_dsr3
  source(file.path("R/etl.R")) # define calculate_values, check_row_counts

  # Decide whether to pass metadata or not (calculate_values handles NULL)
  md <- if (use_metadata) metadata else NULL

  # Run ETL
  message("▶ Running calculate_values() ...")
  result <- calculate_values(staging_data = staging_data, metadata = md)

  # Row-count check against original input
  message("▶ Verifying row counts ...")
  check_row_counts(input_data = result, reference_data = staging_data)

  return(result)
}

# 5) Execute and capture output -------------------------------------------------
output <- run_all(staging_data = staging_data, metadata = metadata, use_metadata = TRUE)


# Add insertion time stamp and make sure data types are consistent
output <- output |>
  filter(status_code == 1) |>
  mutate(insertion_date_time = Sys.time()) |>
  mutate(
    indicator_id    = as.integer(indicator_id),
    start_date      = as.Date(start_date),
    end_date        = as.Date(end_date),
    numerator       = as.numeric(numerator),
    denominator     = as.numeric(denominator),
    indicator_value = as.numeric(indicator_value),
    lower_ci95      = as.numeric(lower_ci95),
    upper_ci95      = as.numeric(upper_ci95),
    imd_code        = as.integer(imd_code),
    aggregation_id  = as.integer(aggregation_id),
    age_group_code  = as.integer(age_group_code),
    sex_code        = as.integer(sex_code),
    ethnicity_code  = as.integer(ethnicity_code),
    creation_date   = as.Date(creation_date),
    value_type_code = as.integer(value_type_code),
    source_code     = as.integer(source_code)
  )


# 6) Write output into database ------------------------------------------------

dbWriteTable(sql_connection,
             name = Id(schema = "OF", table = "OF2_Indicator_Processed_Data"),
             value = output,
             append = TRUE)

