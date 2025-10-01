library(tidyverse)
library(lubridate)
library(readxl)
library(DBI)
library(odbc)
library(PHEindicatormethods)
library(tibble)

# Start timer
run_start <- Sys.time()

# 1 Read metadata --------------------------------------------------------------
metadata <- read_xlsx("data/metadata/metadata.xlsx")

# 2) Database connection -------------------------------------------------------
conn <- dbConnect(
  odbc(),
  Driver   = "SQL Server",
  Server   = "MLCSU-BI-SQL",
  Database = "EAT_Reporting_BSOL",
  Trusted_Connection = "True"
)

# 3) Define a runner that sources functions and executes the ETL ---------------
run_all <- function(conn, metadata) {

  # Source function files
  source(file.path("R/utils.R")) # define calculate_dsr3
  source(file.path("R/etl.R")) # define calculate_values, check_row_counts

  # Process SharePoint data
  message("▶ Processing Sharepoint data ...")
  source(file.path("R/insert_sharepoint_data.R"))

  # Combine latest data from multiple sources
  message("▶ Combining latest data from multiple sources in SQL ...")
  dbWithTransaction(conn, {
    run_sql_file(conn, "SQL/03_insert_into_staging_table.sql")
  })

  # Pull fresh staging data
  message("▶ Extracting staging data from SQL table  ...")
  staging_data <- dbGetQuery(
    conn,
    "SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]"
  )

  # Run ETL
  message("▶ Rrocessing indicator data ...")
  result <- calculate_values(data = staging_data, metadata = metadata, metadata_key = "indicator_id")

  return(list(
    result = result,
    staging_data = staging_data
  ))

}

# 4) Execute and capture output -------------------------------------------------
output <- run_all(conn = conn, metadata = metadata)

# 5) Run DQ checks -------------------------------------------------------------
staging_data <- output$staging_data|>
  mutate(time_period_type = get_duration_label(as.Date(start_date), as.Date(end_date)))

run_all_dq_checks(
  df = output$result|>
    filter(time_period_type == "1 year" & value_type_code %in% c(2, 3, 7)),
  reference_data = staging_data |>
    filter(time_period_type == "1 year" & value_type_code %in% c(2, 3, 7)),
  metadata = metadata,
  cols_to_check = NULL,  # or c("col1","col2",...)
  show_n = 10
)

# 6) Add insertion time stamp and standardise schema ---------------------------
result <- output$result |>
  mutate(insertion_date_time = Sys.time()) |>
  mutate(
    indicator_id     = as.integer(indicator_id),
    start_date       = as.Date(start_date),
    end_date         = as.Date(end_date),
    numerator        = as.numeric(numerator),
    denominator      = as.numeric(denominator),
    indicator_value  = as.numeric(indicator_value),
    lower_ci95       = as.numeric(lower_ci95),
    upper_ci95       = as.numeric(upper_ci95),
    imd_code         = as.integer(imd_code),
    aggregation_id   = as.integer(aggregation_id),
    age_group_code   = as.integer(age_group_code),
    sex_code         = as.integer(sex_code),
    ethnicity_code   = as.integer(ethnicity_code),
    creation_date    = as.POSIXct(creation_date),
    value_type_code  = as.integer(value_type_code),
    source_code      = as.integer(source_code),
    time_period_type = as.character(time_period_type),
    combination_id   = as.integer(combination_id)
  )


# 7) Write output into database ------------------------------------------------

dbExecute(conn, "DELETE FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Processed_Data]")
dbWriteTable(conn,
             name = Id(schema = "OF", table = "OF2_Indicator_Processed_Data"),
             value = result,
             append = TRUE)

# End timer
run_end <- Sys.time()
total_mins <- as.numeric(difftime(run_end, run_start, units = "mins"))
message(sprintf("⏱ Total run time: %.2f min", total_mins))
