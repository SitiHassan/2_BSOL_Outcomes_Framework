library(tidyverse)
library(lubridate)
library(readxl)
library(DBI)
library(odbc)
library(PHEindicatormethods)
library(tibble)

# Start timer
run_start <- Sys.time()


# 1) Database connection -------------------------------------------------------
conn <- dbConnect(
  odbc(),
  Driver   = "SQL Server",
  Server   = "MLCSU-BI-SQL",
  Database = "EAT_Reporting_BSOL",
  Trusted_Connection = "True"
)

# 2) Read metadata -------------------------------------------------------------
metadata <- dbGetQuery(conn, "SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Metadata]")


# 3) Define a runner that sources functions and executes the ETL ---------------
run_all <- function(conn, metadata, indicator_ids = "All", table_name) {
  # Source function files
  source(file.path("R/utils.R"))
  source(file.path("R/etl.R"))

  # Process SharePoint data
  message("▶ Processing Sharepoint data ...")
  source(file.path("R/insert_sharepoint_data.R"))

  # Combine latest data
  message("▶ Combining latest data from multiple sources in SQL ...")
  dbWithTransaction(conn, {
    run_sql_file(conn, "SQL/03_insert_into_staging_table.sql")
  })

  #  Normalize indicator_ids
  ids <- normalize_indicator_ids(indicator_ids)

  # Pull fresh staging data (optionally filtered)
  if (is.null(ids) || length(ids) == 0) {
    message("▶ Extracting ALL indicators from staging table ...")
  } else {
    message(sprintf("▶ Extracting %d indicator(s) from staging table ...", length(ids)))
  }

  staging_data <- get_indicators_from_sql(
    conn         = conn,
    table_name   = table_name,
    indicator_ids = ids
  )

  # Run ETL
  message("▶ Processing indicator data ...")
  result <- calculate_values(
    data = staging_data,
    metadata = metadata,
    metadata_key = "indicator_id"
  )

  list(
    result = result,
    staging_data = staging_data
  )
}


# 4) Execute and capture output -------------------------------------------------
output <- run_all(conn = conn,
                  metadata = metadata,
                  indicator_ids = "All", # or a vector of numeric/char ids or single comma-separated string like "10, 11, 12"
                  table_name = "[EAT_Reporting_BSOL].[OF].[OF2_Indicator_Staging_Data]")


# 5) Run DQ checks -------------------------------------------------------------
staging_data <- output$staging_data|>
  mutate(time_period_type = get_duration_label(as.Date(start_date), as.Date(end_date)))

run_all_dq_checks(
  df = output$result$combined_calc_dfs|>
    filter(time_period_type == "1 year" & value_type_code %in% c(2, 3, 7)),
  reference_data = staging_data |>
    filter(time_period_type == "1 year" & value_type_code %in% c(2, 3, 7)),
  metadata = metadata,
  cols_to_check = NULL,  # or c("col1","col2",...)
  show_n = 10
)

# 6) Add insertion time stamp and standardise schema ---------------------------
result <- output$result$combined_calc_dfs |>
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
insert_data_into_sql_table(
  conn,
  database = "EAT_Reporting_BSOL",
  schema   = "OF",
  table    = "OF2_Indicator_Processed_Data",
  data     = result,
  indicator_ids = "All",
  id_column = "indicator_id"
)


# End timer
run_end <- Sys.time()
total_mins <- as.numeric(difftime(run_end, run_start, units = "mins"))
message(sprintf("⏱ Total run time: %.2f min", total_mins))
