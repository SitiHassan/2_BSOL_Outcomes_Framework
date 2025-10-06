library(tidyverse)
library(lubridate)
library(PHEindicatormethods)

# Function to check which indicators requiring pooled data ---------------------
require_pooled_data <- function(metadata) {
  # Inputs:
  #   metadata   - a dataframe containing at least column `denominator_source` and `indicator_id`
  # Output:
  #   A vector of indicator ids requiring pooled data
  ids <- metadata |>
    filter(str_detect(tolower(denominator_source), "census")) |>
    distinct(indicator_id) |>
    pull(indicator_id)

  return(ids)
}

# Keys to pool data by
POOL_KEYS <- c(
  "indicator_id","imd_code","aggregation_id",
  "age_group_code","sex_code","ethnicity_code",
  "creation_date","value_type_code","source_code","combination_id"
)

time_period_3yrs <- data.frame(
  from = c(2014,2015,2016,2017,2018,2019,2020,2021,2022),
  to   = c(2016,2017,2018,2019,2020,2021,2022,2023,2024)
)

time_period_5yrs <- data.frame(
  from = c(2014,2015,2016,2017,2018,2019,2020),
  to   = c(2018,2019,2020,2021,2022,2023,2024)
)

# Function to create pooled data -----------------------------------------------
# Inputs:
#   df  - data frame containing at least POOL_KEYS, `start_date`, `numerator`,
#         `denominator`, `year_type` ("Calendar"/"Financial"), and `population_type`
#   ks  - integer vector of window sizes to pool over (e.g., c(3, 5))
#   time_period_3yrs (optional) - data frame with columns `from`, `to` defining
#         explicit 3-year windows; if NULL, windows are auto-generated from data
#   time_period_5yrs (optional) - data frame with columns `from`, `to` defining
#         explicit 5-year windows; if NULL, windows are auto-generated from data
#
# Output:
#   A data frame containing only the 3- and/or 5-year pooled rows (no yearly rows),
#   where `numerator` and `denominator` are summed over each window, dates are set
#   from `from`/`to` according to `year_type`, and `time_period_type` is
#   "3 year pooled" or "5 year pooled".
#
# Notes/Assumptions:
#   - Only rows with `population_type == "Census"` are considered.
#   - One yearly record per POOL_KEYS × year is constructed internally before pooling.
#   - If custom window tables are provided, they must have integer `from`/`to` years.


create_pooled_with_ranges <- function(df, ks = c(3,5), time_period_3yrs = NULL, time_period_5yrs = NULL) {

  # Select only indicators which use Census as denominator source
  df <- df |> filter(population_type == "Census")

  # Group data by POOL KEYS and year
  yearly <- df |>
    mutate(period_year = lubridate::year(as.Date(start_date))) |>
    filter(year_type %in% c("Calendar","Financial"),
           !is.na(period_year)) |>
    group_by(across(all_of(POOL_KEYS)), year_type, period_year, ) |>
    summarise(
      numerator   = sum(numerator,   na.rm = TRUE),
      denominator = sum(denominator, na.rm = TRUE),
      .groups = "drop"
    )

  if (nrow(yearly) == 0) return(slice(df, 0)) # empty in, empty out

  # Build range windows (use data span if not provided)
  miny <- min(yearly$period_year, na.rm = TRUE)
  maxy <- max(yearly$period_year, na.rm = TRUE)

  ranges_list <- list()
  if (3 %in% ks) {
    rng3 <- if (is.null(time_period_3yrs)) {
      # auto-generate from data
      tibble::tibble(from = seq.int(miny, maxy - 2), to = from + 2, k = 3L)
    } else {
      time_period_3yrs |>  transmute(from = as.integer(from), to = as.integer(to), k = 3L)
    }
    ranges_list <- c(ranges_list, list(rng3))
  }
  if (5 %in% ks) {
    rng5 <- if (is.null(time_period_5yrs)) {
      tibble(from = seq.int(miny, maxy - 4), to = from + 4, k = 5L)
    } else {
      time_period_5yrs |>  transmute(from = as.integer(from), to = as.integer(to), k = 5L)
    }
    ranges_list <- c(ranges_list, list(rng5))
  }
  ranges <- bind_rows(ranges_list)

  # If the ranges sit outside the data’s years, drop those that can’t match
  ranges <- ranges |>  filter(from >= miny, to <= maxy)

  if (nrow(ranges) == 0) {
    return(df |>  slice(0))
  }

  # Non-equi join each yearly row to all windows that cover its year
  pooled <- yearly |>
    inner_join(ranges, by = join_by(period_year >= from, period_year <= to)) |>
    group_by(across(all_of(POOL_KEYS)), year_type, from, to, k) |>
    summarise(
      numerator   = sum(numerator,   na.rm = TRUE),
      denominator = sum(denominator, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      # Map window start/end to actual dates by year_type
      start_date = if_else(
        year_type == "Calendar",
        lubridate::make_date(from, 1, 1),
        lubridate::make_date(from, 4, 1)
      ),
      end_date = if_else(
        year_type == "Calendar",
        lubridate::make_date(to, 12, 31),
        lubridate::make_date(to + 1, 3, 31)
      ),
      time_period_type = paste0(k, " year pooled"),
      indicator_value = NA_real_, lower_ci95 = NA_real_, upper_ci95 = NA_real_
    ) |>
    transmute(
      indicator_id, start_date, end_date,
      numerator, denominator,
      indicator_value, lower_ci95, upper_ci95,
      imd_code, aggregation_id, age_group_code, sex_code, ethnicity_code,
      creation_date, value_type_code, source_code, time_period_type, combination_id
    )

  pooled
}


# Function to get age lookup ---------------------------------------------------

age_lookup <- dbGetQuery(
  conn,
  "SELECT * FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Group]"
)


get_age_lookup <- function(metadata, age_lookup){

  # Inputs:
  #   metadata   - a dataframe containing at least column `age` and `indicator_id`
  #   age_lookup - a dataframe containing `age_group_label` and `age_code`
  # Output:
  #   A dataframe with `indicator_id` and `age_code` columns

  lookup <- metadata |>
    mutate(age_group_label = age) |>
    left_join(age_lookup, by = "age_group_label") |>
    select(indicator_id, age_code)

  return(lookup)
}


# Function to create combination id column -------------------------------------
create_combination_id <- function(data) {

  # Inputs:
  #   data   - a dataframe containing columns `imd_code` and `ethnicity_code`
  # Output:
  #   The same dataframe with an added `combination_id` column

  data |>
    mutate(
      combination_id = case_when(
        imd_code != 999 & ethnicity_code != 999 ~ 1L,  # both splits present
        imd_code != 999 & ethnicity_code == 999 ~ 2L,  # IMD only
        imd_code == 999 & ethnicity_code != 999 ~ 3L,  # Ethnicity only
        imd_code == 999 & ethnicity_code == 999 ~ 4L,  # neither (overall)
        TRUE ~ NA_integer_                           # anything unexpected/NA
      )
    )
}


# Function to clean data types -------------------------------------------------
clean_data_types <- function (data){

  # Inputs:
  #   data   - a dataframe containing columns to be cleansed
  # Output:
  #   The same dataframe with corrected datatypes

  data |>
    mutate(indicator_id    = as.integer(indicator_id),
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
           creation_date   = as.POSIXct(creation_date),
           value_type_code = as.integer(value_type_code),
           source_code     = as.integer(source_code),
           time_period_type= as.character(time_period_type),
           combination_id  = as.integer(combination_id))
}


# Function to  tidy output -----------------------------------------------------
TIDY_COLS = c("indicator_id","start_date","end_date","numerator","denominator",
              "indicator_value","lower_ci95","upper_ci95","imd_code","aggregation_id",
              "age_group_code","sex_code","ethnicity_code","creation_date",
              "value_type_code","source_code", "time_period_type", "combination_id")

tidy_output <- function(data) {

  # Inputs:
  #   data - a dataframe containing calculated results
  #          (e.g. DASR, crude ratio, percentage)
  # Output:
  #   A dataframe with corrected column names and standardised schema
  #   to enable binding results from different calculations

  out <- data |>
    mutate(
      indicator_value = value,
      lower_ci95      = lowercl,
      upper_ci95      = uppercl
    )

  # If DASR output: replace numerator/denominator
  if ("total_count" %in% names(out)) {
    out <- out |> mutate(numerator = .data$total_count)
  }
  if ("total_pop" %in% names(out)) {
    out <- out |> mutate(denominator = .data$total_pop)
  }

  out <- out |> select(all_of(TIDY_COLS))

  return(out)
}

# Function to identify time period type ----------------------------------------

get_duration_label <- function(start_date, end_date) {

  # Inputs:
  #   start_date - a Date vector
  #   end_date   - a Date vector
  # Output:
  #   A character vector of duration labels, aligned with input length

  # helper to check if period matches exactly n years
  is_n_years <- function(n) {
    anniv <- start_date %m+% years(n)
    (end_date == anniv) | (end_date == anniv - days(1))
  }

  case_when(
    is_n_years(1) ~ "1 year",
    is_n_years(3) ~ "3 year pooled",
    is_n_years(5) ~ "5 year pooled",
    TRUE ~ NA_character_
  )
}

# Function to calculate percentage ---------------------------------------------

calc_percentage <- function(df_in){

  # Inputs:
  #   df_in - dataframe eligible for processing, containing `numerator`, `denominator`,
  #           `multiplier`, and `value_type_code` (2 = percentage)
  # Output:
  #   The same rows (where value_type_code == 2) with percentage and 95% CI
  #   columns added by PHEindicatormethods::phe_proportion:
  #   `value`, `lowercl`, `uppercl`, `method`

  output <- df_in |>
    filter(value_type_code == 2) |>
    phe_proportion(
      x = numerator,
      n = denominator,
      confidence = 0.95,
      type = "standard",
      multiplier = multiplier
    )

  return(output)

}

# Function to calculate crude or ratio -----------------------------------------

calc_crude_or_ratio <- function(df_in){

  # Inputs:
  #   df_in - dataframe eligible for processing, containing `numerator`, `denominator`,
  #           `multiplier`, and `value_type_code` (3 = crude rate, 7 = ratio)
  # Output:
  #   The same rows (where value_type_code is either 3 or 7) with crude rates or ratios calculated
  #   and 95% CI columns added by PHEindicatormethods::phe_rate:
  #   `value`, `lowercl`, `uppercl`, `method`

  output <- df_in |>
    filter(value_type_code %in% c(3, 7)) |>
    mutate(
      num_sign = sign(coalesce(numerator, 0)),
      num_abs  = abs(coalesce(numerator, 0))
    ) |>
    phe_rate(
      x = num_abs,
      n = denominator,
      confidence = 0.95,
      type = "standard",
      multiplier = multiplier
    ) |>
    mutate(numerator = num_abs * num_sign,
           value = value * num_sign)

  return(output)
}

# Function to calculate dasr ---------------------------------------------------

calc_dasr <- function(df_in, metadata, age_lookup){

  # Inputs:
  #   df_in      - dataframe containing `numerator`, `denominator`, `multiplier`,
  #                and `value_type_code` (4 = DASR)
  #   metadata   - dataframe containing `indicator_id` and `age` columns,
  #                used to derive the final age code
  #   age_lookup - dataframe containing age lookups (e.g., `age_code`,
  #                `age_group_label`)
  #
  # Output:
  #   A dataframe filtered to rows where `value_type_code == 4`, with DASR
  #   calculated and 95% confidence interval columns added using
  #   `calculate_dsr3` adapted from pheindicatormethods (utils.R). The output also
  #   includes the correct `age_group_code` derived from `metadata` and
  #   `age_lookup`.

  esp2013_lookup <- PHEindicatormethods::esp2013 |>
    as_tibble() |>
    rename(stdpop = value) |>
    mutate(age_group_code = as.integer(c(1:18, 18))) |>
    group_by(age_group_code) |>
    summarise(stdpop = sum(stdpop), .groups = "drop")

  dasr_keys <- c(
    "indicator_id","start_date","end_date","imd_code","aggregation_id", "sex_code",
    "ethnicity_code","creation_date", "value_type_code","source_code","time_period_type", "combination_id"
  )

  df_calc <- df_in |>
    filter(value_type_code == 4) |>
    left_join(esp2013_lookup, by = "age_group_code")

  output <- df_calc |>
    group_by(across(all_of(dasr_keys))) |>
    calculate_dsr3(
      x = numerator,
      n = denominator,
      stdpop = stdpop,
      type = "standard",
      multiplier = multiplier
    ) |>
    ungroup() |>
    left_join(get_age_lookup(metadata = metadata, age_lookup = age_lookup), by = "indicator_id") |>
    mutate(age_group_code = age_code)


  return(output)

}


# Function to calculate different value types ----------------------------------

# Purpose:
#   Calculates and standardises indicator values (Percentage, Crude/Ratio, DASR)
#   and returns a unified schema ready for row-binding across methods.
#
# Inputs:
#   data         - dataframe with (at minimum) the following columns:
#                  indicator_id, start_date, end_date, numerator, denominator,
#                  indicator_value (may be NA), value_type_code, imd_code,
#                  aggregation_id, sex_code, ethnicity_code, creation_date,
#                  source_code
#   metadata     - OPTIONAL dataframe used to enrich `data`; must contain:
#                  <metadata_key>, age, year_type, multiplier, status_code
#   metadata_key - character scalar giving the join key name in `metadata`
#                  (default: "indicator_id")
#
# Output:
#   A dataframe in the standardised schema `TIDY_COLS`, containing:
#     - Rows not requiring calculation (kept as-is, standardised)
#     - Rows that required calculation but were ineligible (standardised)
#     - Newly calculated results for:
#         * Percentage (via calc_percentage)
#         * Crude rate / Ratio (via calc_crude_or_ratio)
#         * DASR (via calc_dasr)
#   All outputs have consistent columns (e.g., indicator_value, lower_ci95,
#   upper_ci95) and cleaned datatypes.
#
# Eligibility rules for calculation:
#   - indicator_value is NA
#   - denominator is present and > 0
#   - multiplier is present (non-NA)
#   - status_code == 1
#   - value_type_code is present (non-NA)

calculate_values <- function(data, metadata, metadata_key = "indicator_id"){

  # -------- Validate inputs / bring in multiplier ---------------------------
  message("▶ Cleaning data types and validating inputs...")
  df <- data |>
    mutate(time_period_type = get_duration_label(as.Date(start_date), as.Date(end_date))) |>
    create_combination_id() |>
    clean_data_types()

  # metadata is REQUIRED
  if (is.null(metadata)) {
    stop("`metadata` is required and must not be NULL")
  }

  # Basic checks
  if (!metadata_key %in% names(metadata)) {
    stop(sprintf("\u274C metadata must contain the key column '%s'", metadata_key))
  }
  if (!"multiplier" %in% names(metadata)) {
    stop("\u274C metadata must contain a 'multiplier' column")
  }
  if (!"status_code" %in% names(metadata)) {
    stop("\u274C metadata must contain a 'status_code' column")
  }

  # Select required columns from metadata
  metadata_cols = c(metadata_key, "age", "year_type", "multiplier", "status_code", "population_type")
  metadata2 <- metadata |> select(all_of(metadata_cols))

  # Bring metadata onto staging
  message("▶ Bringing metadata onto staging table...")
  df <- df |>
      left_join(metadata2, by = setNames(metadata_key, metadata_key))

  # Create 3 and 5 year pooled data
  message("▶ Creating 3 and 5 year pooled data...")
  pooled_df <- create_pooled_with_ranges(df, ks = c(3,5)) |>
    left_join(metadata2, by = setNames(metadata_key, metadata_key))

  # Combine pooled data with yearly data
  message("▶ Combining yearly and pooled data...")
  all_df <- bind_rows(pooled_df, df)

  # --- Partition rows -------------------------------------------------------
  # Rows that *need* a calculation: missing indicator_value AND denominator present/non-zero
  message("▶ Determining rows requiring calculations...")
  needs_calc <- is.na(all_df$indicator_value) & (!is.na(all_df$denominator) & all_df$denominator != 0)

  # Only process where status_code == 1 AND multiplier is present
  eligible_for_processing <- needs_calc &
    !is.na(all_df$multiplier) &
    all_df$status_code == 1 &
    !is.na(all_df$value_type_code)

  # To be calculated
  df_calc <- all_df[eligible_for_processing, ] |>
    mutate(numerator = if_else(is.na(numerator), 0, numerator))

  # Rows that we keep as-is (no calc needed)
  df_keep <- all_df[!needs_calc, ] |>
    mutate(denominator = if_else(is.na(denominator), 0, denominator)) |>
    left_join(get_age_lookup(metadata = metadata2, age_lookup = age_lookup), by = "indicator_id") |>
    mutate(age_group_code = age_code) |>
    select(all_of(TIDY_COLS))

  # Rows that needed calc but were NOT eligible (will be appended to the output later)
  df_skip <- all_df[needs_calc & !eligible_for_processing, ] |>
    mutate(denominator = if_else(is.na(denominator), 0, denominator)) |>
    left_join(get_age_lookup(metadata = metadata2, age_lookup = age_lookup), by = "indicator_id") |>
    mutate(age_group_code = age_code) |>
    select(all_of(TIDY_COLS))


  # -------- Percentage ------------------------------------------------------
  message("▶ Calculating percentage...")
  temp1 <- df_calc |> calc_percentage() |>
    tidy_output()

  # -------- Crude rate & Ratio ----------------------------------------------
  message("▶ Calculating crude rate and ratio...")
  temp2 <- df_calc |> calc_crude_or_ratio() |>
    tidy_output()

  # -------- Directly age standardised rate (DASR) ---------------------------
  message("▶ Calculating DASR...")
  temp3 <- df_calc |> calc_dasr(metadata = metadata2, age_lookup = age_lookup) |>
    tidy_output()

  # -------- Combine all outputs ---------------------------------------------
  message("▶ Combining all outputs...")
  output <- bind_rows(
    df_keep,          # Rows not calculated
    df_skip,          # Rows that needed calc but were ineligible (status/multiplier)
    temp1,            # Percentage
    temp2,            # Crude rate & Ratio
    temp3             # DASR
  ) |> clean_data_types()

  # -------- Reporting of output ---------------------------------------------
  message("▶ Reporting output total rows...")
  print(paste("Total rows for df_keep:", nrow(df_keep)))
  print(paste("Total rows for df_skip:", nrow(df_skip)))
  print(paste("Total rows for temp1 (perc):", nrow(temp1)))
  print(paste("Total rows for temp2 (crude & ratio):", nrow(temp2)))
  print(paste("Total rows for temp3 (DASR):", nrow(temp3)))

  # -------- Reporting of skipped items --------------------------------------
  message("▶ Reporting skipped items...")
  if (any(needs_calc & !eligible_for_processing)) {
    reasons_df <- all_df[needs_calc & !eligible_for_processing, ] |>
      transmute(
        indicator_id = indicator_id,
        status_code = status_code,
        reason_missing_multiplier = is.na(multiplier),
        reason_status_not_1 = is.na(status_code) | status_code != 1,
        reason_missing_value_type = is.na(value_type_code)
      ) |>
      group_by(indicator_id, status_code) |>
      summarise(
        reasons = paste0(
          c(
            if (any(reason_missing_multiplier)) "missing multiplier" else NULL,
            if (any(reason_status_not_1)) paste0("status_code = ", unique(status_code)) else NULL,
            if (any(reason_missing_value_type)) "missing value_type_code" else NULL
          ),
          collapse = "; "
        ),
        .groups = "drop"
      )

    message("\u26A0\uFE0F  Some indicators were not processed:")
    apply(
      reasons_df,
      1,
      function(r) message("  ID ", r[["indicator_id"]], ": ", r[["reasons"]])
    )
  }

  message("▶ Process completed!")
  return(output)
}

# Function to run SQL script ---------------------------------------------------
run_sql_file <- function(conn, path) {
  # Inputs:
  #   conn - SQL connection
  #   path - path to the SQL file
  #
  # Output:
  #   Prints a message indicating the SQL script has run successfully.
  sql_text <- paste(readLines(path, warn = FALSE), collapse = "\n")
  dbExecute(conn, sql_text)
  message("\u2705 SQL script has succesfully run!")
}

get_indicators_from_sql <- function(conn, table_name, indicator_ids = NULL) {
  # Inputs:
  #   conn          - Active SQL connection (e.g., from DBI::dbConnect)
  #   table_name    - Name of the SQL table (as a string)
  #   indicator_ids - Optional vector of indicator IDs to filter by

  # Base query
  sql_query <- paste0("SELECT * FROM ", table_name)

  # Add WHERE clause if indicator_ids provided
  if (!is.null(indicator_ids) && length(indicator_ids) > 0) {
    # Format IDs safely for SQL (handle numeric or character)
    if (is.numeric(indicator_ids)) {
      id_list <- paste(indicator_ids, collapse = ", ")
    } else {
      id_list <- paste0("'", indicator_ids, "'", collapse = ", ")
    }
    sql_query <- paste0(sql_query, " WHERE Indicator_ID IN (", id_list, ")")
  }

  # Execute and return data
  result <- DBI::dbGetQuery(conn, sql_query)

  message("\u2705 Indicators extracted from SQL!")
  return(result)
}

insert_data_into_sql_table <- function(conn, database, schema, table, data, indicator_ids = NULL,
                                               id_column = "indicator_id") {
  # Delete existing data and append new data
  # Inputs:
  #   conn          - Active SQL connection (e.g., from DBI::dbConnect)
  #   database      - Name of database (as a string)
  #   schema        - Name of schema (as a string)
  #   table         - Name of the SQL table (as a string)
  #   data          - Data frame to append to SQL
  #   indicator_ids - Optional vector of indicator IDs to delete before appending

  tbl_id  <- DBI::Id(catalog = database, schema = schema, table = table)
  tbl_sql <- DBI::dbQuoteIdentifier(conn, tbl_id)

  # Build DELETE query
  sql_query <- paste0("DELETE FROM ", tbl_sql)

  # Add WHERE clause if indicator_ids provided
  if (!is.null(indicator_ids) && length(indicator_ids) > 0) {
    # Format IDs safely for SQL (handle numeric or character)
    if (is.numeric(indicator_ids)) {
      id_list <- paste(indicator_ids, collapse = ", ")
    } else {
      id_list <- paste0("'", indicator_ids, "'", collapse = ", ")
    }
    sql_query <- paste0(sql_query, " WHERE indicator_id IN (", id_list, ")")
  }

  # Execute DELETE query
  DBI::dbExecute(conn, sql_query)

  # Append new data
  DBI::dbWriteTable(conn, name = tbl_id, value = data, append = TRUE)


  # Confirm success
  if (is.null(indicator_ids)) {
    message("\u2705 All processed indicators replaced in SQL table!")
  } else {
    message("\u2705 Processed indicators updated in SQL table for selected indicator_ids!")
  }
}

# DQ functions------------------------------------------------------------------
## Function to check row counts ------------------------------------------------

check_row_counts <- function(df, reference_data) {
  # Inputs:
  #   input_data     - data frame whose row count will be checked
  #   reference_data - data frame to compare against
  #
  # Output:
  #   Prints a message indicating whether the row counts match (with counts).

  input_rows <- nrow(df)
  reference_rows <- nrow(reference_data)

  if (input_rows == reference_rows) {
    message("\u2705 Row counts match: ", input_rows, " rows")
  } else {
    message("\u274C Row counts do NOT match. ",
            "Input: ", input_rows, " rows | Reference: ", reference_rows, " rows")
  }
}

## Function to check non-populated columns -------------------------------------

# ignoring numerator and denominator as these can be empty columns for pre-calculated indicators

check_rows_with_missing <- function(df, metadata, cols = NULL,
                              ignore = c("numerator", "denominator")
                              ) {
  # Inputs:
  #   df     - data frame whose rows will be checked
  #   metadata - metadata to filter indicators to be checked
  #
  # Output:
  #   A dataframe with columns containing missing rows

  current_ids <- metadata |>
    filter(status_code == 1 & precalculated == "No") |> # only want to check non pre-calculated indicators
    distinct(indicator_id) |>
    pull(indicator_id)

  # columns to check
  cols_to_check <-
    if (is.null(cols)) setdiff(names(df), ignore) else intersect(cols, names(df))

  df %>%
    filter(denominator != 0 & !is.na(denominator)) |> # rows required processing
    filter(indicator_id %in% current_ids) |>
    filter(if_any(all_of(cols_to_check), ~ is.na(.x)))

}

## Function to check unique age group code for dasr indicator ------------------

check_age_group_code <- function(df) {
  #  Inputs:
  #   df - data frame containing DASR records; must include
  #        `indicator_id`, `age_group_code`, and `value_type_code`
  #
  # Output:
  #   Prints a message indicating whether each DASR (`value_type_code == 4`)
  #   indicator_id has exactly one unique `age_group_code` (OK) or if any
  #   indicator_id has more than one (problem).

  unique_age_count <- df |>
    filter(value_type_code == 4) |>
    distinct(indicator_id, age_group_code) |>
    group_by(indicator_id) |>
    summarise(count = n()) |>
    arrange(indicator_id) |>
    filter(count > 1)

  if(nrow(unique_age_count) == 0){
    print("\u2705 One unique age group code per dasr indicator ID")
  } else{
    print("\u274C More than 1 age group code per dasr indicator ID")
  }
}

## Function to check all time period types are populated -----------------------
check_time_period_type <- function(df) {
  #  Inputs:
  #   df - data frame whose rows will be checked
  #
  # Output:
  #   Prints a message indicating whether time period column is populated or not
  missing_rows <- df |>
    filter(is.na(time_period_type)) |>
    distinct(indicator_id, start_date, end_date)

  if (nrow(missing_rows) == 0) {
    message("\u2705 All indicators passed: time_period_type is populated for all rows.")
  } else {
    message("\u274C Some indicators are missing time_period_type. Details below:")
    print(missing_rows)
  }
}


## Function to check value columns are populated for active indicators ---------

check_active_indicator_values <- function(df, metadata) {
  active_ids <- metadata %>%
    filter(status_code == 1) %>%
    distinct(indicator_id) %>%
    pull(indicator_id)

  # Return rows with missing values in any of the key columns
  failures <- df %>%
    filter(indicator_id %in% active_ids,
           !is.na(denominator), denominator != 0) %>%
    filter(is.na(indicator_value) | is.na(lower_ci95) | is.na(upper_ci95))

  if (nrow(failures) == 0) {
    message("\u2705 PASS: All active indicators have non-missing values.")
    return(invisible(NULL))   # don’t clutter console when no failures
  } else {
    message("\u274C FAIL: Found missing values for some active indicators.")
    return(failures)
  }
}


## Function to check combination id is populated -------------------------------
check_combination_splits <- function(df) {
  #  Inputs:
  #   df - data frame whose rows will be checked
  #
  # Output:
  #   Prints a message indicating whether `combination_id` column is populated or not
  missing_rows <- df |>
    filter(is.na(combination_id))

  if (nrow(missing_rows) == 0) {
    message("\u2705 All indicators passed: combination_id is populated for all rows.")
  } else {
    message("\u274C Some indicators are missing combination_id. Details below:")
    print(missing_rows)
  }
}

## Run all DQ checks ------------------------------------------------------------

run_all_dq_checks <- function(df, reference_data, metadata, cols_to_check = NULL, show_n = 10) {
  message("\n==== Running Data Quality Checks ====\n\n")

  message("1) Row counts\n")
  check_row_counts(df, reference_data)
  cat("\n")

  message("2) Rows with missing required columns\n")
  miss_df <- check_rows_with_missing(df, metadata, cols = cols_to_check)
  if (nrow(miss_df) == 0) {
    message("\u2705 No rows with missing values in the checked columns.")
  } else {
    message("\u274C Found rows with missing values in the checked columns: ", nrow(miss_df))
    print(utils::head(miss_df, show_n))
  }
  cat("\n")

  message("3) Unique age_group_code for DASR indicators\n")
  check_age_group_code(df)
  cat("\n")

  message("4) time_period_type populated\n")
  check_time_period_type(df)
  cat("\n")

  message("5) Active indicator values populated\n")
  act_fail <- check_active_indicator_values(df, metadata)
  if (is.data.frame(act_fail) && nrow(act_fail) > 0) {
    print(utils::head(act_fail, show_n))
  }
  cat("\n")

  message("6) combination_id populated\n")
  check_combination_splits(df)
  message("\n==== DQ checks completed ====\n")
}


































