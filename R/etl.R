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

# Global keys to pool data by
POOL_KEYS <- c(
  "indicator_id","imd_code","aggregation_id",
  "age_group_code","sex_code","ethnicity_code",
  "creation_date","value_type_code","source_code","combination_id"
)

# Function to generate year series ---------------------------------------------
generate_year_series <- function(min_year, max_year, span_years = 3L) {
  stopifnot(is.numeric(min_year), is.numeric(max_year), is.numeric(span_years))
  min_year     <- as.integer(min_year)
  max_year     <- as.integer(max_year)
  window_years <- as.integer(span_years)
  if (window_years < 1L) stop("span_years must be >= 1")

  gap <- window_years - 1L
  if ((max_year - min_year) < gap) {
    return(data.frame(from = integer(0), to = integer(0), k = integer(0)))
  }

  starts <- seq.int(min_year, max_year - gap)
  data.frame(
    from = starts,
    to   = starts + gap,
    k    = window_years
  )
}

# Example
# generate_year_series(2014, 2024, 3)
#
# generate_year_series(2014, 2024, 5)

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


create_pooled_with_ranges <- function(df, ks = c(3,5), POOL_KEYS, time_period_3yrs = NULL, time_period_5yrs = NULL) {

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

# Function to exclude incomplete data ------------------------------------------
# Automatically exclude incomplete data cycles (e.g., financial, calendar, or other)
# based on the typical end date (mode month-day) and typical duration between start and end dates.
exclude_incomplete_data <- function(
    df,
    year_type_col, # column name: year type (e.g., "Calendar", "Financial", "Other")
    start_date_col, # column name: start date
    end_date_col, # column name: end date
    time_period_col = NULL, # optional column name for time period type (e.g., "1 year", "3 year pooled")
    indicator_col = NULL,     # optional column name for indicator (e.g., "indicator_id")
    current_date = Sys.Date(), # current date reference for detecting incomplete cycles
    enforce_duration = TRUE, # whether to filter by typical duration
    tolerance_days = 5L  # allowed deviation (days) from typical duration
) {
  # Capture column names for tidy evaluation
  year_type <- rlang::ensym(year_type_col)
  start_date <- rlang::ensym(start_date_col)
  end_date <- rlang::ensym(end_date_col)
  time_period <- if (!is.null(time_period_col)) rlang::ensym(time_period_col) else NULL
  indicator   <- if (!is.null(indicator_col)) rlang::ensym(indicator_col) else NULL

  # Standardize year type and time period text for grouping
  df2 <- df |>
    mutate(
      .row_id  = dplyr::row_number(), # row id for tracking
      yt_lower = stringr::str_to_lower(as.character(!!year_type)),
      tp_lower = if (!is.null(time_period)) stringr::str_to_lower(as.character(!!time_period)) else NA_character_
    )

  # Determine which columns to group by (year type + optional time period)
  group_keys <- c("yt_lower", if (!is.null(time_period)) "tp_lower" else NULL)

  meta <- df2 |>
    filter(!is.na(!!end_date), !!end_date <= current_date) |> # keep only past data
    mutate(
      mmdd = format(!!end_date, "%m-%d"),  # extract month-day part of end date
      duration_days = if_else(!is.na(!!start_date), as.numeric(!!end_date - !!start_date) + 1, NA_real_)
    ) |>
    group_by(across(all_of(group_keys))) |>
    summarise(
      end_mmdd = stat_mode(mmdd), # most common end month-day (e.g., 03-31)
      typical_duration = stats::median(duration_days, na.rm = TRUE), # median number of days per cycle
      .groups = "drop"
    )

  # Compute filtered result
  result <- df2 |>
    left_join(meta, by = group_keys) |>  # Join metadata back to the original data and filter incomplete periods
    mutate(
      end_mmdd = coalesce(end_mmdd, "12-31"), # Default to Dec 31 if no mode end date found
      # Construct this year's expected cycle end date
      cycle_end_this_year = as.Date(sprintf("%d-%s", lubridate::year(current_date), end_mmdd)),
      # If this year’s cycle hasn’t ended yet, use last year’s end date
      latest_cycle_end = if_else(
        cycle_end_this_year <= current_date, cycle_end_this_year,
        as.Date(sprintf("%d-%s", lubridate::year(current_date) - 1L, end_mmdd))
      ),
      # Calculate actual duration for each record
      duration_days = if_else(!is.na(!!start_date) & !is.na(!!end_date),
                                     as.numeric(!!end_date - !!start_date) + 1, NA_real_),
      # Check if duration is within the expected tolerance range
      duration_ok = if_else(
        enforce_duration & !is.na(typical_duration) & !is.na(duration_days),
        abs(duration_days - typical_duration) <= tolerance_days,
        TRUE
      )
    ) |>
    # Keep only rows that end on or before the latest completed cycle and have valid duration
    filter(!is.na(!!end_date), !!end_date <= latest_cycle_end, duration_ok)

  # Determine excluded rows (anti-join by row id)
  excluded <- df2 |> dplyr::anti_join(result |> dplyr::select(.row_id), by = ".row_id")

  # Print summary of exclusions
  total_excluded <- nrow(excluded)
  total_rows     <- nrow(df)
  if (total_excluded == 0) {
    message("No rows were excluded (0/", total_rows, ").")
  } else {
    message("Excluded ", total_excluded, " row(s) out of ", total_rows, ".")
    if (!is.null(indicator)) {
      by_indicator <- excluded |>
        dplyr::mutate(`Indicator` = as.character(!!indicator)) |>
        dplyr::count(`Indicator`, name = "Excluded_Rows") |>
        dplyr::arrange(dplyr::desc(Excluded_Rows))
      print(by_indicator)
    }
  }

  # Return filtered data without helper columns
  result |>
    dplyr::select(-yt_lower, -tp_lower, -cycle_end_this_year) |>
    dplyr::select(-.row_id)
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
           value = value * num_sign,
           lower_tmp = pmin(lowercl, uppercl),
           upper_tmp = pmax(lowercl, uppercl),
           lowercl   = if_else(num_sign < 0, -upper_tmp, lower_tmp),
           uppercl   = if_else(num_sign < 0, -lower_tmp, upper_tmp)) |>
    select(-lower_tmp, -upper_tmp)

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

  # Exclude incomplete data
  message("▶ Excluding incomplete data..")
  df <- exclude_incomplete_data(df = df, year_type_col = "year_type",
                                    start_date_col = "start_date", end_date_col = "end_date",
                                    time_period_col = "time_period_type",
                                    current_date = Sys.Date(), enforce_duration = TRUE, tolerance_days = 5L)


  # Create 3 and 5 year pooled data
  message("▶ Creating 3 and 5 year pooled data...")
  pooled_df <- create_pooled_with_ranges(df, ks = c(3,5), POOL_KEYS = POOL_KEYS) |>
    left_join(metadata2, by = setNames(metadata_key, metadata_key))

  # Combine pooled data with yearly data
  message("▶ Combining yearly and pooled data...")
  all_df <- bind_rows(pooled_df, df)

  # --- Partition rows -------------------------------------------------------
  # Rows that *need* a calculation: missing indicator_value AND denominator present/non-zero
  message("▶ Determining rows requiring calculations...")
  needs_calc <- is.na(all_df$indicator_value) & (!is.na(all_df$denominator) & all_df$denominator != 0)

  # Only process where status_code == 1 or 2 AND multiplier is present
  eligible_for_processing <- needs_calc &
    !is.na(all_df$multiplier) &
    all_df$status_code %in% c(1,2) &
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

  message("\u2705 Process completed!")
  # Return a list of all dfs for tracking
  return(list(combined_calc_dfs = output,
              df_keep = df_keep,
              df_skip = df_skip,
              perc = temp1,
              crude_ratio = temp2,
              dasr = temp3))
}

# Function to normalise indicator ids ------------------------------------------
# To normalise indicator ids so that function accepts either
# "All" / "all" / "*" : process all indicators, or
# a vector of IDs (numeric or character), or
# a single comma-separated string like "10, 11, 12"
# especially useful when specifying which ids to extract from SQL using
# get_indicators_from_sql function and which ids to process using run_all function

normalize_indicator_ids <- function(indicator_ids) {
  # Treat NULL or "all"/"*"(any case) as no filter
  if (is.null(indicator_ids)) return(NULL)
  if (is.character(indicator_ids) && length(indicator_ids) == 1) {
    if (tolower(indicator_ids) %in% c("all", "*")) return(NULL)
    # If a single comma-separated string, split it
    if (grepl(",", indicator_ids)) {
      indicator_ids <- trimws(unlist(strsplit(indicator_ids, ",")))
    }
  }
  # Coerce numerics when possible; keep characters that aren't numeric
  suppressWarnings({
    as_num <- suppressWarnings(as.numeric(indicator_ids))
  })
  out <- ifelse(!is.na(as_num), as_num, indicator_ids)
  out <- unique(out)
  out[!is.na(out) & out != ""]
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

# To extract indicators from sql table
get_indicators_from_sql <- function(conn, table_name, indicator_ids = NULL) {
  # Base query
  sql_query <- paste0("SELECT * FROM ", table_name)

  # WHERE clause if IDs provided
  if (!is.null(indicator_ids) && length(indicator_ids) > 0) {
    # Quote each literal safely for this connection (handles numbers/strings)
    quoted <- vapply(indicator_ids, DBI::dbQuoteLiteral, character(1), conn = conn)
    sql_query <- paste0(sql_query, " WHERE Indicator_ID IN (", paste(quoted, collapse = ", "), ")")
  }

  # Execute
  result <- DBI::dbGetQuery(conn, sql_query)
  message("\u2705 Indicators extracted from SQL!")
  print(paste("Total rows for data extracted:", nrow(result)))
  result
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

  # Normalize the ids (NULL or "all"/"*" to delete all rows or certain ids)
  ids <- normalize_indicator_ids(indicator_ids)

  tbl_id  <- DBI::Id(catalog = database, schema = schema, table = table)
  tbl_sql <- DBI::dbQuoteIdentifier(conn, tbl_id)
  col_sql <- DBI::dbQuoteIdentifier(conn, id_column)

  # Build DELETE
  sql_query <- paste0("DELETE FROM ", tbl_sql)
  if (!is.null(ids) && length(ids) > 0) {
    # Quote each literal (works for numbers and strings)
    quoted_vals <- vapply(ids, DBI::dbQuoteLiteral, character(1), conn = conn)
    sql_query <- paste0(sql_query, " WHERE ", col_sql, " IN (", paste(quoted_vals, collapse = ", "), ")")
  } # else: NULL/empty then delete ALL rows in the table

  # Execute DELETE and append
  DBI::dbExecute(conn, sql_query)
  DBI::dbWriteTable(conn, name = tbl_id, value = data, append = TRUE)

  # Messages
  if (is.null(ids) || length(ids) == 0) {
    message("\u2705 All rows deleted and new data appended to ", DBI::dbQuoteIdentifier(conn, DBI::Id(schema=schema, table=table)), ".")
  } else {
    message("\u2705 Deleted and replaced data for selected indicator_ids in ", DBI::dbQuoteIdentifier(conn, DBI::Id(schema=schema, table=table)), ".")
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

## Function to check duplicates ------------------------------------------------

check_duplicates <- function(df, key_cols=c(
  "indicator_id",
  "time_period_type",
  "aggregation_id",
  "start_date",
  "end_date",
  "imd_code",
  "ethnicity_code"
), indicator_filter = NULL) {

  result <- df %>%

    # Optional indicator filter
    {
      if (!is.null(indicator_filter)) {
        filter(., indicator_id %in% indicator_filter)
      } else {
        .
      }
    } |>

    # Group by key columns
    group_by(across(all_of(key_cols))) |>

    # Count rows
    summarise(row_count = n(), .groups = "drop") |>

    # Keep only duplicates
    filter(row_count > 1) |>

    arrange(indicator_id)

  if (nrow(result) > 0) {
    message(
      "❌ Duplicate records found: ",
      nrow(result),
      " duplicated key combination(s)."
    )
  } else {
    message("✅ No duplicate records found for the specified key columns.")
  }

  return(result)
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
  cat("\n")

  message("7) Identify duplicates\n")
  check_duplicates(df)
  message("\n==== DQ checks completed ====\n")
}


































