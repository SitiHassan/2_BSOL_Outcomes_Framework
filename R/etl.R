library(tidyverse)
library(PHEindicatormethods)

#1. Function to calculate values -----------------------------------------------

calculate_values <- function(staging_data, metadata = NULL, metadata_key = "indicator_id"){

  # -------- Validate inputs / bring in multiplier ---------------------------
  df <- staging_data |>
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
           creation_date   = as.Date(creation_date),
           value_type_code = as.integer(value_type_code),
           source_code     = as.integer(source_code))

  if (!is.null(metadata)) {
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
    metadata2 <- metadata |>
      select(all_of(c(metadata_key, "multiplier", "status_code")))

    # Bring metadata onto staging
    df <- df |>
      left_join(metadata2, by = setNames(metadata_key, metadata_key))
  } else {
    # If metadata is NULL, create placeholders so downstream logic works
    df <- df |>
      mutate(multiplier = NA_real_,
             status_code = NA_integer_)
  }

  # --- Partition rows -------------------------------------------------------
  # Rows that *need* a calculation: missing indicator_value AND denominator present/non-zero
  needs_calc <- is.na(df$indicator_value) & (!is.na(df$denominator) & df$denominator != 0)

  # Only process where status_code == 1 AND multiplier is present
  eligible_for_processing <- needs_calc &
    !is.na(df$multiplier) &
    df$status_code == 1 &
    !is.na(df$value_type_code)

  # To be calculated
  df_calc <- df[eligible_for_processing, ] |>
    mutate(numerator = if_else(is.na(numerator), 0, numerator))

  # Rows that we keep as-is (no calc needed)
  df_keep <- df[!needs_calc, ] |>
    select(-multiplier, -status_code) |>
    mutate(denominator = if_else(is.na(denominator), 0, denominator))

  # Rows that needed calc but were NOT eligible (will be appended to the output later)
  df_skip <- df[needs_calc & !eligible_for_processing, ] |>
    select(-multiplier, -status_code) |>
    mutate(denominator = if_else(is.na(denominator), 0, denominator))

  # --- Helper to tidy outputs to a consistent schema ------------------------
  tidy_output <- function(x) {
    x |>
      mutate(
        indicator_value = value,
        lower_ci95 = lowercl,
        upper_ci95 = uppercl
      ) |>
      select(
        indicator_id, start_date, end_date, numerator, denominator,
        indicator_value, lower_ci95, upper_ci95, imd_code, aggregation_id,
        age_group_code, sex_code, ethnicity_code, creation_date,
        value_type_code, source_code
      )
  }

  # -------- Percentage ------------------------------------------------------
  temp1 <- df_calc |>
    filter(value_type_code == 2) %>%
    phe_proportion(
      x = numerator,
      n = denominator,
      confidence = 0.95,
      type = "standard",
      multiplier = multiplier
    ) |>
    tidy_output()

  # -------- Crude rate & Ratio ----------------------------------------------
  temp2 <- df_calc |>
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
           value = value * num_sign) |>
    tidy_output()

  # -------- Directly age standardised rate (DASR) ---------------------------
  esp2013_lookup <- PHEindicatormethods::esp2013 |>
    as_tibble() |>
    rename(stdpop = value) |>
    mutate(age_group_code = as.integer(c(1:18, 18))) |>
    group_by(age_group_code) |>
    summarise(stdpop = sum(stdpop), .groups = "drop")

  dasr_keys <- c(
    "indicator_id","start_date","end_date",
    "imd_code","aggregation_id", "age_group_code",
    "sex_code","ethnicity_code",
    "creation_date","value_type_code","source_code"
  )

  df_dasr_calc <- df_calc |>
    filter(value_type_code == 4) |>
    left_join(esp2013_lookup, by = "age_group_code")

  temp3 <- df_dasr_calc |>
    group_by(across(all_of(dasr_keys))) |>
    calculate_dsr3(
      x = numerator,
      n = denominator,
      stdpop = stdpop,
      type = "standard",
      multiplier = multiplier
    ) |>
    rename(
      numerator = total_count,
      denominator = total_pop,
      indicator_value = value,
      lower_ci95 = lowercl,
      upper_ci95 = uppercl
    ) |>
    ungroup() |>
    select(indicator_id, start_date, end_date, numerator, denominator,
           indicator_value, lower_ci95, upper_ci95, imd_code, aggregation_id,
           age_group_code, sex_code, ethnicity_code, creation_date,
           value_type_code, source_code)

  # -------- Combine all outputs ---------------------------------------------
  output <- bind_rows(
    df_keep,          # Rows not calculated
    df_skip,          # Rows that needed calc but were ineligible (status/multiplier)
    temp1,            # Percentage
    temp2,            # Crude rate & Ratio
    temp3             # DASR
  )

  # -------- Reporting of skipped items --------------------------------------
  if (any(needs_calc & !eligible_for_processing)) {
    reasons_df <- df[needs_calc & !eligible_for_processing, ] |>
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

  return(output)
}

#2. Function to check row counts -------------------------------------------------

check_row_counts <- function(input_data, reference_data) {
  input_rows <- nrow(input_data)
  reference_rows <- nrow(reference_data)

  if (input_rows == reference_rows) {
    message("\u2705 Row counts match: ", input_rows, " rows")
  } else {
    message("\u274C Row counts do NOT match. ",
            "Input: ", input_rows, " rows | Reference: ", reference_rows, " rows")
  }
}
#2. Function to check row counts -------------------------------------------------

check_row_counts <- function(input_data, reference_data) {
  input_rows <- nrow(input_data)
  reference_rows <- nrow(reference_data)

  if (input_rows == reference_rows) {
    message("\u2705 Row counts match: ", input_rows, " rows")
  } else {
    message("\u274C Row counts do NOT match. ",
            "Input: ", input_rows, " rows | Reference: ", reference_rows, " rows")
  }
}




















































