library(tidyverse)
library(PHEindicatormethods)

#1. Function to calculate values -----------------------------------------------

calculate_values <- function(staging_data, metadata = NULL, metadata_key = "indicator_id"){


  # -------- Validate inputs / bring in multiplier ---------------------------
  df <- staging_data

  if (!is.null(metadata)) {
    # Basic checks
    if (!metadata_key %in% names(metadata)) {
      stop(sprintf("metadata must contain the key column '%s'", metadata_key))
    }
    if (!"multiplier" %in% names(metadata)) {
      stop("metadata must contain a 'multiplier' column")
    }

    # Avoid name clashes; coalesce if staging_data already has multiplier
    metadata2 <- metadata |>
      select(all_of(c(metadata_key, "multiplier"))) |>
      rename(multiplier_meta = multiplier)

    df <- df |>
      left_join(metadata2, by = setNames(metadata_key, metadata_key))

    # If staging had multiplier already, prefer it; else take metadata’s
    if ("multiplier" %in% names(df)) {
      df <- df |>
        mutate(multiplier = coalesce(multiplier, multiplier_meta)) |>
        select(-multiplier_meta)
    } else {
      df <- df |>
        rename(multiplier = multiplier_meta)
    }
  }

  # If no metadata supplied, staging_data MUST already have multiplier
  if (!"multiplier" %in% names(df)) {
    stop("No 'multiplier' available. Supply metadata with a 'multiplier' column or include 'multiplier' in staging_data.")
  }

  # --- Partition rows -------------------------------------------------------
  # calc rows: missing indicator_value AND denominator present/non-zero
  needs_calc <- is.na(df$indicator_value) & (!is.na(df$denominator) & df$denominator != 0)

  df_calc <- df[needs_calc, ] |>
    mutate(numerator = if_else(is.na(numerator), 0, numerator))

  df_keep <- df[!needs_calc, ] |>
    select(-multiplier) |>
    mutate(denominator = if_else(is.na(denominator), 0, denominator))


  # --- Helper to tidy outputs to a consistent schema ------------------------
  # For percentage, crude and ratio outputs
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
  # Define the grouping keys that represent ONE population unit

  # --- Standard population lookup
  esp2013_lookup <- PHEindicatormethods::esp2013 |>
    as_tibble() |>
    rename(stdpop = value) |>
    mutate(age_group_code = c(1:18, 18)) |>
    group_by(age_group_code) |>
    summarise(stdpop = sum(stdpop), .groups = "drop")

  dasr_keys <- c(
    "indicator_id","start_date","end_date",
    "imd_code","aggregation_id", "age_group_code",
    "sex_code","ethnicity_code",
    "creation_date","value_type_code","source_code"
  )

  # rows to calculate dasr
  df_dasr_calc <- df_calc |>
    filter(value_type_code == 4) |>
    left_join(esp2013_lookup, by = "age_group_code")

  # one DSR per population (across ages)
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
    temp1,            # Percentage
    temp2,            # Crude rate & Ratio
    temp3             # DASR
  )

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






















































