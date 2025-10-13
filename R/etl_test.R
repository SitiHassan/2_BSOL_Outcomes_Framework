library(tidyverse)
library(readxl)
library(PHEindicatormethods)

#1. Create dummy metadta -------------------------------------------------------

set.seed(123)  # for reproducibility

indicator_ids <- 1:10

metadata_test <- data.frame(
  indicator_id    = indicator_ids,
  age             = sample(c("All ages", "<75 yrs"), length(indicator_ids), replace = TRUE),
  year_type       = rep(c("Financial", "Calendar"), length.out = length(indicator_ids)),
  multiplier      = rep(c(1, 100, 100000), length.out = length(indicator_ids)),
  status_code     = rep(c(1, 2), length.out = length(indicator_ids)),
  population_type = rep(c("Census", "Defined cohort", "GP registered"),
                        length.out = length(indicator_ids)),
  stringsAsFactors = FALSE
)

metadata_test

#2. Create dummy dataset  ------------------------------------------------------

set.seed(123)  # for reproducibility

test_df <- data.frame(
  indicator_id      = indicator_ids,
  start_date        = as.Date(rep("2025-04-01", 10)),
  end_date          = as.Date(rep("2026-03-31", 10)),
  numerator         = sample(c(0:100, NA), length(indicator_ids), replace = TRUE),
  denominator       = sample(c(0:500, NA), length(indicator_ids), replace = TRUE),
  indicator_value   = NA_real_,
  lower_ci95        = NA_real_,
  upper_ci95        = NA_real_,
  imd_code          = sample(1:5, length(indicator_ids), replace = TRUE),
  aggregation_id    = sample(1:150, length(indicator_ids), replace = TRUE),
  age_group_code    = sample(1:18, length(indicator_ids), replace = TRUE),
  sex_code          = sample(c(1:3, 999, -99), length(indicator_ids), replace = TRUE),
  ethnicity_code    = sample(c(1:49, 999, -99), length(indicator_ids), replace = TRUE),
  creation_date     = as.Date(Sys.Date()) - sample(0:1000, length(indicator_ids), replace = TRUE),
  value_type_code   = sample(c(2, 3, 4, 7), length(indicator_ids), replace = TRUE),
  source_code       = 1,
  time_period_type  = sample(c("1 year", "3 year pooled", "5 year pooled"), length(indicator_ids), replace = TRUE),
  combination_id    = sample(c(1, 2, 3, 4), length(indicator_ids), replace = TRUE)
)


# If value_type_code == 7 (ratio), make numerator randomly positive or negative
test_df <- test_df |>
  mutate(
    numerator = if_else(
      value_type_code == 7 & !is.na(numerator),
      numerator * sample(c(1, -1), n(), replace = TRUE),
      numerator
    )
  )

test_df

# Example (test passed)
test_output <- calculate_values(test_df, metadata_test)

test_output
















