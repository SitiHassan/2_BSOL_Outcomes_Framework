library(tidyverse)
library(readxl)
library(PHEindicatormethods)

#1. Read metadata file ---------------------------------------------------------
metadata <- read_xlsx("data/metadata.xlsx")

#2. Create dummy dataset with multiplier ---------------------------------------

set.seed(123)  # for reproducibility

test_df_with_multiplier <- data.frame(
  indicator_id     = 1:10,
  start_date       = as.Date(rep("2025-04-01", 10)),
  end_date         = as.Date(rep("2026-03-31", 10)),
  numerator        = sample(c(0:100, NA), 10, replace = TRUE),
  denominator      = sample(c(0:500, NA), 10, replace = TRUE),
  indicator_value  = NA_real_,
  lower_ci95       = NA_real_,
  upper_ci95       = NA_real_,
  imd_code          = sample(1:5, 10, replace = TRUE),
  aggregation_id    = sample(1:150, 10, replace = TRUE),
  age_group_code    = sample(1:18, 10, replace = TRUE),
  sex_code           = sample(c(1:3, 999, -99), 10, replace = TRUE),
  ethnicity_code     = sample(c(1:49, 999, -99), 10, replace = TRUE),
  creation_date      = as.Date(Sys.Date()) - sample(0:1000, 10, replace = TRUE),
  value_type_code    = sample(c(2, 3, 4, 7), 10, replace = TRUE),
  source_code         = 1,
  multiplier           = sample(c(1, 100, 1000, 100000), 10, replace = TRUE)
)

# If value_type_code == 7, make numerator randomly positive or negative
test_df_with_multiplier <- test_df_with_multiplier |>
  mutate(
    numerator = if_else(
      value_type_code == 7 & !is.na(numerator),
      numerator * sample(c(1, -1), n(), replace = TRUE),
      numerator
    )
  )

test_df_with_multiplier

# Example (test passed)
test_output <- calculate_values(test_df_with_multiplier)

check_row_counts(test_output, test_df_with_multiplier)


#3. Create dummy dataset without multiplier ---------------------------------------

set.seed(123)  # for reproducibility

test_df_without_multiplier <- data.frame(
  indicator_id     = 1:10,
  start_date       = as.Date(rep("2025-04-01", 10)),
  end_date         = as.Date(rep("2026-03-31", 10)),
  numerator        = sample(c(0:100, NA), 10, replace = TRUE),
  denominator      = sample(c(0:500, NA), 10, replace = TRUE),
  indicator_value  = NA_real_,
  lower_ci95       = NA_real_,
  upper_ci95       = NA_real_,
  imd_code          = sample(1:5, 10, replace = TRUE),
  aggregation_id    = sample(1:150, 10, replace = TRUE),
  age_group_code    = sample(1:18, 10, replace = TRUE),
  sex_code           = sample(c(1:3, 999, -99), 10, replace = TRUE),
  ethnicity_code     = sample(c(1:49, 999, -99), 10, replace = TRUE),
  creation_date      = as.Date(Sys.Date()) - sample(0:1000, 10, replace = TRUE),
  value_type_code    = sample(c(2, 3, 4, 7), 10, replace = TRUE),
  source_code         = 1
)

# If value_type_code == 7, make numerator randomly positive or negative
test_df_without_multiplier <- test_df_without_multiplier |>
  mutate(
    numerator = if_else(
      value_type_code == 7 & !is.na(numerator),
      numerator * sample(c(1, -1), n(), replace = TRUE),
      numerator
    )
  )

test_df_without_multiplier

# Example (test passed)
test_output <- calculate_values(test_df_without_multiplier, metadata)

check_row_counts(test_output, test_df_without_multiplier)


# Example (test failed)
test_output <- calculate_values(test_df_without_multiplier) # no multiplier supplied
















































