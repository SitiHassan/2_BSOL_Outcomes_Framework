# clean dates
.to_ymd <- function(x) {
  out <- suppressWarnings({
    if (is.numeric(x)) as.Date(x, origin = "1899-12-30") else as.Date(x)
  })
  ifelse(is.na(out), NA_character_, format(out, "%Y-%m-%d"))
}

# process excel files
process_indicator_excels <- function(
    folder_new_data,
    folder_old_data,
    validation_sheet = "1.Cover",
    validation_cell  = "D41",
    data_sheet       = "4.Data Entry"
){
  files <- list.files(folder_new_data, pattern = "^ID.*\\.(xlsx|xlsm)$",
                      full.names = TRUE, ignore.case = TRUE)

  if (!length(files)) {
    message("No files found.")
    return(list(data = tibble(), success_files = character(0), failed_files = character(0)))
  }

  if (!dir.exists(folder_old_data)) dir.create(folder_old_data, recursive = TRUE)

  required_cols <- c(
    "indicator_id","start_date","end_date","numerator","denominator",
    "indicator_value","lower_ci95","upper_ci95","imd_code","aggregation_id",
    "age_group_code","sex_code","ethnicity_code","creation_date",
    "value_type_code","source_code"
  )

  total   <- length(files)
  success <- character()
  fail    <- character()
  parts   <- list()

  for (i in seq_along(files)) {
    path  <- files[i]
    fname <- basename(path)
    message(sprintf("🔍 Processing file %d of %d: %s", i, total, fname))

    # --- validation ---
    val <- try(
      suppressMessages(
        read_excel(path, sheet = validation_sheet, range = validation_cell, col_names = FALSE)
      ),
      silent = TRUE
    )
    if (inherits(val, "try-error") || is.na(suppressWarnings(as.numeric(val[[1,1]]))) ||
        as.numeric(val[[1,1]]) != 0) {
      message(sprintf("⚠️  Skipping file %d of %d due to validation error: %s", i, total, fname))
      fail <- c(fail, fname)
      next
    }

    # --- read data sheet ---
    df <- try(
      suppressMessages(read_excel(path, sheet = data_sheet)),
      silent = TRUE
    )
    if (inherits(df, "try-error")) {
      message(sprintf("⚠️  Skipping file %d of %d due to read error: %s", i, total, fname))
      fail <- c(fail, fname)
      next
    }

    # --- select and coerce ---
    df <- df %>%
      select(all_of(required_cols)) %>%
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
        creation_date   = as.POSIXct(creation_date),
        value_type_code = as.integer(value_type_code),
        source_code     = as.integer(source_code)
      )

    parts[[length(parts)+1]] <- df
    success <- c(success, fname)

    # --- move file ---
    dest <- file.path(folder_old_data, fname)
    moved <- try(file.rename(path, dest), silent = TRUE)
    if (inherits(moved, "try-error") || isFALSE(moved)) {
      if (file.copy(path, dest, overwrite = TRUE)) unlink(path)
    }

    message(sprintf("✅ Processed file %d of %d: %s", i, total, fname))
  }

  # --- summary ---
  message("────────────────────────────────────")
  message(sprintf("Finished: %d of %d files processed.", length(success), total))
  if (length(fail)) {
    message("❌ The following files failed:")
    message("   - ", paste(fail, collapse = "\n   - "))
  } else {
    message("All files processed successfully ✅")
  }

  list(
    data          = if (length(parts)) bind_rows(parts) else tibble(),
    success_files = success,
    failed_files  = fail
  )
}


# Example:
result <- process_indicator_excels(
  folder_new_data = "data/sharepoint_data/new_data",
  folder_old_data = "data/sharepoint_data/old_data"
)

# The processed dataset
indicator_excels <- result$data

# Write data into database -----------------------------------------------------
# Get unique, non-NA indicator_ids from the new data
indicator_ids <- unique(na.omit(indicator_excels$indicator_id))


if (length(indicator_ids) > 0) {
  # Build a comma-separated list for the IN() clause
  id_list <- paste(indicator_ids, collapse = ",")

  delete_sql <- paste0(
    "DELETE FROM [EAT_Reporting_BSOL].[OF].[OF2_Indicator_Sharepoint_Data] ",
    "WHERE indicator_id IN (", id_list, ")"
  )

  # Run the delete statement first to remove existing rows for the specified indicators
  message("Deleting the existing rows...")
  dbExecute(conn, delete_sql)
  message("Existing rows deleted ✅")
}

message("Loading data into the SharePoint table...")

# Write the data
dbWriteTable(
  conn,
  name  = Id(schema = "OF", table = "OF2_Indicator_Sharepoint_Data"),
  value = indicator_excels,
  append = TRUE
)

message("SharePoint data loaded ✅")