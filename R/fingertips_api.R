library(fingertipsR)

# test ID = 212 FTPs -The percentage of patients registered with a history of stroke or transient ischaemic attack (TIA) recorded on their GP practice's disease register.



get_fingertips_indicators <- function(indicator_ids) {
  # Load required packages
  if (!requireNamespace("httr", quietly = TRUE)) install.packages("httr")
  if (!requireNamespace("jsonlite", quietly = TRUE)) install.packages("jsonlite")

  library(httr)
  library(jsonlite)

  # Ensure input is a character vector
  indicator_ids <- as.character(indicator_ids)

  # Base URL for Fingertips API
  base_url <- "https://fingertips.phe.org.uk/api/all_data/csv/by_indicator_id?indicator_ids="

  # Initialize empty list to store results
  results_list <- list()

  # Loop through each indicator ID
  for (id in indicator_ids) {
    url <- paste0(base_url, id)

    # Try to fetch and parse data
    tryCatch({
      response <- GET(url)

      if (status_code(response) != 200) {
        stop(paste("HTTP error", status_code(response)))
      }

      # Read CSV content directly from response
      content_text <- content(response, "text", encoding = "UTF-8")
      df <- read.csv(text = content_text, stringsAsFactors = FALSE)

      results_list[[id]] <- df
      message(paste("Successfully retrieved indicator:", id))

    }, error = function(e) {
      message(paste("Error retrieving indicator", id, ":", e$message))
      results_list[[id]] <- NULL
    })
  }

  # Combine all successful data frames into one
  combined_df <- do.call(rbind, results_list[!sapply(results_list, is.null)])

  return(combined_df)
}

# Example indicator IDs
ids <- c(90362, 11001, "bad_id")

# Fetch data
fingertips_data <- get_fingertips_indicators(ids)

# View result
head(fingertips_data)

a <- get_fingertips_indicators(212)
