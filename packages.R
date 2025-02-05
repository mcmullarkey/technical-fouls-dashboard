# Load required packages
library(hoopR)
library(dplyr)
library(stringr)
library(tidyr)
library(arrow)
library(duckplyr)
library(lubridate)
library(logger)
library(yaml)
library(future)
library(furrr)
library(digest)
library(jsonlite)
library(R6)
library(testthat)
library(tidymodels)

# Function to ensure all required packages are installed and loaded
ensure_packages <- function() {
  required_packages <- c(
    "hoopR",
    "dplyr",
    "stringr",
    "tidyr",
    "arrow",
    "duckplyr",
    "lubridate",
    "logger",
    "yaml",
    "future",
    "furrr",
    "digest",
    "jsonlite",
    "R6",
    "testthat",
    "tidymodels"
  )

  # Check for missing packages
  missing_packages <- required_packages[
    !required_packages %in% installed.packages()
  ]
  if (length(missing_packages) > 0) {
    stop(
      sprintf(
        "Missing required packages: %s\nPlease install them using install.packages()",
        paste(missing_packages, collapse = ", ")
      )
    )
  }

  # Load all packages
  invisible(lapply(required_packages, library, character.only = TRUE))
}
