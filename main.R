# Source both packages and functions
source("packages.R")
source("functions.R")

#' Debug wrapper for running the pipeline
#' @param config_path Path to configuration file
debug_run <- function(config_path = "config.yml") {
  # Print current working directory
  cat("Current working directory:", getwd(), "\n")

  # Check if config file exists
  cat("Checking config file:", config_path, "\n")
  cat("Config file exists:", file.exists(config_path), "\n")

  if (file.exists(config_path)) {
    # Print first few lines of config
    cat("\nFirst few lines of config file:\n")
    cat(readLines(config_path, n = 5), sep = "\n")
  }

  # Try to load yaml directly
  cat("\nTrying to load YAML...\n")
  tryCatch(
    {
      config <- yaml::read_yaml(config_path)
      cat("YAML loaded successfully. Contents:\n")
      print(str(config))
    },
    error = function(e) {
      cat("Error loading YAML:", conditionMessage(e), "\n")
    }
  )

  # Try to run pipeline with extra debugging
  cat("\nAttempting to run pipeline...\n")
  tryCatch(
    {
      result <- run_pipeline(config_path)
      cat("Pipeline completed successfully\n")
      return(result)
    },
    error = function(e) {
      cat("Pipeline error:", conditionMessage(e), "\n")

      # Try to read the error report
      error_dir <- file.path("feature_store", "errors")
      if (dir.exists(error_dir)) {
        error_files <- list.files(
          error_dir,
          pattern = "error_.*\\.rds$",
          full.names = TRUE
        )
        if (length(error_files) > 0) {
          latest_error <- error_files[which.max(file.info(error_files)$mtime)]
          cat("\nContents of error report:", latest_error, "\n")
          print(readRDS(latest_error))
        }
      }
      stop(e) # Re-throw the error
    }
  )
}

# Main execution
if (!interactive()) {
  debug_run()
}
