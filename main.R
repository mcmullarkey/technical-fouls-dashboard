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
  
  # Initialize S3 connection first
  cat("\nInitializing S3 connection...\n")
  tryCatch(
    {
      initialize_s3()
      cat("S3 connection initialized successfully\n")
      
      # Check for existing versions before run
      cat("\nChecking existing versions in S3...\n")
      versions <- tryCatch(
        list_versions(),
        error = function(e) {
          cat("Note: No existing versions found (this is normal for first run)\n")
          return(NULL)
        }
      )
      if (!is.null(versions) && nrow(versions) > 0) {
        cat("Existing versions:\n")
        print(versions)
      }
    },
    error = function(e) {
      cat("Error initializing S3:", conditionMessage(e), "\n")
      stop(e)
    }
  )
  
  # Try to run pipeline with extra debugging
  cat("\nAttempting to run pipeline...\n")
  tryCatch(
    {
      # Using run_pipeline which calls main()
      result <- run_pipeline(config_path)
      cat("Pipeline completed successfully\n")
      
      # Verify versions after run
      cat("\nVerifying saved version...\n")
      versions <- tryCatch(
        list_versions(),
        error = function(e) NULL
      )
      if (!is.null(versions) && nrow(versions) > 0) {
        cat("Current versions in S3:\n")
        print(versions)
      } else {
        cat("Warning: No versions found after pipeline run\n")
      }
      
      return(result)
    },
    error = function(e) {
      cat("Pipeline error:", conditionMessage(e), "\n")
      
      # Try to get error from S3 first
      tryCatch({
        # List error files in S3
        error_objects <- aws.s3::get_bucket(
          bucket = "carbonite-bucket",
          prefix = "errors/error_"
        )
        
        if (length(error_objects) > 0) {
          # Get most recent error file
          latest_error <- error_objects[[
            which.max(sapply(error_objects, function(x) x$LastModified))
          ]]
          
          cat("\nAttempting to read error report from S3:", latest_error$Key, "\n")
          
          # Create temporary file to read error report
          tmp_file <- tempfile(fileext = ".rds")
          aws.s3::save_object(
            object = latest_error$Key,
            bucket = "carbonite-bucket",
            file = tmp_file
          )
          
          error_report <- readRDS(tmp_file)
          unlink(tmp_file)
          
          cat("\nContents of error report from S3:\n")
          print(error_report)
        }
      }, error = function(s3_error) {
        cat("Could not retrieve error report from S3:", conditionMessage(s3_error), "\n")
        
        # Fall back to local error file check
        error_dir <- file.path("feature_store", "errors")
        if (dir.exists(error_dir)) {
          error_files <- list.files(
            error_dir,
            pattern = "error_.*\\.rds$",
            full.names = TRUE
          )
          if (length(error_files) > 0) {
            latest_error <- error_files[which.max(file.info(error_files)$mtime)]
            cat("\nContents of local error report:", latest_error, "\n")
            print(readRDS(latest_error))
          }
        }
      })
      
      stop(e) # Re-throw the error
    }
  )
}

# Main execution
if (!interactive()) {
  debug_run()
}