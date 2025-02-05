# Source packages first
source("packages.R")

#' Main execution function for feature pipeline
#' @param config_path Path to configuration file
#' @return List containing features and statistics
main <- function(config_path = "config.yml") {
  run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")

  tryCatch(
    {
      # Initialize configuration and logging
      config <- load_config(config_path)
      log_info("Starting feature extraction pipeline. Run ID: {run_id}")

      # Check eligibility counts from latest version
      log_info("Checking eligibility counts from latest version...")
      eligibility_info <- get_latest_eligibility_counts()

      if (!is.null(eligibility_info)) {
        log_info(
          "Latest version ({eligibility_info$version_hash}) eligibility counts:"
        )
        log_info(
          "- Training eligible rows: {eligibility_info$counts$training_eligible}"
        )
        log_info(
          "- Prediction eligible rows: {eligibility_info$counts$prediction_eligible}"
        )
        log_info("- Total rows: {eligibility_info$counts$total_rows}")
        log_info("- From run ID: {eligibility_info$run_id}")
        log_info("- Created at: {eligibility_info$timestamp}")
      }

      run_model_training()

      # # Initialize parallel processing
      # workers <- get_config("feature_store.max_parallel_workers")
      # plan(multisession, workers = workers)
      # log_info("Initialized parallel processing with {workers} workers")
      #
      # # Get play-by-play data
      # log_info("Fetching play-by-play data")
      # df <- get_pbp_data()
      # log_info(
      #   "Retrieved {nrow(df)} plays across {n_distinct(df$game_id)} games"
      # )
      #
      # # Extract features
      # log_info("Beginning feature extraction")
      # features <- extract_turnover_features(df, run_id)
      # log_info("Extracted features for {nrow(features)} turnover events")
      #
      # # Get statistics
      # log_info("Generating feature store statistics")
      # stats <- get_feature_store_stats(features)
      # log_debug("Feature statistics: {str(stats)}")
      #
      # # Analyze outcomes
      # log_info("Analyzing outcomes")
      # outcome_analysis <- analyze_outcomes(features)
      #
      # # Create pipeline results
      # results <- list(
      #   run_id = run_id,
      #   features = features,
      #   stats = stats,
      #   outcome_analysis = outcome_analysis,
      #   execution_time = difftime(
      #     Sys.time(),
      #     as.POSIXct(run_id, format = "%Y%m%d_%H%M%S")
      #   )
      # )
      #
      # log_success("Pipeline completed successfully in {results$execution_time}")
      # results
    },
    error = function(e) {
      log_error("Pipeline failed: {conditionMessage(e)}")
      log_error(
        "Stack trace: {paste(capture.output(traceback()), collapse = '\n')}"
      )

      # Create error report
      error_report <- list(
        run_id = run_id,
        error_message = conditionMessage(e),
        error_time = Sys.time(),
        error_trace = capture.output(traceback()),
        session_info = sessionInfo()
      )

      # Save error report
      error_path <- file.path(
        get_config("paths.base_dir"),
        "errors",
        paste0("error_", run_id, ".rds")
      )
      dir.create(dirname(error_path), recursive = TRUE, showWarnings = FALSE)
      saveRDS(error_report, error_path)

      log_info("Error report saved to: {error_path}")
      stop(sprintf("Pipeline failed. See error report at: %s", error_path))
    },
    finally = {
      # Clean up resources
      plan(sequential) # Reset parallel processing
      log_info("Cleaned up resources")

      # Archive logs if configured
      if (get_config("logging.archive", default = FALSE)) {
        archive_logs(run_id)
      }
    }
  )
}

#' Archive logs for a specific run
#' @param run_id Run identifier
#' @return Path to archived log file
archive_logs <- function(run_id) {
  log_file <- get_config("logging.file")
  archive_dir <- file.path(get_config("paths.base_dir"), "logs")
  dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)

  archived_path <- file.path(
    archive_dir,
    paste0("log_", run_id, ".log")
  )

  file.copy(log_file, archived_path)
  log_info("Archived logs to: {archived_path}")

  archived_path
}

#' Run the pipeline with specified configuration
#' @param config_path Path to configuration file
#' @param archive_logs Whether to archive logs
#' @return Pipeline results
run_pipeline <- function(config_path = "config.yml", archive_logs = FALSE) {
  # Override logging archive setting for this run
  if (!is.null(archive_logs)) {
    Sys.setenv(FEATURE_STORE_ARCHIVE_LOGS = as.character(archive_logs))
  }

  main(config_path)
}

#' Load configuration from YAML file
#' @param config_path Path to the YAML configuration file
#' @return List of configuration values
load_config <- function(config_path = "config.yml") {
  tryCatch(
    {
      if (!file.exists(config_path)) {
        stop(sprintf("Configuration file not found: %s", config_path))
      }

      config <- yaml::read_yaml(config_path)

      # Validate required configuration sections
      required_sections <- c(
        "feature_store",
        "turnover_rules",
        "game_rules",
        "paths"
      )
      missing_sections <- setdiff(required_sections, names(config))

      if (length(missing_sections) > 0) {
        stop(
          sprintf(
            "Missing required configuration sections: %s",
            paste(missing_sections, collapse = ", ")
          )
        )
      }

      # Set up logging based on configuration
      log_threshold(config$logging$level)
      log_appender(appender_file(config$logging$file))

      # Convert the config to an environment for global access
      config_env <- new.env()
      config_env$CONFIG <- config
      attach(config_env)

      log_info("Configuration loaded successfully from {config_path}")
      config
    },
    error = function(e) {
      stop(sprintf("Failed to load configuration: %s", e$message))
    }
  )
}

#' Get a configuration value with dot notation
#' @param path Dot-separated path to configuration value
#' @param default Default value if path not found
#' @return Configuration value
get_config <- function(path, default = NULL) {
  if (!exists("CONFIG")) {
    stop("Configuration not loaded. Call load_config() first.")
  }

  parts <- strsplit(path, "\\.")[[1]]
  value <- CONFIG

  for (part in parts) {
    if (!is.list(value) || !part %in% names(value)) {
      return(default)
    }
    value <- value[[part]]
  }

  value
}

get_pbp_data <- function() {
  # Check if parquet file exists
  if (file.exists("nba_pbp_2025.parquet")) {
    df <- df_from_parquet("nba_pbp_2025.parquet")
  } else {
    tictoc::tic()
    progressr::with_progress({
      nba_pbp <- hoopR::load_nba_pbp(2025)
    })
    tictoc::toc()

    # Write to parquet for future use
    write_parquet(nba_pbp, "nba_pbp_2025.parquet")
    df <- df_from_parquet("nba_pbp_2025.parquet")
  }
  return(df)
}

get_free_throw_points <- function(pbp_data, game_id, after_sequence, team_id) {
  # Get subsequent free throws until we hit a non-free throw play
  subsequent_plays <- pbp_data |>
    filter(
      game_id == !!game_id,
      sequence_number > after_sequence,
      team_id == !!team_id
    ) |>
    arrange(sequence_number)

  ft_points <- 0
  for (i in 1:nrow(subsequent_plays)) {
    play <- subsequent_plays[i, ]
    # Safely check for free throws and scoring
    is_ft <- !is.na(play$type_text) && str_detect(play$type_text, "Free Throw")
    is_scoring <- !is.na(play$scoring_play) && play$scoring_play
    score_value <- if (is.null(play$score_value)) 0 else play$score_value

    if (is_ft && is_scoring) {
      ft_points <- ft_points + score_value
    } else if (!is_ft) {
      break
    }
  }

  return(ft_points)
}

#' Feature Version Control Class
#' @description R6 class for managing feature store versions
FeatureVersionControl <- R6::R6Class(
  "FeatureVersionControl",

  public = list(
    base_path = NULL,

    #' @description Initialize the version control system
    #' @param base_path Base directory for feature store
    initialize = function(base_path = "feature_store") {
      self$base_path <- base_path
      dir.create(
        file.path(base_path, "versions"),
        recursive = TRUE,
        showWarnings = FALSE
      )
      log_info("Initialized FeatureVersionControl at {base_path}")
    },

    #' @description Create a new version of features
    #' @param features_df Data frame of features
    #' @param metadata List of metadata
    #' @return Version hash
    create_version = function(features_df, metadata) {
      version_hash <- self$generate_version_hash(features_df, metadata)
      version_path <- file.path(self$base_path, "versions", version_hash)
      dir.create(version_path, recursive = TRUE, showWarnings = FALSE)

      # Save features and metadata
      write_parquet(features_df, file.path(version_path, "features.parquet"))
      write_json(
        metadata,
        file.path(version_path, "metadata.json"),
        auto_unbox = TRUE
      )

      # Update version registry
      self$update_registry(version_hash, metadata)
      log_info("Created new version: {version_hash}")

      version_hash
    },

    #' @description Generate version hash based on data and metadata
    #' @param features_df Data frame of features
    #' @param metadata List of metadata
    #' @return Hash string
    generate_version_hash = function(features_df, metadata) {
      data_hash <- digest(features_df, algo = "sha256")
      metadata_hash <- digest(metadata, algo = "sha256")
      digest(list(data_hash, metadata_hash), algo = "sha256")
    },

    #' @description Update version registry with new version
    #' @param version_hash Hash of the version
    #' @param metadata List of metadata
    update_registry = function(version_hash, metadata) {
      registry_path <- file.path(self$base_path, "version_registry.json")
      registry <- if (file.exists(registry_path)) {
        fromJSON(registry_path)
      } else {
        list(versions = list())
      }

      registry$versions[[version_hash]] <- list(
        timestamp = as.character(now()),
        metadata = metadata
      )

      write_json(registry, registry_path, auto_unbox = TRUE)
      log_info("Updated version registry with {version_hash}")
    },

    #' @description Get version information
    #' @param version_hash Hash of the version to retrieve
    #' @return List of version information
    get_version = function(version_hash) {
      version_path <- file.path(self$base_path, "versions", version_hash)
      if (!dir.exists(version_path)) {
        log_error("Version not found: {version_hash}")
        return(NULL)
      }

      list(
        features = read_parquet(file.path(version_path, "features.parquet")),
        metadata = fromJSON(file.path(version_path, "metadata.json"))
      )
    },

    #' @description List all versions
    #' @return Data frame of version information
    list_versions = function() {
      registry_path <- file.path(self$base_path, "version_registry.json")
      if (!file.exists(registry_path)) {
        log_warn("Version registry not found")
        return(data.frame())
      }

      registry <- fromJSON(registry_path)
      as_tibble(registry$versions) |>
        mutate(version_hash = names(registry$versions))
    }
  )
)

#' Prepare turnover plays data
#' @param pbp_data Play-by-play data frame
#' @param run_id Current run ID
#' @return Data frame of prepared turnover plays
prepare_turnover_plays <- function(pbp_data, run_id) {
  live_ball_turnovers <- get_config("turnover_rules.live_ball_types")

  pbp_data |>
    filter(type_text %in% live_ball_turnovers) |>
    mutate(
      receiving_team_id = determine_receiving_team(
        home_team_id,
        away_team_id,
        team_id
      ),
      run_id = run_id,
      ingestion_timestamp = now(),
      data_partition = create_date_partition(game_date)
    )
}

#' Determine receiving team for turnover
#' @param home_id Home team ID
#' @param away_id Away team ID
#' @param team_id Current team ID
#' @return Receiving team ID
determine_receiving_team <- function(home_id, away_id, team_id) {
  case_when(
    team_id == home_id ~ away_id,
    team_id == away_id ~ home_id,
    TRUE ~ NA_integer_
  )
}

#' Create date partition string
#' @param date Date to partition
#' @return Partition string in format "YYYY/MM/DD"
create_date_partition <- function(date) {
  paste(
    year(date),
    sprintf("%02d", month(date)),
    sprintf("%02d", day(date)),
    sep = "/"
  )
}

#' Process play sequence to determine outcome
#' @param current_play Current turnover play
#' @param pbp_data Full play-by-play data
#' @return Processed play with outcome information
process_play_sequence <- function(current_play, pbp_data) {
  next_play <- find_next_play(pbp_data, current_play)

  if (nrow(next_play) == 0) {
    return(create_empty_outcome(current_play))
  }

  points_scored <- calculate_points_scored(
    next_play,
    pbp_data,
    current_play$receiving_team_id
  )
  outcome <- determine_outcome(
    next_play,
    current_play$receiving_team_id,
    points_scored
  )

  enrich_play_features(current_play, next_play, outcome, points_scored)
}

#' Find next play in sequence
#' @param pbp_data Play-by-play data
#' @param current_play Current play
#' @return Next play data frame
find_next_play <- function(pbp_data, current_play) {
  pbp_data |>
    filter(
      game_id == current_play$game_id,
      sequence_number > current_play$sequence_number
    ) |>
    arrange(sequence_number) |>
    slice(1)
}

#' Enrich play with additional features
#' @param current_play Current play
#' @param next_play Next play in sequence
#' @param outcome Determined outcome
#' @param points_scored Points scored on play
#' @return Enriched play data
enrich_play_features <- function(
  current_play,
  next_play,
  outcome,
  points_scored
) {
  clutch_period <- get_config("game_rules.clutch_time.min_period")
  clutch_seconds <- get_config("game_rules.clutch_time.seconds_threshold")

  current_play |>
    mutate(
      # Training eligibility - UPDATED LOGIC
      is_training_eligible = case_when(
        # Games from exactly 2 days ago are prediction only
        game_date == (today() - days(2)) ~ FALSE,
        # All other dates are training eligible
        TRUE ~ TRUE
      ),

      # Standard play features
      next_play_type = next_play$type_text,
      next_play_team = next_play$team_id,
      next_play_score = points_scored,
      next_play_shooting = next_play$shooting_play,
      outcome = outcome,
      time_to_next_play = calculate_time_between_plays(current_play, next_play),

      # Game context features
      minutes_remaining = clock_minutes,
      seconds_remaining = clock_seconds,
      period = period_number,
      time_remaining_in_period = start_quarter_seconds_remaining,
      score_difference = calculate_score_difference(
        team_id,
        home_team_id,
        home_score,
        away_score
      ),
      is_clutch_time = is_clutch_situation(
        period_number,
        start_quarter_seconds_remaining,
        clutch_period,
        clutch_seconds
      )
    )
}

#' Calculate time between plays
#' @param current_play Current play
#' @param next_play Next play
#' @return Time difference in seconds
calculate_time_between_plays <- function(current_play, next_play) {
  current_play$start_game_seconds_remaining -
    next_play$start_game_seconds_remaining
}

#' Calculate score difference from team perspective
#' @param team_id Current team ID
#' @param home_id Home team ID
#' @param home_score Home team score
#' @param away_score Away team score
#' @return Score difference from team perspective
calculate_score_difference <- function(
  team_id,
  home_id,
  home_score,
  away_score
) {
  case_when(
    team_id == home_id ~ home_score - away_score,
    TRUE ~ away_score - home_score
  )
}

#' Determine if play is in clutch situation
#' @param period Current period
#' @param time_remaining Time remaining in period
#' @param clutch_period Clutch period threshold
#' @param clutch_seconds Clutch seconds threshold
#' @return Boolean indicating clutch situation
is_clutch_situation <- function(
  period,
  time_remaining,
  clutch_period,
  clutch_seconds
) {
  period >= clutch_period & time_remaining <= clutch_seconds
}

#' Extract turnover features with chunking
#' @param pbp_data Play-by-play data frame
#' @param current_run_id Current run ID
#' @return Processed features data frame
extract_turnover_features <- function(pbp_data, current_run_id = NULL) {
  current_run_id <- current_run_id %||% format(Sys.time(), "%Y%m%d_%H%M%S")

  # Initialize version control
  vcs <- FeatureVersionControl$new(get_config("paths.base_dir"))

  # Prepare initial turnover plays
  turnover_plays <- prepare_turnover_plays(pbp_data, current_run_id)

  # Process in chunks
  processed_features <- process_in_chunks(turnover_plays, pbp_data)

  # Create version and metadata
  metadata <- create_feature_metadata(processed_features, current_run_id)
  version_hash <- vcs$create_version(processed_features, metadata)

  # Return features with version
  processed_features |>
    mutate(version_hash = version_hash) |>
    filter(
      outcome != "other",
      !is.na(next_play_score),
      next_play_score <= get_config("turnover_rules.max_valid_score")
    )
}

#' Process data in chunks
#' @param turnover_plays Prepared turnover plays
#' @param pbp_data Full play-by-play data
#' @return Processed features
process_in_chunks <- function(turnover_plays, pbp_data) {
  chunk_size <- get_config("feature_store.chunk_size")
  chunks <- split(
    1:nrow(turnover_plays),
    ceiling(seq_along(1:nrow(turnover_plays)) / chunk_size)
  )

  # Process chunks in parallel
  processed_chunks <- future_map(
    chunks,
    function(chunk_indices) {
      chunk_plays <- turnover_plays[chunk_indices, ]
      map_df(
        1:nrow(chunk_plays),
        ~process_play_sequence(chunk_plays[.x, ], pbp_data)
      )
    },
    .progress = TRUE
  )

  bind_rows(processed_chunks)
}

#' Create feature metadata
#' @param features Processed features
#' @param run_id Current run ID
#' @return Metadata list
create_feature_metadata <- function(features, run_id) {
  list(
    run_id = run_id,
    feature_version = get_config("feature_store.version"),
    timestamp = now(),
    n_games = n_distinct(features$game_id),
    n_features = ncol(features),
    data_range = list(
      start = min(features$game_date),
      end = max(features$game_date)
    )
  )
}

#' Calculate points scored for a play
calculate_points_scored <- function(next_play, pbp_data, receiving_team_id) {
  if (!is.na(next_play$type_text) && str_detect(next_play$type_text, "Foul")) {
    get_free_throw_points(
      pbp_data,
      next_play$game_id,
      next_play$sequence_number,
      receiving_team_id
    )
  } else {
    if (is.null(next_play$score_value) || is.na(next_play$score_value)) 0 else
      next_play$score_value
  }
}

#' Determine outcome of a turnover
determine_outcome <- function(next_play, receiving_team_id, points_scored) {
  case_when(
    next_play$team_id == receiving_team_id & points_scored > 0 ~ "score",
    next_play$team_id != receiving_team_id &
      str_detect(next_play$type_text, "Foul") ~
      "foul",
    next_play$team_id == receiving_team_id &
      str_detect(next_play$type_text, "Shot|Layup|Dunk|Turnover|Traveling") &
      (is.na(next_play$scoring_play) || !next_play$scoring_play) ~
      "stop",
    str_detect(next_play$type_text, "Substitution|End Period|Timeout") ~
      "stoppage",
    TRUE ~ "other"
  )
}

#' Create empty outcome for end of game
create_empty_outcome <- function(current_play) {
  current_play |>
    mutate(
      next_play_type = NA_character_,
      next_play_team = NA_integer_,
      next_play_score = NA_integer_,
      next_play_shooting = NA,
      outcome = "other",
      time_to_next_play = NA_real_
    )
}

#' Create processed play with features
create_processed_play <- function(
  current_play,
  next_play,
  outcome,
  points_scored
) {
  current_play |>
    mutate(
      next_play_type = next_play$type_text,
      next_play_team = next_play$team_id,
      next_play_score = points_scored,
      next_play_shooting = next_play$shooting_play,
      outcome = outcome,
      time_to_next_play = current_play$start_game_seconds_remaining -
        next_play$start_game_seconds_remaining,

      # Additional features
      minutes_remaining = clock_minutes,
      seconds_remaining = clock_seconds,
      period = period_number,
      time_remaining_in_period = start_quarter_seconds_remaining,

      score_difference = case_when(
        team_id == home_team_id ~ home_score - away_score,
        team_id == away_team_id ~ away_score - home_score,
        TRUE ~ NA_real_
      ),

      is_clutch_time = (
        period >= CONSTANTS$CLUTCH_TIME_PERIOD &
          time_remaining_in_period <= CONSTANTS$CLUTCH_TIME_SECONDS
      )
    )
}

#' Get feature store statistics
#' @param features_df Features data frame
#' @return Statistics summary
get_feature_store_stats <- function(features_df) {
  features_df |>
    group_by(version_hash) |>
    summarise(
      total_games = n_distinct(game_id),
      total_turnovers = n(),
      earliest_game = min(game_date),
      latest_game = max(game_date),
      n_training_eligible = sum(is_training_eligible, na.rm = TRUE),
      n_prediction_eligible = sum(!is_training_eligible, na.rm = TRUE),
      .groups = "drop"
    )
}

analyze_outcomes <- function(features_df) {
  # Create list to store analysis results
  analysis_results <- list()

  # Overall outcome distribution
  analysis_results$overall_distribution <- features_df |>
    count(outcome) |>
    mutate(
      pct = n / sum(n) * 100,
      pct = round(pct, 1)
    ) |>
    arrange(desc(n))

  log_info("Calculated overall outcome distribution")

  # Outcome distribution in clutch time
  analysis_results$clutch_distribution <- features_df |>
    count(is_clutch_time, outcome) |>
    group_by(is_clutch_time) |>
    mutate(
      pct = n / sum(n) * 100,
      pct = round(pct, 1)
    ) |>
    arrange(is_clutch_time, desc(n))

  log_info("Calculated clutch time outcome distribution")

  # Points distribution
  analysis_results$points_distribution <- features_df |>
    count(next_play_score) |>
    mutate(
      pct = n / sum(n) * 100,
      pct = round(pct, 1)
    ) |>
    arrange(desc(n))

  log_info("Calculated points distribution")

  # Average points by outcome
  analysis_results$points_by_outcome <- features_df |>
    group_by(outcome) |>
    summarise(
      avg_points = mean(next_play_score, na.rm = TRUE),
      n = n(),
      .groups = "drop"
    ) |>
    arrange(desc(n))

  log_info("Calculated average points by outcome")

  # Print summary if requested
  if (get_config("logging.print_analysis", default = TRUE)) {
    log_info("\nAnalysis Results:")
    log_info("\nOverall outcome distribution:")
    print(analysis_results$overall_distribution)

    log_info("\nOutcome distribution in clutch time vs non-clutch:")
    print(analysis_results$clutch_distribution)

    log_info("\nPoints distribution following turnovers:")
    print(analysis_results$points_distribution)

    log_info("\nAverage points by outcome:")
    print(analysis_results$points_by_outcome)
  }

  return(analysis_results)
}

#' Get eligibility counts from most recent version
#' @description Gets training and prediction eligibility counts from most recent version
#' @return Named list with eligibility counts and version info
get_latest_eligibility_counts <- function() {
  base_path <- get_config("paths.base_dir")
  versions_dir <- file.path(base_path, "versions")

  # Check if versions directory exists
  if (!dir.exists(versions_dir)) {
    log_warn("No versions directory found at: {versions_dir}")
    return(NULL)
  }

  # Get all version directories
  version_dirs <- list.dirs(versions_dir, full.names = TRUE, recursive = FALSE)
  if (length(version_dirs) == 0) {
    log_warn("No versions found in: {versions_dir}")
    return(NULL)
  }

  # Get most recent version based on directory modification time
  latest_version <- version_dirs[which.max(file.info(version_dirs)$mtime)]
  version_hash <- basename(latest_version)

  # Read features parquet file
  features_path <- file.path(latest_version, "features.parquet")
  if (!file.exists(features_path)) {
    log_error("Features file not found in latest version: {features_path}")
    return(NULL)
  }

  features_df <- read_parquet(features_path)

  log_info("The features are {names(features_df)}")

  # Calculate eligibility counts
  eligibility_counts <- features_df |>
    summarise(
      training_eligible = sum(is_training_eligible, na.rm = TRUE),
      prediction_eligible = sum(!is_training_eligible, na.rm = TRUE),
      total_rows = n()
    )

  # Read metadata
  metadata_path <- file.path(latest_version, "metadata.json")
  metadata <- if (file.exists(metadata_path)) {
    fromJSON(metadata_path)
  } else {
    NULL
  }

  # Return results
  list(
    counts = eligibility_counts,
    version_hash = version_hash,
    timestamp = metadata$timestamp,
    run_id = metadata$run_id
  )
}

#' Get training eligible data from latest version
#' @param version_hash Optional specific version hash
#' @return Training eligible data frame
get_training_data <- function(version_hash = NULL) {
  base_path <- get_config("paths.base_dir")

  if (is.null(version_hash)) {
    # Get most recent version
    versions_dir <- file.path(base_path, "versions")
    version_dirs <- list.dirs(
      versions_dir,
      full.names = TRUE,
      recursive = FALSE
    )
    latest_version <- version_dirs[which.max(file.info(version_dirs)$mtime)]
    version_hash <- basename(latest_version)
  }

  features_path <- file.path(
    base_path,
    "versions",
    version_hash,
    "features.parquet"
  )

  if (!file.exists(features_path)) {
    stop("Features file not found: ", features_path)
  }

  # Read and filter for training eligible rows
  features_df <- read_parquet(features_path) |>
    filter(is_training_eligible)

  log_info("The features are {names(features_df)}")

  log_info(
    "Retrieved {nrow(features_df)} training eligible rows from version {version_hash}"
  )
  features_df
}

#' Train linear regression model using tidymodels
#' @param data Training data frame
#' @param outcome_col Name of outcome column
#' @param seed Random seed for reproducibility
#' @return Trained workflow
train_linear_model <- function(
  data,
  outcome_col = "next_play_score",
  seed = 33
) {
  data_reduced <- data |>
    select(
      game_id,
      home_team_name,
      away_team_name,
      away_score,
      home_score,
      period_number,
      end_quarter_seconds_remaining,
      coordinate_x,
      coordinate_y,
      next_play_score
    ) |>
    mutate(
      across(
        where(is.character),
        as.factor
      )
    )

  # Set seed for reproducibility
  set.seed(seed)

  # Create train/test split
  data_split <- initial_split(
    data_reduced,
    prop = 0.8,
    strata = next_play_score
  )
  train_data <- training(data_split)
  test_data <- testing(data_split)

  # Create cross-validation folds
  folds <- vfold_cv(train_data, v = 5, strata = next_play_score)

  # Define model spec
  lm_spec <- linear_reg(
    penalty = tune(),
    mixture = 1
  ) |>
    set_engine("glmnet")

  # Create recipe
  model_recipe <- recipe(
    next_play_score ~ .,
    data = train_data
  ) |>
    # Remove ID/metadata columns
    update_role(
      c(game_id, home_team_name, away_team_name),
      new_role = "id"
    ) |>
    # Remove zero variance predictors
    step_zv() |>
    # Standardize numeric predictors
    step_normalize(all_numeric_predictors())

  # Create workflow
  lm_workflow <- workflow() |>
    add_model(lm_spec) |>
    add_recipe(model_recipe)

  # Define tuning grid
  penalty_grid <- grid_regular(
    penalty(range = c(-4, 0)), # log10 scale from 0.0001 to 1
    levels = 10
  )

  # Tune model
  log_info("Beginning model tuning with 5-fold CV")
  tuning_results <- tune_grid(
    lm_workflow,
    resamples = folds,
    grid = penalty_grid,
    metrics = metric_set(rmse, mae, rsq)
  )

  # Select best model
  best_penalty <- select_best(tuning_results, metric = "rmse")

  # Finalize workflow with best parameters
  final_workflow <- finalize_workflow(
    lm_workflow,
    best_penalty
  )

  # Fit final model
  log_info("Fitting final model with best parameters")
  final_fit <- fit(final_workflow, train_data)

  # Calculate test metrics
  test_metrics <- augment(final_fit, test_data) |>
    metrics(truth = !!sym(outcome_col), estimate = .pred)

  log_info("Test set metrics: {test_metrics}")

  # Return results
  list(
    workflow = final_fit,
    tuning_results = tuning_results,
    test_metrics = test_metrics,
    train_data = train_data,
    test_data = test_data
  )
}

#' Run full model training pipeline
#' @param version_hash Optional specific version hash
#' @param seed Random seed
#' @return Training results
run_model_training <- function(version_hash = NULL, seed = 33) {
  # Get training data
  training_data <- get_training_data(version_hash)

  # Train model
  results <- train_linear_model(
    data = training_data,
    outcome_col = "next_play_score",
    seed = seed
  )

  # Save model artifacts
  save_model_artifacts(results)

  results
}

#' Save model artifacts
#' @param results Model training results
#' @return Path to saved artifacts
save_model_artifacts <- function(results) {
  run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  base_path <- get_config("paths.base_dir")
  models_dir <- file.path(base_path, "models", run_id)
  dir.create(models_dir, recursive = TRUE, showWarnings = FALSE)

  # Save workflow
  workflow_path <- file.path(models_dir, "workflow.rds")
  saveRDS(results$workflow, workflow_path)

  # Save metrics and tuning results
  metrics_path <- file.path(models_dir, "metrics.rds")
  saveRDS(
    list(
      tuning_results = results$tuning_results,
      test_metrics = results$test_metrics
    ),
    metrics_path
  )

  log_info("Saved model artifacts to: {models_dir}")
  models_dir
}
