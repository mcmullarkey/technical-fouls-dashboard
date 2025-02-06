# Source packages first
source("packages.R")

#' Main execution function for feature pipeline
#' @param config_path Path to configuration file
#' @return List containing features and statistics
main <- function(config_path = "config.yml", static_only = FALSE) {
  Sys.setenv(TZ = "America/Denver")
  run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")

  initialize_s3()

  tryCatch({
    # Initialize configuration and logging
    config <- load_config(config_path)
    log_info("Starting pipeline. Run ID: {run_id}")

    if (static_only) {
      log_info("Running in static generation mode...")

      # Load latest offense metrics directly
      offense_metrics <- load_latest_offense_metrics()

      if (!is.null(offense_metrics)) {
        log_info("Creating team performance visualization...")
        table_result <- create_team_performance_table(offense_metrics)

        if (!is.null(table_result)) {
          log_info("Successfully created team performance visualization")
          return(list(status = "success", message = "Static content generated"))
        } else {
          log_error("Failed to create team performance visualization")
          return(
            list(status = "error", message = "Failed to generate visualization")
          )
        }
      } else {
        log_error("No metrics data available")
        return(list(status = "error", message = "No metrics data available"))
      }
    }
  })

  tryCatch(
    {
      # Initialize configuration and logging
      config <- load_config(config_path)
      log_info("Starting feature extraction pipeline. Run ID: {run_id}")

      # Initialize parallel processing
      workers <- get_config("feature_store.max_parallel_workers")
      plan(multisession, workers = workers)
      log_info("Initialized parallel processing with {workers} workers")

      # Get play-by-play data
      log_info("Debug: About to fetch play-by-play data")
      df <- get_pbp_data()
      log_info("Debug: Play-by-play data fetched successfully")
      log_info(
        "Retrieved {nrow(df)} plays across {n_distinct(df$game_id)} games"
      )

      # Extract features
      log_info("Debug: Beginning feature extraction")
      features <- extract_turnover_features(df, run_id)
      log_info("Debug: Features extracted successfully")
      log_info("Extracted features for {nrow(features)} turnover events")

      # Save version to S3
      log_info("Debug: About to call save_version_to_s3")
      version_hash <- save_version_to_s3(features, run_id)
      log_info("Debug: save_version_to_s3 completed successfully")

      # Run model training with current features
      log_info("Debug: About to run model training")
      model_results <- run_model_training()
      log_info("Debug: Model training completed")

      # Generate prediction report with current features
      log_info("Debug: About to generate prediction report")
      report <- generate_prediction_report()
      log_info("Debug: Prediction report generated")

      log_info("Debug: About to save prediction report")
      save_prediction_report(report)
      log_info("Debug: Prediction report saved")

      # Explicitly create team performance visualization
      log_info("Debug: About to create team performance visualization")
      if (!is.null(report) && !is.null(report$team_metrics)) {
        log_info("Creating team performance visualization...")
        table_result <- create_team_performance_table(
          report$team_metrics$offense
        )
        if (!is.null(table_result)) {
          log_info("Successfully created team performance visualization")
        } else {
          log_error("Failed to create team performance visualization")
        }
      } else {
        log_warn("No team metrics available for visualization")
      }

      # Create pipeline results
      results <- list(
        run_id = run_id,
        version_hash = version_hash,
        features = features,
        stats = get_feature_store_stats(features),
        outcome_analysis = analyze_outcomes(features),
        execution_time = difftime(
          Sys.time(),
          as.POSIXct(run_id, format = "%Y%m%d_%H%M%S")
        )
      )

      # Save run results to S3
      log_info("Saving run results to S3")
      results_path <- paste0("runs/", run_id, "/results.rds")
      save_to_s3(results, results_path)

      log_success("Pipeline completed successfully in {results$execution_time}")

      results
    },
    error = function(e) {
      log_error("Pipeline failed: {conditionMessage(e)}")
      print(e)
      stop(e)
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
  tictoc::tic()
  progressr::with_progress({
    nba_pbp <- hoopR::load_nba_pbp(2025)
  })
  tictoc::toc()

  # Write to parquet for future use
  write_parquet(nba_pbp, "nba_pbp_2025.parquet")
  df <- df_from_parquet("nba_pbp_2025.parquet")

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
        game_date == (today() - days(1)) ~ FALSE,
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
get_training_data <- function(version_hash = NULL, current_features = NULL) {
  # If current features provided, use those
  if (!is.null(current_features)) {
    log_info("Using current run features for training (first run)")
    return(current_features %>% filter(is_training_eligible))
  }

  base_path <- get_config("paths.base_dir")

  if (is.null(version_hash)) {
    # Get most recent version
    versions_dir <- file.path(base_path, "versions")
    version_dirs <- list.dirs(
      versions_dir,
      full.names = TRUE,
      recursive = FALSE
    )

    if (length(version_dirs) == 0) {
      log_warn("No version directories found")
      return(data.frame())
    }

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
    log_warn("Features file not found: {features_path}")
    return(data.frame())
  }

  # Read and filter for training eligible rows
  features_df <- read_parquet(features_path) %>%
    filter(is_training_eligible)

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
      c(
        game_id,
        home_team_name,
        away_team_name
      ),
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

#' @param results Model training results
#' @return Path to saved artifacts
save_model_artifacts <- function(results) {
  bucket <- "carbonite-bucket" # Make sure this matches your actual bucket name
  run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  s3_prefix <- paste0("models/", run_id, "/")

  # Debug logging
  log_info("Attempting to save artifacts with prefix: {s3_prefix}")

  # Save workflow
  workflow_path <- paste0(s3_prefix, "workflow.rds")
  log_info("Saving workflow to: {workflow_path}")
  save_to_s3(results$workflow, workflow_path)

  # Save metrics and tuning results
  metrics_path <- paste0(s3_prefix, "metrics.rds")
  log_info("Saving metrics to: {metrics_path}")
  save_to_s3(
    list(
      tuning_results = results$tuning_results,
      test_metrics = results$test_metrics
    ),
    metrics_path
  )

  # Verify the files were saved
  model_objects <- aws.s3::get_bucket(
    bucket = bucket,
    prefix = s3_prefix
  )
  log_info(
    "Found {length(model_objects)} objects saved with prefix {s3_prefix}"
  )

  log_info("Saved model artifacts to S3: {s3_prefix}")
  s3_prefix
}

#' Calculate team performance metrics
#' @param data Data frame with predictions and actuals
#' @return Data frame of team metrics
calculate_team_metrics <- function(data) {
  # Calculate point differential (actual - predicted)
  predictions_with_diff <- data |>
    mutate(
      points_vs_expected = next_play_score - .pred,
      abs_error = abs(next_play_score - .pred)
    ) |>
    # Create proper team name mapping
    mutate(
      # For the offensive team (team_id)
      offense_team_name = case_when(
        team_id == home_team_id ~ home_team_name,
        team_id == away_team_id ~ away_team_name,
        TRUE ~ NA_character_
      ),
      # For the defensive team (receiving_team_id)
      defense_team_name = case_when(
        receiving_team_id == home_team_id ~ home_team_name,
        receiving_team_id == away_team_id ~ away_team_name,
        TRUE ~ NA_character_
      )
    )

  # Calculate metrics for offense (team causing turnover)
  offense_metrics <- predictions_with_diff |>
    group_by(team_id, team_name = offense_team_name) |>
    summarise(
      n_turnovers = n(),
      avg_points_after = mean(next_play_score, na.rm = TRUE),
      avg_predicted_points = mean(.pred, na.rm = TRUE),
      points_vs_expected = mean(points_vs_expected, na.rm = TRUE),
      mae = mean(abs_error, na.rm = TRUE),
      total_points_vs_expected = sum(points_vs_expected, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(points_vs_expectation = total_points_vs_expected * n_turnovers) |>
    arrange(desc(points_vs_expectation))

  # Calculate metrics for defense (receiving team)
  defense_metrics <- predictions_with_diff |>
    group_by(receiving_team_id, defense_team = defense_team_name) |>
    summarise(
      n_turnovers_forced = n(),
      avg_points_allowed = mean(next_play_score, na.rm = TRUE),
      avg_predicted_points_allowed = mean(.pred, na.rm = TRUE),
      points_vs_expected_defense = mean(points_vs_expected, na.rm = TRUE),
      mae_defense = mean(abs_error, na.rm = TRUE),
      total_points_vs_expected_defense = sum(points_vs_expected, na.rm = TRUE),
      .groups = "drop"
    ) |>
    arrange(desc(total_points_vs_expected_defense))

  # Add validation checks
  if (any(duplicated(offense_metrics$team_id))) {
    log_warn("Duplicate team IDs found in offense metrics")
  }
  if (any(duplicated(defense_metrics$receiving_team_id))) {
    log_warn("Duplicate team IDs found in defense metrics")
  }

  # Return both sets of metrics
  list(
    offense = offense_metrics,
    defense = defense_metrics
  )
}

#' Generate prediction report and visualization
#' @return Metrics data
generate_prediction_report <- function(current_features = NULL) {
  log_info("Starting prediction performance analysis")

  # Get latest workflow and prediction data
  workflow <- get_latest_workflow()
  pred_data <- get_prediction_data()

  if (nrow(pred_data) == 0) {
    log_warn("No prediction-eligible data found")
    return(NULL)
  }

  # Prepare data for prediction
  log_info("Preparing data for predictions...")
  pred_data_reduced <- pred_data |>
    select(
      game_id,
      team_id,
      receiving_team_id,
      home_team_id,
      away_team_id,
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

  # Generate predictions
  log_info("Generating predictions for {nrow(pred_data_reduced)} plays")
  predictions <- augment(workflow, new_data = pred_data_reduced)

  # Calculate metrics
  log_info("Calculating team metrics...")
  team_metrics <- calculate_team_metrics(predictions)
  overall_metrics <- predictions |>
    metrics(truth = next_play_score, estimate = .pred)

  # Log summary statistics
  log_info("\nOverall prediction metrics:")
  print(overall_metrics)

  if (!is.null(team_metrics$offense) && nrow(team_metrics$offense) > 0) {
    log_info("\nTop 5 teams by points vs expected:")
    print(
      head(
        team_metrics$offense[,
          c("team_name", "points_vs_expectation", "n_turnovers")
        ],
        5
      )
    )

    # Return metrics for further use
    list(
      team_metrics = team_metrics,
      overall_metrics = overall_metrics
    )
  } else {
    log_warn("No team metrics calculated")
    NULL
  }
}

#' Get latest model workflow from S3
#' @return Latest model workflow
get_latest_workflow <- function() {
  bucket <- "carbonite-bucket" # Make consistent with save function

  # Debug logging
  log_info("Searching for models in bucket: {bucket}")

  # List all objects in models directory
  model_objects <- aws.s3::get_bucket(
    bucket = bucket,
    prefix = "models/"
  )

  # Debug logging
  log_info("Found {length(model_objects)} total objects in models/ directory")
  if (length(model_objects) > 0) {
    log_info("First few objects found:")
    for (i in seq_len(min(3, length(model_objects)))) {
      log_info(
        "  {i}. {model_objects[[i]]$Key} (Modified: {model_objects[[i]]$LastModified})"
      )
    }
  }

  if (length(model_objects) == 0) {
    stop("No model objects found in S3 bucket")
  }

  # Find most recent workflow
  workflow_objects <- Filter(
    function(x) grepl("workflow\\.rds$", x$Key),
    model_objects
  )

  # Debug logging
  log_info("Found {length(workflow_objects)} workflow.rds files")

  if (length(workflow_objects) == 0) {
    stop("No workflow files found in S3 bucket")
  }

  # Convert LastModified strings to POSIXct timestamps and handle missing values
  timestamps <- sapply(workflow_objects, function(x) {
    if (is.null(x$LastModified)) return(as.POSIXct(NA))
    # Parse the ISO 8601 timestamp
    tryCatch(
      {
        as.POSIXct(x$LastModified, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")
      },
      error = function(e) {
        log_warn("Could not parse timestamp for {x$Key}: {x$LastModified}")
        as.POSIXct(NA)
      }
    )
  })

  # Find the index of the most recent non-NA timestamp
  valid_timestamps <- which(!is.na(timestamps))
  if (length(valid_timestamps) == 0) {
    stop("No valid timestamps found for workflow files")
  }

  latest_idx <- valid_timestamps[which.max(timestamps[valid_timestamps])]
  latest_workflow <- workflow_objects[[latest_idx]]

  log_info("Loading most recent workflow from S3: {latest_workflow$Key}")
  log_info("Last modified time: {latest_workflow$LastModified}")

  load_from_s3(latest_workflow$Key, type = "rds")
}

#' Save prediction report
#' @param report Prediction report from generate_prediction_report()
#' @return Path to saved report
save_prediction_report <- function(report) {
  run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  s3_prefix <- paste0("prediction_reports/", run_id, "/")

  # Save full report
  save_to_s3(report, paste0(s3_prefix, "prediction_report.rds"))

  # Save CSVs
  save_to_s3(
    report$team_metrics$offense,
    paste0(s3_prefix, "offense_metrics.csv")
  )
  save_to_s3(
    report$team_metrics$defense,
    paste0(s3_prefix, "defense_metrics.csv")
  )
  save_to_s3(report$overall_metrics, paste0(s3_prefix, "overall_metrics.csv"))

  log_info("Saved prediction report to S3: {s3_prefix}")
  s3_prefix
}

#' Load latest offense metrics from S3
#' @return Data frame of offense metrics
load_latest_offense_metrics <- function(current_metrics = NULL) {
  if (!is.null(current_metrics)) {
    log_info("Using current run metrics")
    return(current_metrics)
  }

  bucket <- Sys.getenv("AWS_S3_BUCKET")

  # First try to get the most recent prediction report
  report_objects <- aws.s3::get_bucket(
    bucket = bucket,
    prefix = "prediction_reports/"
  )

  if (length(report_objects) > 0) {
    # Find most recent offense metrics
    metrics_objects <- Filter(
      function(x) grepl("offense_metrics\\.csv$", x$Key),
      report_objects
    )

    if (length(metrics_objects) > 0) {
      latest_metrics <- metrics_objects[[
        which.max(sapply(metrics_objects, function(x) x$LastModified))
      ]]

      # Load the metrics
      temp_file <- tempfile()
      aws.s3::save_object(latest_metrics$Key, temp_file, bucket)
      metrics_data <- read.csv(temp_file) %>%
        mutate(team = str_trim(team_name))
      unlink(temp_file)

      return(metrics_data)
    }
  }

  # Fallback: generate metrics from latest features
  log_info("No recent metrics found, generating from latest features...")
  latest_features <- get_prediction_data(days_back = 1)

  if (nrow(latest_features) > 0) {
    workflow <- get_latest_workflow()
    if (!is.null(workflow)) {
      predictions <- augment(workflow, new_data = latest_features)
      team_metrics <- calculate_team_metrics(predictions)
      return(team_metrics$offense)
    }
  }

  log_warn("Could not load or generate metrics")
  NULL
}

#' Create and save team performance visualization
#' @param data Team metrics data
#' @return Path to saved HTML file
create_team_performance_table <- function(data) {
  log_info("Starting team performance table creation...")

  tryCatch(
    {
      # Get team data from hoopR with logos
      log_info("Fetching team data from hoopR...")
      nba_teams <- hoopR::nba_teams() |>
        select(
          team_id,
          team_name,
          team_abbreviation,
          team_city,
          nba_logo_svg
        )

      # Merge data with logos
      log_info("Merging team data with performance metrics...")
      data_with_logos <- data |>
        left_join(nba_teams, by = c("team_name" = "team_city")) |>
        arrange(desc(points_vs_expectation))

      # Store logos vector
      logos_vector <- data_with_logos$nba_logo_svg

      # Create the visualization with explicit output capture
      log_info("Creating table visualization...")
      table_html <- data_with_logos |>
        select(
          nba_logo_svg,
          points_vs_expectation,
          n_turnovers,
          avg_points_after,
          avg_predicted_points
        ) |>
        tt(
          notes = sprintf(
            "Data source: NBA play by play data via hoopR (Updated: %s)",
            format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
          )
        ) |>
        plot_tt(images = logos_vector, j = 1, height = 4) |>
        format_tt(j = 1:5, digits = 2, num_fmt = "decimal", num_zero = TRUE) |>
        style_tt(j = 1:5, align = "c", fontsize = 1.5) |>
        style_tt(i = 0, color = "white", background = "black") |>
        style_tt(i = -1, fontsize = 2.5) |>
        group_tt(
          j = list(
            "Who scored the most points off live turnovers vs. modeled expectations in the most recent games?" = 1:5
          )
        ) |>
        setNames(
          c(
            "Team",
            "Points vs. Predicted",
            "Live TO Forced",
            "PPP Off Live TO",
            "xPPP Off Live TO"
          )
        ) |>
        theme_tt("striped") |>
        save_tt("index.html", overwrite = TRUE)

      # # Explicitly capture the HTML output
      # log_info("Capturing HTML output...")
      # output_lines <- capture.output({
      #   print(table_html)
      # })
      #
      # # Add HTML wrapper elements
      # final_html <- c(
      #   "<!DOCTYPE html>",
      #   "<html>",
      #   "<head>",
      #   "<meta charset='UTF-8'>",
      #   "<title>NBA Team Performance</title>",
      #   "</head>",
      #   "<body>",
      #   output_lines,
      #   "</body>",
      #   "</html>"
      # )
      #
      # # Save the output
      # log_info("Saving HTML output...")
      # output_path <- save_html_output(final_html)
      #
      # if (!is.null(output_path)) {
      #   log_info("Successfully created and saved team performance table")
      #   return(output_path)
      # } else {
      #   log_error("Failed to save HTML output")
      #   return(NULL)
      # }
    },
    error = function(e) {
      log_error("Error in create_team_performance_table: {conditionMessage(e)}")
      return(NULL)
    }
  )
}

#' Save HTML output to file
#' @param html_content Vector of HTML content lines
#' @return Path to saved file or NULL if failed
save_html_output <- function(html_content) {
  output_path <- "/app/index.html"
  temp_path <- "/app/temp_index.html"

  log_info("Attempting to save HTML output...")
  log_info("Target path: {output_path}")
  log_info("Current working directory: {getwd()}")

  # Check directory permissions
  dir_info <- file.info("/app")
  log_info("Directory permissions: {dir_info$mode}")
  log_info("Directory owner: {dir_info$uname}")

  tryCatch(
    {
      # First write to temporary file
      log_info("Writing to temporary file: {temp_path}")
      writeLines(html_content, temp_path)

      if (!file.exists(temp_path)) {
        log_error("Failed to create temporary file")
        return(NULL)
      }

      # Check temporary file
      temp_content <- readLines(temp_path)
      log_info("Temporary file created with {length(temp_content)} lines")

      # Move temporary file to final location
      log_info("Moving temporary file to final location")
      file.copy(temp_path, output_path, overwrite = TRUE)
      unlink(temp_path)

      if (!file.exists(output_path)) {
        log_error("Final file was not created")
        return(NULL)
      }

      # Verify final file
      final_content <- readLines(output_path)
      log_info("Final file created with {length(final_content)} lines")
      log_info("Final file permissions: {file.info(output_path)$mode}")

      return(output_path)
    },
    error = function(e) {
      log_error("Error saving HTML output: {conditionMessage(e)}")
      if (file.exists(temp_path)) {
        unlink(temp_path)
      }
      return(NULL)
    }
  )
}

#' Initialize S3 bucket connection and create necessary paths
#' @return NULL
initialize_s3 <- function() {
  bucket <- "carbonite-bucket"
  region <- "us-west-2"

  # Remove the credential unsetting - let AWS SDK handle credentials
  # Remove forced credentials file usage

  # Set region if not already set
  if (Sys.getenv("AWS_DEFAULT_REGION") == "") {
    Sys.setenv("AWS_DEFAULT_REGION" = region)
  }

  # Configure aws.s3 package - simplified configuration
  options(
    "aws.s3.region" = region
  )

  log_info("Initialized S3 connection to bucket {bucket} in region {region}")

  # Verify access by listing bucket contents
  tryCatch(
    {
      # First try a simple head-bucket operation
      b_exists <- bucket_exists(bucket)
      log_info("Bucket exists check: {b_exists}")

      # Then try to list contents
      contents <- get_bucket(bucket)
      log_info("Successfully verified S3 access")
      log_info("Found {length(contents)} objects in bucket")

      # Print first object if any exist
      if (length(contents) > 0) {
        log_info("First object: {contents[[1]]$Key}")
      }
    },
    error = function(e) {
      log_error("Failed to access bucket: {conditionMessage(e)}")
      log_error("Error details:")
      print(e)
      stop(e)
    }
  )
}

save_version_to_s3 <- function(features_df, version_hash = NULL) {
  bucket <- "carbonite-bucket"

  # Generate version hash if not provided
  if (is.null(version_hash)) {
    version_hash <- format(Sys.time(), "%Y%m%d_%H%M%S")
  }

  # First create a marker file to establish the version directory
  marker_path <- sprintf("versions/%s/.version", version_hash)
  log_info("Creating version marker at {marker_path}")
  save_to_s3(list(version = version_hash), marker_path)

  # Now save the features
  features_path <- sprintf("versions/%s/features.parquet", version_hash)
  log_info("Saving features to {features_path}")
  save_to_s3(features_df, features_path)

  # Verify both files exist
  tryCatch(
    {
      marker_exists <- aws.s3::object_exists(bucket, marker_path)
      features_exist <- aws.s3::object_exists(bucket, features_path)
      log_info(
        "Verification - marker exists: {marker_exists}, features exist: {features_exist}"
      )

      # if (!marker_exists || !features_exist) {
      #   stop("Failed to verify saved files in S3")
      # }
    },
    error = function(e) {
      log_error("Error verifying saved version: {conditionMessage(e)}")
      stop(e)
    }
  )

  version_hash
}

list_versions <- function() {
  bucket <- "carbonite-bucket"

  log_info("Listing all objects in bucket...")
  all_objects <- aws.s3::get_bucket(bucket)
  log_info("Total objects in bucket: {length(all_objects)}")

  # Look specifically for version markers
  version_markers <- Filter(
    function(x) grepl("^versions/.+/\\.version$", x$Key),
    all_objects
  )

  log_info("Found {length(version_markers)} version markers")

  if (length(version_markers) == 0) {
    log_warn("No version markers found in S3")
    return(
      data.frame(
        version_hash = character(0),
        timestamp = as.POSIXct(character(0)),
        stringsAsFactors = FALSE
      )
    )
  }

  # Extract version info
  versions <- lapply(version_markers, function(marker) {
    # Extract version hash from path (versions/HASH/.version)
    version_hash <- strsplit(marker$Key, "/")[[1]][2]
    timestamp <- as.POSIXct(
      marker$LastModified,
      format = "%Y-%m-%dT%H:%M:%S",
      tz = "UTC"
    )

    data.frame(
      version_hash = version_hash,
      timestamp = timestamp,
      stringsAsFactors = FALSE
    )
  })

  result <- do.call(rbind, versions)
  result[order(result$timestamp, decreasing = TRUE), ]
}

#' Load object from S3
#' @param path S3 path
#' @param type Type of object ("parquet" or "rds")
#' @return Object loaded from S3
load_from_s3 <- function(path, type = "parquet") {
  bucket <- "carbonite-bucket"
  tmp_file <- tempfile(fileext = paste0(".", type))

  tryCatch(
    {
      save_object(
        object = path,
        file = tmp_file,
        bucket = bucket
      )

      if (type == "parquet") {
        object <- read_parquet(tmp_file)
      } else {
        object <- readRDS(tmp_file)
      }
      unlink(tmp_file)
      log_info("Successfully loaded object from s3://{bucket}/{path}")
      return(object)
    },
    error = function(e) {
      unlink(tmp_file)
      log_error("Failed to load from S3: {conditionMessage(e)}")
      print(e)
      stop(sprintf("Failed to load object from S3: %s", conditionMessage(e)))
    }
  )
}

#' List objects in S3 with prefix
#' @param prefix S3 prefix
#' @return Data frame of objects
list_objects_s3 <- function(prefix) {
  bucket <- "carbonite-bucket"

  tryCatch(
    {
      objects <- get_bucket(
        bucket = bucket,
        prefix = prefix
      )

      # Convert list to data frame
      if (length(objects) > 0) {
        data.frame(
          key = sapply(objects, function(x) x$Key),
          last_modified = sapply(objects, function(x) x$LastModified),
          size = sapply(objects, function(x) x$Size),
          stringsAsFactors = FALSE
        )
      } else {
        data.frame(
          key = character(0),
          last_modified = as.POSIXct(character(0)),
          size = numeric(0),
          stringsAsFactors = FALSE
        )
      }
    },
    error = function(e) {
      log_error("Failed to list S3 objects: {conditionMessage(e)}")
      print(e)
      stop(e)
    }
  )
}

#' Get prediction data from most recent version in S3
#' @param current_features Optional current features dataframe
#' @param days_back Number of days to look back for predictions if no current data
#' @return Prediction data frame or empty data frame if no data found
get_prediction_data <- function(current_features = NULL, days_back = 1) {
  bucket <- "carbonite-bucket"
  versions_prefix <- "versions/"

  # Create empty data frame with expected columns as fallback
  empty_df <- data.frame(
    game_id = character(),
    team_id = integer(),
    receiving_team_id = integer(),
    home_team_id = integer(),
    away_team_id = integer(),
    home_team_name = character(),
    away_team_name = character(),
    away_score = integer(),
    home_score = integer(),
    period_number = integer(),
    end_quarter_seconds_remaining = numeric(),
    coordinate_x = numeric(),
    coordinate_y = numeric(),
    next_play_score = numeric(),
    stringsAsFactors = FALSE
  )

  log_info("Getting prediction data, looking back {days_back} days if needed")

  # List all version directories in S3
  log_info("Listing version directories in S3...")
  version_objects <- tryCatch(
    {
      aws.s3::get_bucket(
        bucket = bucket,
        prefix = versions_prefix
      )
    },
    error = function(e) {
      log_error("Error listing S3 bucket: {conditionMessage(e)}")
      return(empty_df)
    }
  )

  if (length(version_objects) == 0) {
    log_error("No version objects found in S3")
    return(empty_df)
  }

  # Get unique version directories that contain features.parquet
  version_dirs <- unique(
    sapply(version_objects, function(x) {
      if (grepl("features\\.parquet$", x$Key)) {
        dirname(x$Key)
      }
    })
  )
  version_dirs <- version_dirs[!is.na(version_dirs)]

  if (length(version_dirs) == 0) {
    log_error("No versions with features.parquet found")
    return(empty_df)
  }

  # Get the latest version
  latest_version_dir <- version_dirs[
    which.max(as.numeric(gsub("[^0-9]", "", version_dirs)))
  ]
  log_info("Using latest version directory: {latest_version_dir}")

  # Load features
  features_path <- file.path(latest_version_dir, "features.parquet")
  log_info("Loading features from: {features_path}")

  features_df <- tryCatch(
    {
      df <- load_from_s3(features_path)

      if (is.null(df) || !is.data.frame(df) || nrow(df) == 0) {
        log_warn("Loaded features file is empty or invalid")
        return(empty_df)
      }

      log_info("Loaded {nrow(df)} total rows from features file")

      # First try to get prediction-eligible data
      pred_eligible <- df |>
        filter(!is_training_eligible) |>
        as.data.frame()

      if (nrow(pred_eligible) > 0) {
        log_info("Found {nrow(pred_eligible)} prediction-eligible rows")
        return(pred_eligible)
      }

      # If no prediction-eligible data, get recent historical data
      log_info(
        "No prediction-eligible data found, getting recent historical data..."
      )
      recent_data <- df %>%
        filter(game_date == (Sys.Date() - days(days_back))) %>%
        arrange(desc(game_date)) %>%
        as.data.frame()

      if (nrow(recent_data) > 0) {
        log_info(
          "Found {nrow(recent_data)} rows from the past {days_back} days"
        )
        return(recent_data)
      }

      # If still no data, return most recent data available
      log_info("No data from yesterday found, returning data from 2 days ago")
      most_recent <- df %>%
        filter(game_date == (Sys.Date() - days(days_back + 1))) %>%
        arrange(desc(game_date)) %>%
        as.data.frame()

      if (nrow(most_recent) > 0) {
        return(most_recent)
      }

      empty_df
    },
    error = function(e) {
      log_error("Error loading features: {conditionMessage(e)}")
      return(empty_df)
    }
  )

  # Final validation
  if (!is.data.frame(features_df) || nrow(features_df) == 0) {
    log_warn("No valid prediction data found, returning empty data frame")
    return(empty_df)
  }

  log_info("Returning {nrow(features_df)} rows of prediction data")
  features_df
}

#' Generate prediction report and visualization
#' @param current_features Optional current features dataframe
#' @return Metrics data or NULL if no data available
generate_prediction_report <- function(current_features = NULL) {
  log_info("Starting prediction performance analysis")

  # Get latest workflow
  workflow <- tryCatch(
    {
      get_latest_workflow()
    },
    error = function(e) {
      log_error("Error getting latest workflow: {conditionMessage(e)}")
      return(NULL)
    }
  )

  if (is.null(workflow)) {
    log_error("No workflow available for predictions")
    return(NULL)
  }

  # Get prediction data with fallback to recent historical data
  pred_data <- get_prediction_data(
    current_features = current_features,
    days_back = 1
  )

  # Validate prediction data
  if (!is.data.frame(pred_data)) {
    log_error("Invalid prediction data type: {class(pred_data)}")
    return(NULL)
  }

  if (nrow(pred_data) == 0) {
    log_warn("No prediction or recent historical data found")
    return(NULL)
  }

  log_info("Processing {nrow(pred_data)} rows for predictions")

  # Prepare data for prediction
  pred_data_reduced <- tryCatch(
    {
      pred_data %>%
        select(
          game_id,
          team_id,
          receiving_team_id,
          home_team_id,
          away_team_id,
          home_team_name,
          away_team_name,
          away_score,
          home_score,
          period_number,
          end_quarter_seconds_remaining,
          coordinate_x,
          coordinate_y,
          next_play_score
        ) %>%
        mutate(
          across(
            where(is.character),
            as.factor
          )
        )
    },
    error = function(e) {
      log_error("Error preparing prediction data: {conditionMessage(e)}")
      return(NULL)
    }
  )

  if (is.null(pred_data_reduced) || nrow(pred_data_reduced) == 0) {
    log_error("Failed to prepare prediction data")
    return(NULL)
  }

  # Generate predictions
  log_info("Generating predictions...")
  predictions <- tryCatch(
    {
      augment(workflow, new_data = pred_data_reduced)
    },
    error = function(e) {
      log_error("Error generating predictions: {conditionMessage(e)}")
      return(NULL)
    }
  )

  if (is.null(predictions) || nrow(predictions) == 0) {
    log_error("Failed to generate predictions")
    return(NULL)
  }

  # Calculate metrics
  log_info("Calculating team metrics...")
  team_metrics <- calculate_team_metrics(predictions)

  if (is.null(team_metrics) || !is.list(team_metrics)) {
    log_error("Failed to calculate team metrics")
    return(NULL)
  }

  overall_metrics <- predictions %>%
    metrics(truth = next_play_score, estimate = .pred)

  # Log summary statistics
  log_info("\nOverall prediction metrics:")
  print(overall_metrics)

  if (!is.null(team_metrics$offense) && nrow(team_metrics$offense) > 0) {
    log_info("\nTop 5 teams by points vs expected:")
    print(
      head(
        team_metrics$offense[,
          c("team_name", "points_vs_expectation", "n_turnovers")
        ],
        5
      )
    )

    # Create visualization
    log_info("Creating team performance visualization...")
    tryCatch(
      {
        create_team_performance_table(team_metrics$offense)
        log_info("Successfully created visualization")
      },
      error = function(e) {
        log_error("Error creating visualization: {conditionMessage(e)}")
      }
    )

    list(
      team_metrics = team_metrics,
      overall_metrics = overall_metrics,
      prediction_date = max(pred_data$game_date)
    )
  } else {
    log_warn("No team metrics calculated")
    NULL
  }
}

#' Helper function to get date from version string
#' @param version_str Version string
#' @return Date object
get_version_date <- function(version_str) {
  # Extract date part (assumes format YYYYMMDD_HHMMSS)
  date_str <- substr(basename(version_str), 1, 8)
  as.Date(date_str, format = "%Y%m%d")
}

#' Helper function to check if object exists in S3
#' @param path S3 path
#' @return Boolean indicating if object exists
object_exists_s3 <- function(path) {
  bucket <- "carbonite-bucket"
  tryCatch(
    {
      head_object(
        object = path,
        bucket = bucket
      )
      TRUE
    },
    error = function(e) {
      FALSE
    }
  )
}

# Modify save_version_to_s3 to ensure proper directory structure
save_version_to_s3 <- function(features_df, version_hash = NULL) {
  bucket <- "carbonite-bucket"

  # Generate version hash if not provided
  if (is.null(version_hash)) {
    version_hash <- format(Sys.time(), "%Y%m%d_%H%M%S")
  }

  log_info("Starting to save version {version_hash} to S3...")

  # Create full directory structure
  versions_prefix <- "versions/"
  version_dir <- paste0(versions_prefix, version_hash, "/")

  tryCatch(
    {
      # 1. Save features
      features_path <- paste0(version_dir, "features.parquet")
      log_info("Saving features to {features_path}")

      # Convert to arrow Table and write to parquet
      arrow::write_parquet(features_df, tempfile())
      temp_parquet <- tempfile()
      arrow::write_parquet(features_df, temp_parquet)
      put_object(
        file = temp_parquet,
        object = features_path,
        bucket = bucket
      )

      # 2. Save metadata
      metadata <- list(
        version_hash = version_hash,
        timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        n_rows = nrow(features_df),
        n_columns = ncol(features_df)
      )
      metadata_path <- paste0(version_dir, "metadata.json")
      temp_metadata <- tempfile()
      jsonlite::write_json(metadata, temp_metadata)
      put_object(
        file = temp_metadata,
        object = metadata_path,
        bucket = bucket
      )

      # 3. Create version marker
      marker_path <- paste0(version_dir, ".version")
      temp_marker <- tempfile()
      writeLines(version_hash, temp_marker)
      put_object(
        file = temp_marker,
        object = marker_path,
        bucket = bucket
      )

      # 4. Verify saved files
      log_info("Verifying saved files...")

      # Wait a short time for S3 consistency
      Sys.sleep(2)

      # List all objects in version directory
      saved_objects <- aws.s3::get_bucket(
        bucket = bucket,
        prefix = version_dir
      )

      if (length(saved_objects) < 3) {
        # We expect 3 files: features, metadata, and marker
        stop(
          sprintf(
            "Expected 3 files in version directory, but found %d",
            length(saved_objects)
          )
        )
      }

      # Verify each file exists and has content
      # files_to_check <- c(features_path, metadata_path, marker_path)
      # for (file_path in files_to_check) {
      #   if (!aws.s3::object_exists(bucket, file_path)) {
      #     stop(sprintf("Failed to verify file: %s", file_path))
      #   }
      # }

      log_info("Successfully verified all files for version {version_hash}")

      # Clean up temp files
      unlink(temp_parquet)
      unlink(temp_metadata)
      unlink(temp_marker)

      # Return the version hash
      version_hash
    },
    error = function(e) {
      log_error("Error saving version to S3: {conditionMessage(e)}")
      # Clean up any temp files
      if (exists("temp_parquet")) unlink(temp_parquet)
      if (exists("temp_metadata")) unlink(temp_metadata)
      if (exists("temp_marker")) unlink(temp_marker)
      stop(e)
    }
  )
}

# Also add this helper function to verify version exists
verify_version_exists <- function(version_hash) {
  bucket <- "carbonite-bucket"
  version_dir <- paste0("versions/", version_hash, "/")

  log_info("Verifying version {version_hash}...")

  # List objects in version directory
  objects <- aws.s3::get_bucket(
    bucket = bucket,
    prefix = version_dir
  )

  if (length(objects) == 0) {
    log_error("No files found for version {version_hash}")
    return(FALSE)
  }

  # Check for required files
  required_files <- c(
    paste0(version_dir, "features.parquet"),
    paste0(version_dir, "metadata.json"),
    paste0(version_dir, ".version")
  )

  existing_files <- sapply(objects, function(x) x$Key)
  missing_files <- setdiff(required_files, existing_files)

  if (length(missing_files) > 0) {
    log_error(
      "Missing required files for version {version_hash}: {paste(missing_files, collapse=', ')}"
    )
    return(FALSE)
  }

  log_info("Successfully verified version {version_hash}")
  TRUE
}

inspect_s3_versions <- function() {
  bucket <- "carbonite-bucket"
  versions_prefix <- "versions/"

  log_info("Inspecting S3 versions structure...")

  # List all objects
  objects <- aws.s3::get_bucket(bucket, prefix = versions_prefix)

  log_info("Total objects found: {length(objects)}")

  # Group objects by directory
  dirs <- list()
  for (obj in objects) {
    parts <- strsplit(obj$Key, "/")[[1]]
    if (length(parts) >= 2) {
      dir_name <- parts[2]
      if (!(dir_name %in% names(dirs))) {
        dirs[[dir_name]] <- list()
      }
      dirs[[dir_name]][[length(dirs[[dir_name]]) + 1]] <- obj
    }
  }

  # Print directory structure
  log_info("\nDirectory structure:")
  for (dir_name in names(dirs)) {
    log_info("\nDirectory: {dir_name}")
    log_info("Files:")
    for (obj in dirs[[dir_name]]) {
      log_info("  - {basename(obj$Key)} ({obj$Size} bytes)")
    }
  }

  invisible(dirs)
}

#' Save object to S3
#' @param object Object to save
#' @param path S3 path
#' @return TRUE if successful, FALSE otherwise
save_to_s3 <- function(object, path) {
  bucket <- "carbonite-bucket"
  temp_file <- tempfile()

  tryCatch(
    {
      log_info("Attempting to save object to s3://{bucket}/{path}")

      # Determine file type from extension
      ext <- tools::file_ext(path)

      # Save object to temp file based on type
      if (ext == "parquet") {
        arrow::write_parquet(object, temp_file)
      } else if (ext == "rds") {
        saveRDS(object, temp_file)
      } else if (ext == "json") {
        jsonlite::write_json(object, temp_file, auto_unbox = TRUE)
      } else if (ext == "csv") {
        write.csv(object, temp_file, row.names = FALSE)
      } else {
        # Default to RDS
        saveRDS(object, temp_file)
      }

      # Upload to S3
      result <- aws.s3::put_object(
        file = temp_file,
        object = path,
        bucket = bucket
      )

      # Verify upload
      if (!result) {
        log_error("Failed to upload object to S3")
        return(FALSE)
      }

      # Verify object exists
      exists <- aws.s3::object_exists(bucket, path)
      if (!exists) {
        log_error("Object not found in S3 after upload")
        return(FALSE)
      }

      # Get object info
      obj_info <- aws.s3::get_object_info(bucket, path)
      log_info(
        "Successfully saved object ({obj_info$Size} bytes) to s3://{bucket}/{path}"
      )

      TRUE
    },
    error = function(e) {
      log_error("Error saving to S3: {conditionMessage(e)}")
      FALSE
    },
    finally = {
      # Clean up temp file
      if (file.exists(temp_file)) {
        unlink(temp_file)
      }
    }
  )
}

# Add a new function for static generation
generate_static_content <- function(config_path = "config.yml") {
  main(config_path, static_only = TRUE)
}
