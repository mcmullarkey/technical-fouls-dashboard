library(hoopR)
library(dplyr)
library(stringr)
library(tidyr)
library(arrow)
library(duckplyr)

main <- function() {
  df <- get_pbp_data()
  features <- extract_turnover_features(df)
  features <- df_from_parquet("nba_turnovers_2025.parquet")
  analyze_outcomes(features)
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
  subsequent_plays <- pbp_data %>%
    filter(
      game_id == !!game_id,
      sequence_number > after_sequence,
      team_id == !!team_id
    ) %>%
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

extract_turnover_features <- function(pbp_data) {
  # First identify live-ball turnovers
  live_ball_turnovers <- c("Bad Pass\nTurnover", "Lost Ball Turnover")

  # Get the turnover plays
  turnover_plays <- pbp_data %>%
    filter(type_text %in% live_ball_turnovers) %>%
    mutate(
      receiving_team_id = case_when(
        team_id == home_team_id ~ away_team_id,
        team_id == away_team_id ~ home_team_id
      )
    )

  # Process each turnover to find next play
  processed_turnovers <- lapply(1:nrow(turnover_plays), function(i) {
    current_play <- turnover_plays[i, ]

    # Find the next play in the same game
    next_play <- pbp_data %>%
      filter(
        game_id == current_play$game_id,
        sequence_number > current_play$sequence_number
      ) %>%
      arrange(sequence_number) %>%
      slice(1)

    if (nrow(next_play) > 0) {
      # Calculate points including free throws if it's a foul
      points_scored <- if (
        !is.na(next_play$type_text) && str_detect(next_play$type_text, "Foul")
      ) {
        # Get points from subsequent free throws
        get_free_throw_points(
          pbp_data,
          next_play$game_id,
          next_play$sequence_number,
          current_play$receiving_team_id
        )
      } else {
        if (is.null(next_play$score_value) || is.na(next_play$score_value))
          0 else next_play$score_value
      }

      # Determine outcome using updated logic
      outcome <- case_when(
        # If next play is by receiving team and they score (including FTs)
        next_play$team_id == current_play$receiving_team_id &
          points_scored > 0 ~
          "score",
        # If next play is a foul by the turnover team
        next_play$team_id == current_play$team_id &
          str_detect(next_play$type_text, "Foul") ~
          "foul",
        # If the next play is by the receiving team and they miss or turn over
        next_play$team_id == current_play$receiving_team_id &
          (
            (
              str_detect(next_play$type_text, "Shot") &
                next_play$scoring_play == FALSE
            ) |
              (
                str_detect(next_play$type_text, "Layup") &
                  next_play$scoring_play == FALSE
              ) |
              (
                str_detect(next_play$type_text, "Dunk") &
                  next_play$scoring_play == FALSE
              ) |
              str_detect(next_play$type_text, "Turnover") |
              str_detect(next_play$type_text, "Traveling") |
              str_detect(next_play$type_text, "End Period") |
              str_detect(next_play$type_text, "Foul") |
              str_detect(next_play$type_text, "Charge")
          ) ~
          "stop",
        str_detect(next_play$type_text, "Substitution") |
          str_detect(next_play$type_text, "End Period") |
          str_detect(next_play$type_text, "Full Timeout") ~
          "stoppage",
        # Otherwise it's another type of play
        TRUE ~ "other"
      )

      # Combine current play with next play info and outcome
      current_play %>%
        mutate(
          next_play_type = next_play$type_text,
          next_play_team = next_play$team_id,
          next_play_score = points_scored,
          next_play_shooting = next_play$shooting_play,
          outcome = outcome,
          time_to_next_play = current_play$start_game_seconds_remaining -
            next_play$start_game_seconds_remaining
        )
    } else {
      # If no next play (end of game), mark as other
      current_play %>%
        mutate(
          next_play_type = NA_character_,
          next_play_team = NA_integer_,
          next_play_score = NA_integer_,
          next_play_shooting = NA,
          outcome = "other",
          time_to_next_play = NA_real_
        )
    }
  })

  # Combine all processed turnovers
  turnover_outcomes <- bind_rows(processed_turnovers)

  # Extract features
  features_df <- turnover_outcomes %>%
    mutate(
      # Temporal features
      minutes_remaining = clock_minutes,
      seconds_remaining = clock_seconds,
      period = period_number,
      time_remaining_in_period = start_quarter_seconds_remaining,

      # Game context features
      score_difference = case_when(
        team_id == home_team_id ~ home_score - away_score,
        team_id == away_team_id ~ away_score - home_score,
        TRUE ~ NA_real_
      ),

      # Sequence features
      plays_since_last_timeout = sequence_number -
        lag(
          sequence_number,
          default = first(sequence_number),
          order_by = sequence_number
        ),

      is_clutch_time = (period >= 4 & time_remaining_in_period <= 300)
    ) %>%
    # Select relevant columns
    select(
      # Identifiers
      game_id,
      sequence_number,

      # Location (raw coordinates only)
      coordinate_x,
      coordinate_y,

      # Temporal
      period,
      minutes_remaining,
      seconds_remaining,
      time_remaining_in_period,
      time_to_next_play,

      # Game context
      score_difference,
      is_clutch_time,

      # Teams
      team_id,
      receiving_team_id,

      # Sequence
      plays_since_last_timeout,

      # Next play details for verification
      next_play_type,
      next_play_team,
      next_play_score,
      next_play_shooting,

      # Outcome (what we want to predict)
      outcome
    )

  write_parquet(features_df, "nba_turnovers_2025.parquet")

  return(features_df)
}

analyze_outcomes <- function(features_df) {
  features_df |>
    glimpse()

  features_df |>
    filter(outcome == "other") |>
    count(outcome, next_play_type, sort = TRUE) |>
    print(n = 50)

  features_df |>
    filter(next_play_type == "Substitution") |>
    glimpse()

  # Overall outcome distribution
  print("Overall outcome distribution:")
  features_df %>%
    count(outcome) %>%
    mutate(pct = n / sum(n) * 100) %>%
    arrange(desc(n)) %>%
    print(n = Inf)

  # Outcome distribution in clutch time
  print("\nOutcome distribution in clutch time vs non-clutch:")
  features_df %>%
    count(is_clutch_time, outcome) %>%
    group_by(is_clutch_time) %>%
    mutate(pct = n / sum(n) * 100) %>%
    arrange(is_clutch_time, desc(n)) %>%
    print(n = Inf)

  # Points distribution
  print("\nPoints distribution following turnovers:")
  features_df %>%
    count(next_play_score) %>%
    mutate(pct = n / sum(n) * 100) %>%
    arrange(desc(n)) %>%
    print(n = Inf)

  # Average points by outcome
  print("\nAverage points by outcome:")
  features_df %>%
    group_by(outcome) %>%
    summarise(
      avg_points = mean(next_play_score, na.rm = TRUE),
      n = n()
    ) %>%
    arrange(desc(n)) %>%
    print(n = Inf)

  return(features_df)
}

main()
