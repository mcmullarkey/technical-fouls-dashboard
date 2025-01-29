library(hoopR)
library(dplyr)
library(stringr)
library(tidymodels)
library(glmnet)
library(vetiver)
library(pins)

main <- function() {
  
  df <- get_pbp_data()
  
  df |> 
    get_game_spreads()
  
  df |> 
    get_tech_fouls()
  
  df_data <- df |> 
    create_initial_data() |> 
    print()
  
  glimpse(df_data)
  
  fit_version_vetiver(df_data)
  
}

get_pbp_data <- function() {
  
  tictoc::tic()
  progressr::with_progress({
    nba_pbp <- hoopR::load_nba_pbp(2023)
  })
  tictoc::toc()
  
  return(nba_pbp)
  
}

get_tech_fouls <- function(.df) {
  
  .df |> 
    group_by(game_id) |> 
    summarize(has_tech = any(str_detect(type_text, "Technical Foul"))) |> 
    count(has_tech, sort = TRUE) |> 
    print()
  
}

get_game_spreads <- function(.df) {
  .df |> 
    group_by(game_id) |> 
    summarize(has_favorite = any(home_favorite)) |>
    count(has_favorite) |> 
    print()
}

calculate_longest_run <- function(score, opponent_score) {
  run_length <- 0
  max_run <- 0
  run_score <- 0
  opponent_run_score <- 0
  
  for (i in seq_along(score)[-1]) {
    delta <- score[i] - score[i - 1]
    opponent_delta <- opponent_score[i] - opponent_score[i - 1]
    
    if (opponent_run_score <= 3) {
      run_score <- run_score + delta
      opponent_run_score <- opponent_run_score + opponent_delta
    } else {
      run_score <- delta
      opponent_run_score <- opponent_delta
    }
    
    if (run_score >= 9 && opponent_run_score <= 3) {
      max_run <- max(max_run, run_score - opponent_run_score)
    }
  }
  
  return(max_run)
}

get_athlete_counts <- function(.df) {
  
  athlete_counts <- .df %>%
    select(game_id, starts_with("athlete_id_")) %>%
    pivot_longer(cols = starts_with("athlete_id_"), values_to = "athlete_id") %>%
    filter(!is.na(athlete_id)) %>%
    count(game_id, athlete_id) %>%
    pivot_wider(names_from = athlete_id, values_from = n, values_fill = list(n = 0))
  
}

create_initial_data <- function(.df) {
  
  athlete_counts <- get_athlete_counts(.df)
  
  .df |> 
    group_by(game_id) |> 
    summarize(
      has_tech = factor(any(str_detect(type_text, "Technical Foul"), na.rm = TRUE)),
      max_score_diff = max(abs(home_score - away_score), na.rm = TRUE),
      whic_max_diff = factor(case_when(
        max(home_score - away_score, na.rm = TRUE) == max(abs(home_score - away_score), na.rm = TRUE) ~ "home",
        TRUE ~ "away"
      )),
      away_longest_run = calculate_longest_run(away_score, home_score),
      home_longest_run = calculate_longest_run(home_score, away_score),
      date = first(game_date)
    ) |> 
    left_join(athlete_counts, by = "game_id")
  
}

# Will refactor into helper functions after it's running

fit_version_vetiver <- function(.df) {
  
  set.seed(33)
  games_split <- .df |>
    initial_validation_split(strata = has_tech)
  
  games_set <- validation_set(games_split)
  games_test <- testing(games_split)
  
  # Define recipe for preprocessing
  games_rec <- recipe(has_tech ~ ., data = training(games_split)) |>
    update_role(game_id, new_role = "ID") |> 
    step_normalize(all_numeric_predictors()) |> 
    step_date(date, features = c("dow", "month", "year")) %>%               
    step_holiday(date, 
                 holidays = timeDate::listHolidays("US"), 
                 keep_original_cols = FALSE) |> 
    step_dummy(all_nominal_predictors())
  
  # Elastic net model specification
  elnet_spec <-
    logistic_reg(
      penalty = tune(),
      mixture = tune()
    ) |>
    set_engine("glmnet", validation = 0.2) |>
    set_mode("classification")
  
  # Create workflow using the recipe
  elnet_wf <- workflow() |>
    add_recipe(games_rec) |>
    add_model(elnet_spec)
  
  doParallel::registerDoParallel()
  set.seed(33)
  
  # cross validate
  elnet_rs <- tune_grid(elnet_wf, games_set, grid = 5)
  
  print(show_best(elnet_rs, metric = "roc_auc"))
  
  # train based on best model RMSE
  games_fit <- elnet_wf |>
    finalize_workflow(select_best(elnet_rs)) |>
    last_fit(games_split)
  
  # grab the workflow and create a vetiver model
  v <- extract_workflow(games_fit) |>
    vetiver_model("games-tech-elnet")
  
  # create the folder for storing the model artifact
  board <- board_folder("models")
  
  # store the model
  vetiver_pin_write(board, v)
  
}

main()