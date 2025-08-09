import pandas as pd
import os
import json
import sys

# Attempt to import the nba_api library for fetching team data
try:
    from nba_api.stats.static import teams
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False
    print("WARNING: 'nba_api' library not found. Team IDs will need to be provided manually if not cached.")

# --- Part 1: Rotation Data Loading ---

def fetch_rotation_data(base_path, start_year=2014, end_year=2025):
    """
    Loads rotation data for all NBA teams for specified years from a local directory structure.
    """
    rotations_path = os.path.join(base_path, "rotations")
    years = range(start_year, end_year + 1)
    season_types = ["", "ps"]  # "" for regular season, "ps" for postseason

    team_ids = []
    if NBA_API_AVAILABLE:
        print("Fetching team list from nba_api...")
        try:
            nba_teams = teams.get_teams()
            team_ids = [team['id'] for team in nba_teams]
            print(f"Successfully fetched {len(team_ids)} team IDs.")
        except Exception as e:
            print(f"ERROR: Unable to fetch team IDs from API: {e}")
    else:
        print("WARNING: nba_api not available. Manual team list would be needed if not for local files.")

    if not team_ids:
        print("\n--- Halting execution: No team IDs were loaded. ---")
        return pd.DataFrame()

    all_dfs = []
    print("\n--- Starting Rotation Data Load from Local Files---")
    for year in years:
        for season_suffix in season_types:
            year_str = f"{year}{season_suffix}"
            for team_id in team_ids:
                file_path = os.path.join(rotations_path, year_str, f"{team_id}.csv")
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        df["season"] = year_str
                        df["team_id"] = team_id
                        all_dfs.append(df)
                    except Exception as e:
                        print(f"Could not process file {file_path}: {e}")

    print("\n--- Rotation Data Load Complete ---")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        print("Warning: No rotation data was loaded.")
        return pd.DataFrame()


# --- Part 2: Game Index Generation ---

def generate_game_files(rotation_df, dates_df, pbp_dir, output_dir="game_info"):
    """
    Processes data to generate JSON files for each game, skipping existing files
    and using PBP data as a fallback for team identification.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    game_ids = dates_df["GAME_ID"].unique()
    print(f"Found {len(game_ids)} unique games to process.")
    
    generated_count = 0
    skipped_count = 0

    for game_id in game_ids:
        output_file_path = os.path.join(output_dir, f"{game_id}.json")
        if os.path.exists(output_file_path):
            skipped_count += 1
            continue

        game_rotations = rotation_df[rotation_df["GAME_ID"].astype(str) == str(game_id)]
        if game_rotations.empty:
            print(f"Warning: Skipping game {game_id} due to missing rotation data.")
            continue

        # Get game date info right away, as it's needed regardless of the team ID method.
        game_dates_info = dates_df[dates_df["GAME_ID"].astype(str) == str(game_id)]
        if game_dates_info.empty:
            # This case should be rare since we are iterating on game_ids from dates_df, but it's a good safeguard.
            print(f"Warning: Could not find date info for game {game_id}. Skipping.")
            continue
        
        # **FIX**: Get the date from the correct DataFrame (game_dates_info)
        game_date_str = str(game_dates_info['date'].iloc[0])

        home_team_id = None
        away_team_id = None

        # --- Method 1: Try to get teams from game_dates.csv (Primary) ---
        try:
            home_team_info = game_dates_info[game_dates_info["team"] == game_dates_info["HTM"]].iloc[0]
            away_team_info = game_dates_info[game_dates_info["team"] != game_dates_info["HTM"]].iloc[0]
            home_team_id = home_team_info["TEAM_ID"]
            away_team_id = away_team_info["TEAM_ID"]
        except (IndexError, KeyError):
            # --- Method 2: If primary fails, use PBP file (Fallback) ---
            print(f"Info for game {game_id} not in dates file. Trying PBP fallback...")
            pbp_file_path = os.path.join(pbp_dir, f"{game_id}.csv")
            if os.path.exists(pbp_file_path):
                try:
                    pbp_df = pd.read_csv(pbp_file_path, low_memory=False)
                    home_event = pbp_df[pbp_df['HOMEDESCRIPTION'].notna()].iloc[0]
                    home_team_id = int(home_event['PLAYER1_TEAM_ID'])

                    all_teams_in_game = game_rotations['TEAM_ID'].unique()
                    if len(all_teams_in_game) >= 2:
                        away_team_id = [team for team in all_teams_in_game if team != home_team_id][0]
                    
                    if not away_team_id or home_team_id == away_team_id:
                        print(f"Warning: Could not distinguish home/away teams from PBP for game {game_id}. Skipping.")
                        continue
                except Exception as e:
                    print(f"Warning: Could not determine teams from PBP for game {game_id}: {e}. Skipping.")
                    continue
            else:
                print(f"Warning: No PBP file found for game {game_id} to use as fallback. Skipping.")
                continue
        
        if not home_team_id or not away_team_id:
            print(f"Warning: Failed to identify teams for game {game_id}. Skipping.")
            continue

        try:
            home_team_details = game_rotations[game_rotations["TEAM_ID"] == home_team_id].iloc[0]
            away_team_details = game_rotations[game_rotations["TEAM_ID"] == away_team_id].iloc[0]
        except IndexError:
            print(f"Warning: Could not find team details in rotation data for game {game_id}. Skipping.")
            continue

        home_score, away_score = None, None
        pbp_file_path = os.path.join(pbp_dir, f"{game_id}.csv")
        if os.path.exists(pbp_file_path):
            try:
                pbp_df = pd.read_csv(pbp_file_path, low_memory=False)
                final_score_series = pbp_df['SCORE'].dropna()
                if not final_score_series.empty:
                    score_parts = final_score_series.iloc[-1].split(' - ')
                    if len(score_parts) == 2:
                        home_score, away_score = int(score_parts[0]), int(score_parts[1])
            except Exception as e:
                print(f"Error processing PBP file for game {game_id}: {e}")

        name_dict = {str(row['PERSON_ID']): f"{row['PLAYER_FIRST']} {row['PLAYER_LAST']}"
                     for _, row in game_rotations[['PERSON_ID', 'PLAYER_FIRST', 'PLAYER_LAST']].drop_duplicates().iterrows()}
        starters = game_rotations[game_rotations["IN_TIME_REAL"] == 0.0]
        starter_on = ','.join(starters["PERSON_ID"].astype(str).tolist())

        game_output = {
            "homeTeam": {"name": home_team_details["TEAM_NAME"], "score": home_score, "logo": f"https://cdn.nba.com/logos/nba/{home_team_id}/primary/L/logo.svg"},
            "awayTeam": {"name": away_team_details["TEAM_NAME"], "score": away_score, "logo": f"https://cdn.nba.com/logos/nba/{away_team_id}/primary/L/logo.svg"},
            "game_id": str(game_id),
            # **FIX**: Use the game_date_str variable captured earlier
            "date": pd.to_datetime(game_date_str, format='%Y%m%d').strftime('%Y-%m-%d'),
            "status": "Final",
            "players": name_dict,
            "starter_on": starter_on
        }

        with open(output_file_path, 'w') as f:
            json.dump(game_output, f, indent=4)
        generated_count += 1

    print(f"\n--- Processing Complete ---")
    print(f"Files Generated: {generated_count}")
    print(f"Files Skipped (already existed): {skipped_count}")


# --- Part 3: Execution ---

if __name__ == "__main__":
    SHOT_DATA_BASE_PATH = "../shot_data"
    PBP_DIR = "gameplaybyplay"
    OUTPUT_DIR = "game_info"
    DATES_CSV_URL = "https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv"

    rotation_data = fetch_rotation_data(base_path=SHOT_DATA_BASE_PATH)

    if not rotation_data.empty:
        try:
            dates_data = pd.read_csv(DATES_CSV_URL)
            dates_data = dates_data[dates_data.season >= '2013-14']
            print("Successfully loaded and filtered game dates file.")
        except Exception as e:
            print(f"Fatal Error: Could not load game dates file from URL. {e}")
            dates_data = pd.DataFrame()

        if not dates_data.empty:
            generate_game_files(
                rotation_df=rotation_data,
                dates_df=dates_data,
                pbp_dir=PBP_DIR,
                output_dir=OUTPUT_DIR
            )
    else:
        print("Halting execution because no rotation data could be loaded.")
