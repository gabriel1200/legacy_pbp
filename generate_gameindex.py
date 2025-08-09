import pandas as pd
import os
import json

def generate_game_files(rotation_csv_path, dates_csv_path, pbp_dir, output_dir="game_info"):
    """
    Processes local rotation, game date, and play-by-play CSVs to generate
    JSON files for each game, mimicking the structure of the NBA liveData/boxscore endpoint.

    Args:
        rotation_csv_path (str): Path to the CSV file containing player rotation data.
        dates_csv_path (str): Path to the CSV file containing game date and team info.
        pbp_dir (str): Path to the directory containing play-by-play CSV files.
        output_dir (str): The directory where the output JSON files will be saved.
    """
    # --- 1. Load Data ---
    # Load the provided CSV files into pandas DataFrames.
    try:
        rotation_df = pd.read_csv(rotation_csv_path)
        dates_df = pd.read_csv(dates_csv_path)
        print("Successfully loaded rotation and dates CSV files.")
    except FileNotFoundError as e:
        print(f"Error loading files: {e}. Please ensure the paths are correct.")
        return

    # --- 2. Create Output Directory ---
    # Create the target directory if it doesn't already exist.
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    # --- 3. Process Each Game ---
    # Get a list of unique game IDs from the dates file.
    game_ids = dates_df["GAME_ID"].unique()
    print(f"Found {len(game_ids)} unique games to process.")

    for game_id in game_ids:
        # --- 3a. Filter Data for the Current Game ---
        game_rotations = rotation_df[rotation_df["GAME_ID"] == game_id]
        game_dates_info = dates_df[dates_df["GAME_ID"] == game_id]

        if game_rotations.empty or game_dates_info.empty:
            print(f"Warning: Skipping game {game_id} due to missing rotation/date data.")
            continue

        # --- 3b. Identify Home and Away Teams ---
        home_team_info = game_dates_info[game_dates_info["team"] == game_dates_info["HTM"]].iloc[0]
        away_team_info = game_dates_info[game_dates_info["team"] != game_dates_info["HTM"]].iloc[0]

        home_team_id = home_team_info["TEAM_ID"]
        away_team_id = away_team_info["TEAM_ID"]

        home_team_details = game_rotations[game_rotations["TEAM_ID"] == home_team_id].iloc[0]
        away_team_details = game_rotations[game_rotations["TEAM_ID"] == away_team_id].iloc[0]

        # --- 3c. Get Final Score from Play-by-Play Data ---
        home_score = None
        away_score = None
        # Construct the path to the PBP file. Based on your sample, the filename is the game_id.
        pbp_file_path = os.path.join(pbp_dir, f"{game_id}.csv")

        if os.path.exists(pbp_file_path):
            try:
                # low_memory=False helps prevent errors with mixed data types in large PBP files.
                pbp_df = pd.read_csv(pbp_file_path, low_memory=False)
                # Find the last non-null score entry in the 'SCORE' column.
                final_score_series = pbp_df['SCORE'].dropna()
                if not final_score_series.empty:
                    last_score_str = final_score_series.iloc[-1]
                    # The score format is 'HOME_SCORE - AWAY_SCORE'.
                    score_parts = last_score_str.split(' - ')
                    if len(score_parts) == 2:
                        home_score = int(score_parts[0])
                        away_score = int(score_parts[1])
                    else:
                        print(f"Warning: Could not parse score '{last_score_str}' for game {game_id}.")
                else:
                    print(f"Warning: No scores found in PBP file for game {game_id}.")
            except Exception as e:
                print(f"Error processing PBP file for game {game_id}: {e}")
        else:
            print(f"Warning: PBP file not found for game {game_id} at {pbp_file_path}")

        # --- 3d. Get Player Information and Identify Starters ---
        name_dict = {}
        for _, row in game_rotations[['PERSON_ID', 'PLAYER_FIRST', 'PLAYER_LAST']].drop_duplicates().iterrows():
            name_dict[str(row['PERSON_ID'])] = f"{row['PLAYER_FIRST']} {row['PLAYER_LAST']}"

        starters = game_rotations[game_rotations["IN_TIME_REAL"] == 0.0]
        starter_on = ','.join(starters["PERSON_ID"].astype(str).tolist())

        # --- 3e. Assemble the Final JSON Structure ---
        game_output = {
            "homeTeam": {
                "name": home_team_details["TEAM_NAME"],
                "score": home_score,  # Updated with score from PBP
                "logo": f"https://cdn.nba.com/logos/nba/{home_team_id}/primary/L/logo.svg"
            },
            "awayTeam": {
                "name": away_team_details["TEAM_NAME"],
                "score": away_score,  # Updated with score from PBP
                "logo": f"https://cdn.nba.com/logos/nba/{away_team_id}/primary/L/logo.svg"
            },
            "game_id": str(game_id),
            "date": pd.to_datetime(str(home_team_info["date"]), format='%Y%m%d').strftime('%Y-%m-%d'),
            "status": "Final",
            "players": name_dict,
            "starter_on": starter_on
        }

        # --- 3f. Save the JSON File ---
        file_path = os.path.join(output_dir, f"00{game_id}.json")
        with open(file_path, 'w') as f:
            json.dump(game_output, f, indent=4)

    print(f"\nProcessing complete. JSON files saved in '{output_dir}'.")


# --- Execution ---
if __name__ == "__main__":
    # To run this script, provide the paths to your data files.
    # The PBP files should all be in a single directory.
    generate_game_files(
        rotation_csv_path="rotations_total.csv",
        dates_csv_path="https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv",
        pbp_dir="gamepbp"  # <-- IMPORTANT: Update this to the actual directory of your PBP files.
    )
