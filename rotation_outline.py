import pandas as pd
import os

# Attempt to import the nba_api library
try:
    from nba_api.stats.static import teams
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False


def fetch_rotation_data():
    """
    Loads rotation data for all NBA teams for specified years from a local directory.
    The local directory is ../shot_data/rotations relative to the script.
    Returns one combined DataFrame for all teams/seasons.
    """
    # Base path for local rotations folder
    base_path = os.path.join(os.path.dirname(__file__), "../shot_data/rotations")

    # --- Configuration ---
    start_year = 2014
    end_year = 2025
    years = range(start_year, end_year + 1)

    season_types = ["", "ps"]  # "" for regular season, "ps" for postseason

    # --- Dynamic Team ID Fetching ---
    team_ids = []
    if NBA_API_AVAILABLE:
        print("Fetching team list from nba_api...")
        try:
            nba_teams = teams.get_teams()
            team_ids = [team['id'] for team in nba_teams]
            print(f"Successfully fetched {len(team_ids)} team IDs.")
        except Exception as e:
            print(f"ERROR: Unable to fetch team IDs: {e}")
    else:
        print("WARNING: nba_api not found. Please install it or provide team_ids manually.")

    if not team_ids:
        print("\n--- Halting execution: No team IDs were loaded. ---")
        return pd.DataFrame()

    all_dfs = []
    print("\n--- Starting Rotation Data Load from Local ---")

    for year in years:
        for season_suffix in season_types:
            year_str = f"{year}{season_suffix}"

            for team_id in team_ids:
                file_path = os.path.join(base_path, year_str, f"{team_id}.csv")

                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        df["season"] = year_str
                        df["team_id"] = team_id
                        all_dfs.append(df)
                    except Exception:
                        pass

    print("\n--- Data Load Complete ---")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()


# --- Execution ---
if __name__ == "__main__":
    rotation_df = fetch_rotation_data()

    if not rotation_df.empty:
        rotation_df.tail(40).to_csv('rotation_sample.csv', index=False)
        dates = pd.read_csv(
            "https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv"
        )
        rotation_df.to_csv('rotations_total.csv',index=False)
        dates.tail(40).to_csv("date_sample.csv", index=False)
        
        print(f"\nTotal rows collected: {len(rotation_df)}")
        print("\n--- Example: Displaying first 5 rows ---")
        print(rotation_df.head())
    else:
        print("\nNo data was collected.")
