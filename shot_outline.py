import pandas as pd
from nba_api.stats.static import teams
import os
nba_teams = teams.get_teams()
# Extract just the 'id' from each team dictionary
team_ids = [team['id'] for team in nba_teams]

frames=[]
for year in range(2014,2026):
    for team_id in team_ids:
        carries = ['','ps']
        for carry in carries:
            filepath= f"../shot_data/team/{year}{carry}/{team_id}.csv"
     
            if os.path.isfile(filepath):
              
                columns='SHOT_ZONE_RANGE','SHOT_DISTANCE','LOC_X','LOC_Y','GAME_ID','SHOT_ZONE_BASIC','GAME_ID','GAME_EVENT_ID'

                df = pd.read_csv(filepath,usecols=columns)
                frames.append(df)
    print(year)
all_shots=pd.concat(frames)
all_shots.to_csv('playbyplay_shotdetails.csv',index=False)