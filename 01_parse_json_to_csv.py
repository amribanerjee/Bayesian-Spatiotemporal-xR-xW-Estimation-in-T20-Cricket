import json
import os
import glob
import pandas as pd
import zipfile
import shutil
from typing import Dict, Any, List, Optional

def _analyze_innings_deliveries(innings: Dict[str, Any], match_id: str, innings_number: int) -> List[Dict[str, Any]]:
    batting_stats = {}
    bowling_stats = {}
    total_balls = 0

    for over_data in innings.get('overs', []):
        for delivery in over_data.get('deliveries', []):
            batter = delivery.get('batter')
            bowler = delivery.get('bowler')
            runs_data = delivery.get('runs', {})
            extras_data = delivery.get('extras', {})
            wickets_data = delivery.get('wickets')
            
            is_valid_ball = 'wides' not in extras_data and 'noballs' not in extras_data
            if is_valid_ball: total_balls += 1
            
            if batter:
                batting_stats.setdefault(batter, {'runs': 0, 'balls_faced': 0, '4s': 0, '6s': 0, 'dismissal': None, 'out_by': None})
                runs_scored = runs_data.get('batter', 0)
                batting_stats[batter]['runs'] += runs_scored
                if is_valid_ball: batting_stats[batter]['balls_faced'] += 1
                if runs_scored == 4: batting_stats[batter]['4s'] += 1
                elif runs_scored == 6: batting_stats[batter]['6s'] += 1

                if wickets_data and wickets_data[0].get('player_out') == batter:
                    batting_stats[batter]['dismissal'] = wickets_data[0].get('kind')
                    fielder_name = wickets_data[0].get('fielders', [{}])[0].get('name')
                    batting_stats[batter]['out_by'] = fielder_name or bowler

            if bowler:
                bowling_stats.setdefault(bowler, {'runs_conceded': 0, 'balls_bowled': 0, 'wickets': 0, 'extras': 0})
                bowler_runs = runs_data.get('total', 0) - extras_data.get('byes', 0) - extras_data.get('legbyes', 0)
                bowling_stats[bowler]['runs_conceded'] += bowler_runs
                bowling_stats[bowler]['extras'] += runs_data.get('extras', 0) 
                if is_valid_ball: bowling_stats[bowler]['balls_bowled'] += 1
                
                if wickets_data:
                    kind = wickets_data[0].get('kind')
                    is_bowler_wicket = kind in ['caught', 'bowled', 'lbw', 'stumped', 'hit wicket']
                    if is_bowler_wicket: bowling_stats[bowler]['wickets'] += 1

    all_players = set(batting_stats.keys()) | set(bowling_stats.keys())
    innings_rows = []
    team_name = innings.get('team')

    for player in all_players:
        b_stats = batting_stats.get(player, {})
        w_stats = bowling_stats.get(player, {})
        
        balls_faced = b_stats.get('balls_faced', 0)
        balls_bowled = w_stats.get('balls_bowled', 0)
        runs_scored = b_stats.get('runs', 0)
        runs_conceded = w_stats.get('runs_conceded', 0)

        row = {
            'innings_number': innings_number,
            'innings_team': team_name,
            'player_name': player,
            
            'bat_runs': runs_scored,
            'bat_balls_faced': balls_faced,
            'bat_4s': b_stats.get('4s', 0),
            'bat_6s': b_stats.get('6s', 0),
            'bat_sr': round((runs_scored / balls_faced) * 100, 2) if balls_faced > 0 else 0,
            'bat_dismissal': b_stats.get('dismissal'),
            'bat_out_by': b_stats.get('out_by'),
            
            'bowl_runs_conceded': runs_conceded,
            'bowl_wickets': w_stats.get('wickets', 0),
            'bowl_balls_bowled': balls_bowled,
            'bowl_overs': f"{balls_bowled // 6}.{balls_bowled % 6}",
            'bowl_economy': round((runs_conceded / balls_bowled) * 6, 2) if balls_bowled > 0 else 0,
            'bowl_extras': w_stats.get('extras', 0),
        }
        innings_rows.append(row)
        
    return innings_rows

def extract_data_from_match_file(file_path: str) -> Optional[List[Dict[str, Any]]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        return None

    info = data.get('info', {})
    
    match_data = {
        'file_name': os.path.basename(file_path),
        'match_id': info.get('event', {}).get('match_number', os.path.basename(file_path).split('.')[0]),
        'date': info.get('dates', ['Unknown'])[0],
        'venue': info.get('venue'),
        'team_1': info.get('teams', [''])[0],
        'team_2': info.get('teams', [''])[1],
        'toss_winner': info.get('toss', {}).get('winner'),
        'result_winner': info.get('outcome', {}).get('winner'),
        'player_of_match': ', '.join(info.get('player_of_match', [])),
    }

    all_player_rows = []
    
    for i, inning in enumerate(data.get('innings', [])):
        player_rows = _analyze_innings_deliveries(inning, match_data['match_id'], i + 1)
        
        for row in player_rows:
            row.update(match_data)
            all_player_rows.append(row)
            
    return all_player_rows

def unzip_uploaded_data(zip_file_path: str, extract_to_dir: str) -> bool:
    try:
        if os.path.exists(extract_to_dir):
            shutil.rmtree(extract_to_dir)
        os.makedirs(extract_to_dir, exist_ok=True)
        
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_dir)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        return False

def main():
    ZIP_FILE_NAME = 'ipl_json.zip' 
    EXTRACT_DIR = 'temp_ipl_json_data'
    OUTPUT_CSV_FILE = 'ipl_match_data.csv'
    
    if not unzip_uploaded_data(ZIP_FILE_NAME, EXTRACT_DIR):
        return

    file_paths = glob.glob(os.path.join(EXTRACT_DIR, '*.json'))
    
    if not file_paths:
        return
        
    combined_data = []
    
    for i, path in enumerate(file_paths):
        player_rows = extract_data_from_match_file(path)
        if player_rows:
            combined_data.extend(player_rows)
            
    if not combined_data:
        return

    df = pd.DataFrame(combined_data)
    
    shutil.rmtree(EXTRACT_DIR)

    df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8')

if __name__ == "__main__":
    import sys
    try:
        import pandas as pd
        main()
    except ImportError:
        sys.stderr.write("\nFATAL ERROR: The 'pandas' library is required to create the CSV file.\n")
        sys.stderr.write("Please install it by running: pip install pandas\n")
