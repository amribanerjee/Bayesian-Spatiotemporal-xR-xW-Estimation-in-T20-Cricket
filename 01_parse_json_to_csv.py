import json
import os
import glob
import pandas as pd
from typing import Dict, Any, List, Optional

# --- 1. CORE STATISTICAL ANALYSIS FUNCTION ---

def _analyze_innings_deliveries(innings: Dict[str, Any], match_id: str, innings_number: int) -> List[Dict[str, Any]]:
    """
    Analyzes deliveries to compile comprehensive batting and bowling statistics,
    and returns a list of dictionaries (one row per player) suitable for flattening.
    """
    batting_stats = {}
    bowling_stats = {}
    total_balls = 0

    # --- Pass 1: Gather Stats ---
    for over_data in innings.get('overs', []):
        for delivery in over_data.get('deliveries', []):

            batter = delivery.get('batter')
            bowler = delivery.get('bowler')
            runs_data = delivery.get('runs', {})
            extras_data = delivery.get('extras', {})
            wickets_data = delivery.get('wickets')

            is_valid_ball = 'wides' not in extras_data and 'noballs' not in extras_data
            if is_valid_ball:
                total_balls += 1

            # Init and update batter stats
            if batter:
                batting_stats.setdefault(batter, {'runs': 0, 'balls_faced': 0, '4s': 0, '6s': 0, 'dismissal': None, 'out_by': None, 'is_bowling': False})
                batting_stats[batter]['runs'] += runs_data.get('batter', 0)
                if is_valid_ball: batting_stats[batter]['balls_faced'] += 1
                runs_scored = runs_data.get('batter', 0)
                if runs_scored == 4: batting_stats[batter]['4s'] += 1
                elif runs_scored == 6: batting_stats[batter]['6s'] += 1

                if wickets_data and wickets_data[0].get('player_out') == batter:
                    batting_stats[batter]['dismissal'] = wickets_data[0].get('kind')
                    fielder_name = wickets_data[0].get('fielders', [{}])[0].get('name')
                    batting_stats[batter]['out_by'] = fielder_name or bowler

            # Init and update bowler stats
            if bowler:
                bowling_stats.setdefault(bowler, {'runs_conceded': 0, 'balls_bowled': 0, 'wickets': 0, 'extras': 0, 'is_batting': False})
                batting_stats.setdefault(bowler, {'is_bowling': False}) # Ensure bowler is initialized in batting map for the combined list later

                bowler_runs = runs_data.get('total', 0) - extras_data.get('byes', 0) - extras_data.get('legbyes', 0)
                bowling_stats[bowler]['runs_conceded'] += bowler_runs
                bowling_stats[bowler]['extras'] += runs_data.get('extras', 0)

                if is_valid_ball: bowling_stats[bowler]['balls_bowled'] += 1

                if wickets_data:
                    kind = wickets_data[0].get('kind')
                    is_bowler_wicket = kind in ['caught', 'bowled', 'lbw', 'stumped', 'hit wicket']
                    if is_bowler_wicket: bowling_stats[bowler]['wickets'] += 1

                # Mark player as a bowler in the bowling map
                bowling_stats[bowler]['is_batting'] = True

    # --- Pass 2: Combine and Finalize Data ---

    # Identify all unique players involved in this innings (batting or bowling)
    all_players = set(batting_stats.keys()) | set(bowling_stats.keys())

    # Prepare list of dictionaries for the DataFrame
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
            # Match & Innings Identifiers
            'match_id': match_id,
            'innings_number': innings_number,
            'innings_team': team_name,
            'player_name': player,

            # Batting Stats
            'bat_runs': runs_scored,
            'bat_balls_faced': balls_faced,
            'bat_4s': b_stats.get('4s', 0),
            'bat_6s': b_stats.get('6s', 0),
            'bat_sr': round((runs_scored / balls_faced) * 100, 2) if balls_faced > 0 else 0,
            'bat_dismissal': b_stats.get('dismissal'),
            'bat_out_by': b_stats.get('out_by'),

            # Bowling Stats
            'bowl_runs_conceded': runs_conceded,
            'bowl_wickets': w_stats.get('wickets', 0),
            'bowl_balls_bowled': balls_bowled,
            'bowl_overs': f"{balls_bowled // 6}.{balls_bowled % 6}",
            'bowl_economy': round((runs_conceded / balls_bowled) * 6, 2) if balls_bowled > 0 else 0,
            'bowl_extras': w_stats.get('extras', 0),
        }
        innings_rows.append(row)

    return innings_rows

# --- 2. SINGLE FILE PROCESSING FUNCTION ---

def extract_data_from_match_file(file_path: str) -> Optional[List[Dict[str, Any]]]:
    """Reads a single cricket match JSON file and extracts all data rows."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Error reading or parsing file {file_path}: {e}")
        return None

    info = data.get('info', {})

    # Extract high-level match data once
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

    # Process each innings
    for i, inning in enumerate(data.get('innings', [])):
        # Get list of player rows for this inning
        player_rows = _analyze_innings_deliveries(inning, match_data['match_id'], i + 1)

        # Merge high-level match data into each player row
        for row in player_rows:
            row.update(match_data)
            all_player_rows.append(row)

    return all_player_rows

# --- 3. BULK PROCESSING AND MAIN EXECUTION ---

def process_all_files_to_csv(directory_path: str, output_csv_file: str):
    """
    Dynamically finds all JSON files in the specified directory, processes them,
    and saves the combined data to a single CSV file.
    """
    # Use glob to find all files ending with .json in the directory
    # IMPORTANT: Ensure the directory_path is correct for your setup.
    file_paths = glob.glob(os.path.join(directory_path, '*.json'))

    if not file_paths:
        print(f"⚠️ No JSON files found in the directory: {directory_path}. Please check the path.")
        return

    print(f"Starting analysis. Found {len(file_paths)} files to process in '{directory_path}'.")

    combined_data = []

    for i, path in enumerate(file_paths):
        # Progress indicator
        if (i + 1) % 100 == 0:
            print(f"✅ Processed {i + 1} of {len(file_paths)} files...")

        player_rows = extract_data_from_match_file(path)
        if player_rows:
            combined_data.extend(player_rows)

    if not combined_data:
        print("❌ No data was successfully extracted. CSV file will not be created.")
        return

    # Convert the list of dictionaries into a pandas DataFrame
    df = pd.DataFrame(combined_data)

    # Save the DataFrame to a CSV file
    df.to_csv(output_csv_file, index=False, encoding='utf-8')

    print(f"\n--- Analysis Complete! ---")
    print(f"Data successfully compiled into {len(df)} rows and saved to: {output_csv_file}")


# --- DYNAMIC SETUP ---
# 1. IMPORTANT: Set the path to the folder containing your 1169 JSON files.
#    If the files are in a folder named 'cricket_data' in the same directory as this script:
#    DATA_DIRECTORY_PATH = 'cricket_data'
#    For the purpose of testing with the single file you uploaded, we use '.' (current directory).
DATA_DIRECTORY_PATH = '.'

# 2. Define the name of the final output CSV file.
OUTPUT_CSV_FILE = 'cricket_match_data.csv'

# 3. Execute the main processing function
process_all_files_to_csv(DATA_DIRECTORY_PATH, OUTPUT_CSV_FILE)
