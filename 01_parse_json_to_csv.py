import json
from typing import Dict, Any, List, Optional

def extract_data_from_match_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Reads a JSON file containing cricket match data and extracts key information.

    Args:
        file_path: The full path to the cricket match data file (e.g., '1473503.json').

    Returns:
        A dictionary containing the extracted match data, or None if an error occurs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {file_path}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

    if not data or 'info' not in data or 'innings' not in data:
        print(f"Error: Missing essential keys in the data from {file_path}")
        return None

    info = data['info']
    innings: List[Dict[str, Any]] = data['innings']

    # --- 1. Extract Basic Match Information ---
    match_data = {
        'file_name': file_path,
        'date': info.get('dates', [])[0] if info.get('dates') else None,
        'event_name': info.get('event', {}).get('name'),
        'match_number': info.get('event', {}).get('match_number'),
        'teams': info.get('teams', []),
        'venue': info.get('venue'),
        'city': info.get('city'),
        'toss_winner': info.get('toss', {}).get('winner'),
        'toss_decision': info.get('toss', {}).get('decision'),
        'result_winner': info.get('outcome', {}).get('winner'),
        'result_by': info.get('outcome', {}).get('by'),
        'player_of_match': info.get('player_of_match', []),
        'innings_summary': []
    }

    # --- 2. Process Innings Data ---
    for inning in innings:
        team_name = inning.get('team')
        total_runs = 0
        total_extras = 0
        wickets_lost = 0

        for over in inning.get('overs', []):
            for delivery in over.get('deliveries', []):
                # Calculate total runs for the inning
                total_runs += delivery.get('runs', {}).get('total', 0)
                # Calculate extras (runs - batter runs)
                total_extras += delivery.get('runs', {}).get('extras', 0)
                # Count wickets
                if delivery.get('wickets'):
                    wickets_lost += len(delivery['wickets'])

        # Subtract extras from total_runs to get runs scored *by the bat*
        runs_from_bat = total_runs - total_extras

        innings_summary = {
            'team': team_name,
            'total_score': total_runs,
            'wickets_lost': wickets_lost,
            'runs_from_bat': runs_from_bat,
            'extras': total_extras,
            'overs_bowled': len(inning.get('overs', [])) # Approximate overs count
        }
        match_data['innings_summary'].append(innings_summary)

    return match_data

# --- Example Usage for a single file ---

FILE_TO_ANALYZE = '1473503.json'

# NOTE: You would need to replace this with the actual path if the file is not 
# in the same directory as the script.
extracted_data = extract_data_from_match_file(FILE_TO_ANALYZE)

if extracted_data:
    print(json.dumps(extracted_data, indent=4))
    print("\n--- Summary ---")
    print(f"Match: {extracted_data['event_name']} Match {extracted_data['match_number']}")
    print(f"Venue: {extracted_data['venue']}")
    print(f"Toss Winner: {extracted_data['toss_winner']} (Chose to {extracted_data['toss_decision']})")
    print(f"Winner: {extracted_data['result_winner']} by {extracted_data['result_by']}")
    print(f"Player of the Match: {', '.join(extracted_data['player_of_match'])}")
    print("\nInnings Scores:")
    for inning in extracted_data['innings_summary']:
        print(f"  {inning['team']}: {inning['total_score']}/{inning['wickets_lost']} in approximately {inning['overs_bowled']} overs")


# --- Function to process multiple files ---

def process_multiple_files(file_list: List[str]) -> List[Dict[str, Any]]:
    """
    Processes a list of match files and returns a list of extracted data dictionaries.

    Args:
        file_list: A list of file paths (e.g., ['1.json', '2.json', ...]).

    Returns:
        A list of dictionaries containing the extracted data for all successfully processed matches.
    """
    all_match_data = []
    for file_path in file_list:
        data = extract_data_from_match_file(file_path)
        if data:
            all_match_data.append(data)
    return all_match_data

# Example of how you would list your 1169 files:
# file_paths = [f"{i}.json" for i in range(1, 1170)] 
# all_data = process_multiple_files(file_paths)
# print(f"Successfully processed {len(all_data)} files.")
