"""
01_load_and_clean_csv.py
Purpose: Load the provided cricket_match_data.csv, clean it, 
create some initial features, and save cleaned output as cricket_clean.csv
"""

import pandas as pd
import numpy as np

# -------------------------------
# 1. Load the uploaded CSV
# -------------------------------
df = pd.read_csv("cricket_match_data.csv")

# -------------------------------
# 2. Basic cleaning
# -------------------------------
# Convert date to datetime
df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")

# Replace NAs in numeric fields
numeric_cols = [
    "bat_runs", "bat_balls_faced", "bat_4s", "bat_6s", 
    "bowl_runs_conceded", "bowl_wickets", "bowl_balls_bowled"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# -------------------------------
# 3. Derived Batting Features
# -------------------------------
df["bat_sr_calc"] = np.where(df["bat_balls_faced"] > 0,
                             df["bat_runs"] / df["bat_balls_faced"] * 100,
                             0)

df["bat_boundary_rate"] = np.where(df["bat_balls_faced"] > 0,
                                   (df["bat_4s"] + df["bat_6s"]) / df["bat_balls_faced"],
                                   0)

df["bat_is_out"] = df["bat_dismissal"].notna().astype(int)

# -------------------------------
# 4. Derived Bowling Features
# -------------------------------
df["bowl_overs_calc"] = df["bowl_balls_bowled"] / 6

df["bowl_economy_calc"] = np.where(df["bowl_overs_calc"] > 0,
                                   df["bowl_runs_conceded"] / df["bowl_overs_calc"],
                                   0)

df["bowl_strike_rate"] = np.where(df["bowl_wickets"] > 0,
                                  df["bowl_balls_bowled"] / df["bowl_wickets"],
                                  np.nan)

# -------------------------------
# 5. Match Context Features
# -------------------------------
# Home/away indicator
df["is_home_team"] = df.apply(
    lambda x: 1 if x["innings_team"] == x["team_1"] else 0,
    axis=1
)

# Toss advantage
df["toss_won"] = (df["innings_team"] == df["toss_winner"]).astype(int)

# -------------------------------
# 6. Save cleaned dataset
# -------------------------------
output_name = "cricket_clean.csv"
df.to_csv(output_name, index=False)

print(f"Cleaning complete. Saved cleaned file as {output_name}")
print(f"Rows: {len(df)}  |  Columns: {len(df.columns)}")
