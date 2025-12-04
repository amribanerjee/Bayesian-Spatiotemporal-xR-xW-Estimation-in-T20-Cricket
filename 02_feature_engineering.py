import pandas as pd
import numpy as np

# ---------------------------
# 1. Load Data
# ---------------------------
df = pd.read_csv("cricket_match_data.csv")

print("Initial rows:", len(df))

# ---------------------------
# 2. Basic Cleaning
# ---------------------------
df["bat_runs"] = df["bat_runs"].fillna(0)
df["wicket_flag"] = df["wicket_kind"].notna().astype(int)

# Convert ball_number like 5.6 → over/ball
df["over"] = df["ball_number"].astype(str).str.split(".").str[0].astype(int)
df["ball_in_over"] = df["ball_number"].astype(str).str.split(".").str[1].astype(int)

# ---------------------------
# 3. Match Phase Feature
# Powerplay: 1–6
# Middle overs: 7–15
# Death overs: 16–20
# ---------------------------
def classify_phase(over):
    if 1 <= over <= 6:
        return "powerplay"
    elif 7 <= over <= 15:
        return "middle"
    else:
        return "death"

df["phase"] = df["over"].apply(classify_phase)

# ---------------------------
# 4. Batter Rolling Features
# Rolling strike rate, boundary %, dot-ball %
# ---------------------------
df = df.sort_values(["batter", "match_id", "inning", "ball_number"])

df["is_dot_ball"] = (df["bat_runs"] == 0).astype(int)
df["is_boundary"] = df["runs_batter"].isin([4, 6]).astype(int)

# Rolling window = last 12 balls faced
window = 12

df["bat_rolling_runs"] = df.groupby("batter")["bat_runs"].transform(
    lambda x: x.rolling(window, min_periods=1).sum()
)

df["bat_rolling_balls"] = df.groupby("batter")["bat_runs"].cumcount() + 1
df["bat_strike_rate"] = (df["bat_rolling_runs"] / df["bat_rolling_balls"]) * 100

df["bat_boundary_pct"] = df.groupby("batter")["is_boundary"].transform(
    lambda x: x.rolling(window, min_periods=1).mean()
)

df["bat_dot_pct"] = df.groupby("batter")["is_dot_ball"].transform(
    lambda x: x.rolling(window, min_periods=1).mean()
)

# ---------------------------
# 5. Bowler Rolling Features
# Economy, wicket %, variance
# ---------------------------
df = df.sort_values(["bowler", "match_id", "inning", "ball_number"])

df["bowl_rolling_runs_conceded"] = df.groupby("bowler")["bat_runs"].transform(
    lambda x: x.rolling(window, min_periods=1).sum()
)

df["bowl_rolling_balls"] = df.groupby("bowler")["bat_runs"].cumcount() + 1
df["bowl_economy"] = (df["bowl_rolling_runs_conceded"] / df["bowl_rolling_balls"]) * 6

df["bowl_wicket_pct"] = df.groupby("bowler")["wicket_flag"].transform(
    lambda x: x.rolling(window, min_periods=1).mean()
)

# ---------------------------
# 6. Match Context Features
# Required run rate, wickets remaining
# ---------------------------
df = df.sort_values(["match_id", "inning", "ball_number"])

# cumulative score
df["inning_runs_cum"] = df.groupby(["match_id", "inning"])["bat_runs"].cumsum()

# wickets fallen
df["inning_wkts_cum"] = df.groupby(["match_id", "inning"])["wicket_flag"].cumsum()
df["wickets_in_hand"] = 10 - df["inning_wkts_cum"]

# Required Run Rate (if second innings)
def compute_rrr(x):
    if len(x) == 0:
        return np.nan
    target = x["inning_runs_cum"].max() + 1  # target = final score +1
    remaining_balls = 120 - (x["over"] * 6 + x["ball_in_over"])
    return (target - x["inning_runs_cum"]) * 6 / remaining_balls if remaining_balls > 0 else np.nan

df["required_run_rate"] = df.groupby(["match_id", "inning"]).apply(lambda x: compute_rrr(x)).reset_index(level=[0,1], drop=True)

# ---------------------------
# 7. Pressure Index
# Combines:
# - RRR
# - wickets in hand
# - phase weight
# ---------------------------
phase_weight = {"powerplay": 1, "middle": 1.5, "death": 2}

df["phase_weight"] = df["phase"].map(phase_weight)
df["pressure_index"] = (
    df["required_run_rate"].fillna(0) * df["phase_weight"] / (df["wickets_in_hand"] + 1)
)

# ---------------------------
# 8. Targets for ML
# xR regression target = runs_total next ball
# xW binary classification = wicket_flag next ball
# ---------------------------
df["xR_target"] = df.groupby(["match_id", "inning"])["bat_runs"].shift(-1)
df["xW_target"] = df.groupby(["match_id", "inning"])["wicket_flag"].shift(-1)

# Drop final ball of innings (no next ball)
df = df.dropna(subset=["xR_target", "xW_target"])

# ---------------------------
# 9. Save Output
# ---------------------------
output_csv = "cricsheet_features.csv"
df.to_csv(output_csv, index=False)

print(f"Feature engineering complete. Output saved as {output_csv}.")
print("Final rows:", len(df))
