import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Create mock raw games
raw_games = pd.DataFrame([
    {"game_id": "1001", "date": "2026-01-15", "home_team": "LAL", "away_team": "BOS", "home_score": 112, "away_score": 98},
    {"game_id": "1002", "date": "2026-01-15", "home_team": "GSW", "away_team": "MIA", "home_score": 105, "away_score": 110},
    {"game_id": "1003", "date": "2026-01-15", "home_team": "CHI", "away_team": "NYK", "home_score": 95, "away_score": 100},
])
raw_games.to_parquet(DATA_DIR / "raw_games.parquet", index=False)
print(f"✓ raw_games.parquet: {len(raw_games)} filas")

# Create mock raw players
raw_players = pd.DataFrame([
    {"player_id": 101, "game_id": "1001", "player_name": "lebron james", "points": 28},
    {"player_id": 102, "game_id": "1001", "player_name": "anthony davis", "points": 22},
    {"player_id": 201, "game_id": "1002", "player_name": "stephen curry", "points": 35},
    {"player_id": 202, "game_id": "1002", "player_name": "klay thompson", "points": 18},
    {"player_id": 301, "game_id": "1003", "player_name": "demar derozan", "points": 25},
    {"player_id": 302, "game_id": "1003", "player_name": "julius randle", "points": 30},
])
raw_players.to_parquet(DATA_DIR / "raw_players.parquet", index=False)
print(f"✓ raw_players.parquet: {len(raw_players)} filas")

print("\nAhora ejecuta: python src/transform_nba.py")
