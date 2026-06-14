"""
Genera los parquets historicos de la temporada NBA 2025-26 usando nba_api.
Corre localmente (no requiere Docker ni Airflow).

Uso:
    python scripts/generate_historical_parquets.py

Genera en ./data/:
    raw_games.parquet
    raw_players.parquet
    games_clean.parquet
    players_clean.parquet

Requisitos (instalar localmente, NO en Docker):
    pip install nba_api pandas pyarrow
"""
from __future__ import annotations

import calendar
import logging
import re
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog, PlayerGameLogs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------

SEASON = "2025-26"
DATE_FILTER_MONTH = "2026-01"   # None = sin filtro; "YYYY-MM" = solo ese mes

DATA_DIR = Path(__file__).parent.parent / "data"

# Headers necesarios para evitar bloqueo de stats.nba.com
NBA_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}


# ---------------------------------------------------------------------------
# Paso 1: Obtener todos los partidos de la temporada
# ---------------------------------------------------------------------------

def fetch_all_games() -> pd.DataFrame:
    """
    Usa LeagueGameLog para obtener todos los partidos de la temporada.
    LeagueGameLog devuelve 2 filas por partido (una por equipo);
    aqui las combinamos en 1 fila por partido con home/away bien definidos.
    """
    logger.info("Obteniendo partidos de la temporada %s...", SEASON)

    log = LeagueGameLog(
        season=SEASON,
        league_id="00",
        headers=NBA_HEADERS,
        timeout=60,
    ).get_data_frames()[0]

    logger.info("LeagueGameLog: %d filas recibidas (2 por partido)", len(log))

    # MATCHUP formato: "LAL vs. BOS" = home | "LAL @ BOS" = away
    home_rows = log[log["MATCHUP"].str.contains(r"vs\.", na=False)].copy()
    away_rows = log[log["MATCHUP"].str.contains("@", na=False)].copy()

    home_df = home_rows.set_index("GAME_ID")[
        ["GAME_DATE", "TEAM_ABBREVIATION", "PTS"]
    ].rename(columns={
        "GAME_DATE": "date",
        "TEAM_ABBREVIATION": "home_team",
        "PTS": "home_score",
    })

    away_df = away_rows.set_index("GAME_ID")[
        ["TEAM_ABBREVIATION", "PTS"]
    ].rename(columns={
        "TEAM_ABBREVIATION": "away_team",
        "PTS": "away_score",
    })

    games = home_df.join(away_df, how="inner").reset_index()
    games = games.rename(columns={"GAME_ID": "game_id"})

    # Fecha: "2023-10-24T00:00:00" o "OCT 24, 2023" → "2023-10-24"
    games["date"] = pd.to_datetime(games["date"]).dt.strftime("%Y-%m-%d")

    # Calcular ganador
    games["winner"] = None
    mask = games["home_score"].notna() & games["away_score"].notna()
    games.loc[mask & (games["home_score"] > games["away_score"]), "winner"] = games.loc[mask & (games["home_score"] > games["away_score"]), "home_team"]
    games.loc[mask & (games["away_score"] > games["home_score"]), "winner"] = games.loc[mask & (games["away_score"] > games["home_score"]), "away_team"]

    games["game_id"] = games["game_id"].astype(str)
    games["home_score"] = games["home_score"].astype("Int64")
    games["away_score"] = games["away_score"].astype("Int64")

    logger.info("Partidos unicos encontrados: %d", len(games))

    if DATE_FILTER_MONTH:
        games = games[games["date"].str.startswith(DATE_FILTER_MONTH)].reset_index(drop=True)
        logger.info("Filtrado a %s: %d partidos", DATE_FILTER_MONTH, len(games))

    return games


# ---------------------------------------------------------------------------
# Paso 2: Obtener stats de jugadores (una sola llamada via PlayerGameLogs)
# ---------------------------------------------------------------------------

def fetch_all_player_stats(game_ids: list[str]) -> pd.DataFrame:
    """
    Usa PlayerGameLogs para obtener todas las stats del mes en una sola
    llamada a la API, filtrando luego por los game_ids del paso 1.
    """
    if DATE_FILTER_MONTH:
        year, month = map(int, DATE_FILTER_MONTH.split("-"))
        last_day = calendar.monthrange(year, month)[1]
        date_from = f"{month:02d}/01/{year}"
        date_to = f"{month:02d}/{last_day:02d}/{year}"
    else:
        date_from, date_to = None, None

    logger.info(
        "Obteniendo stats de jugadores con PlayerGameLogs (%s → %s)...",
        date_from or "inicio temporada", date_to or "hoy",
    )

    raw = PlayerGameLogs(
        season_nullable=SEASON,
        date_from_nullable=date_from,
        date_to_nullable=date_to,
        league_id_nullable="00",
        headers=NBA_HEADERS,
        timeout=60,
    ).get_data_frames()[0]

    logger.info("PlayerGameLogs: %d registros recibidos", len(raw))

    if raw.empty:
        return pd.DataFrame(columns=["player_id", "game_id", "player_name", "points"])

    game_ids_set = set(game_ids)
    df = raw[raw["GAME_ID"].isin(game_ids_set)][
        ["PLAYER_ID", "GAME_ID", "PLAYER_NAME", "PTS"]
    ].copy()
    df = df.rename(columns={
        "PLAYER_ID": "player_id",
        "GAME_ID": "game_id",
        "PLAYER_NAME": "player_name",
        "PTS": "points",
    })
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    df = df.dropna(subset=["player_id"])
    df["player_id"] = df["player_id"].astype("int64")
    df["points"] = pd.to_numeric(df["points"], errors="coerce").fillna(0).astype("int64")

    logger.info("Registros de jugadores para los partidos seleccionados: %d", len(df))
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Transformaciones (misma logica que src/transform_nba.py)
# ---------------------------------------------------------------------------

def _clean_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return name
    name = name.strip().title()
    name = re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), name)
    name = re.sub(r"\bMac([a-z])", lambda m: "Mac" + m.group(1).upper(), name)
    name = re.sub(r"\bO'([a-z])", lambda m: "O'" + m.group(1).upper(), name)
    name = re.sub(r"-([a-z])", lambda m: "-" + m.group(1).upper(), name)
    return name


# ---------------------------------------------------------------------------
# Paso 3: Escribir los 4 parquets
# ---------------------------------------------------------------------------

def write_parquets(games: pd.DataFrame, players: pd.DataFrame) -> None:
    DATA_DIR.mkdir(exist_ok=True)

    # raw_games.parquet
    raw_games = games[["game_id", "date", "home_team", "away_team", "home_score", "away_score"]].copy()
    raw_games.to_parquet(DATA_DIR / "raw_games.parquet", index=False, engine="pyarrow")
    logger.info("raw_games.parquet: %d filas", len(raw_games))

    # games_clean.parquet
    games_clean = games[["game_id", "date", "winner", "home_team", "away_team"]].copy()
    games_clean.to_parquet(DATA_DIR / "games_clean.parquet", index=False, engine="pyarrow")
    logger.info("games_clean.parquet: %d filas", len(games_clean))

    # raw_players.parquet
    raw_players = players[["player_id", "game_id", "player_name", "points"]].copy()
    raw_players["player_id"] = raw_players["player_id"].astype("int64")
    raw_players["points"] = raw_players["points"].astype("int64")
    raw_players.to_parquet(DATA_DIR / "raw_players.parquet", index=False, engine="pyarrow")
    logger.info("raw_players.parquet: %d filas", len(raw_players))

    # players_clean.parquet
    players_clean = raw_players.copy()
    players_clean["player_name"] = players_clean["player_name"].apply(_clean_name)
    players_clean.to_parquet(DATA_DIR / "players_clean.parquet", index=False, engine="pyarrow")
    logger.info("players_clean.parquet: %d filas", len(players_clean))



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logger.info("=== Generando parquets historicos NBA %s ===", SEASON)

    games = fetch_all_games()
    if games.empty:
        logger.error("No se obtuvieron partidos. Verifica conexion a internet.")
        sys.exit(1)

    game_ids = games["game_id"].tolist()
    players = fetch_all_player_stats(game_ids)
    logger.info("Total registros de jugadores: %d", len(players))

    write_parquets(games, players)

    logger.info("=== Completado. Parquets en: %s ===", DATA_DIR.resolve())
    logger.info("Siguiente paso:")
    logger.info("  git add data/*.parquet")
    logger.info("  git commit -m 'Add 2025-26 historical parquets'")
    logger.info("  git push")
