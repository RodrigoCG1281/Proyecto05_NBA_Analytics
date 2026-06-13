"""
Extracción de partidos NBA desde balldontlie.io (v1/games).
Responsabilidad (Persona 2): obtener los partidos de un día concreto,
construir el DataFrame según el contrato de datos y persistirlo en parquet.

API: https://api.balldontlie.io/v1/games
  Parámetros: dates[]=YYYY-MM-DD, per_page=100
  Auth header: Authorization: <API_KEY>
"""
import logging
import os
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.balldontlie.io/v1"
DATA_DIR = Path("/opt/airflow/data")
RAW_GAMES_PATH = DATA_DIR / "raw_games.parquet"

# Contrato de datos de salida
_SCHEMA = {
    "game_id": "object",
    "date": "object",
    "home_team": "object",
    "away_team": "object",
    "home_score": "Int64",  # nullable — None si el partido no terminó
    "away_score": "Int64",
}


# ---------------------------------------------------------------------------
# API pública del módulo
# ---------------------------------------------------------------------------

def extract_games(game_date: str) -> list:
    """
    Extrae los partidos del día desde balldontlie.io y escribe raw_games.parquet.

    Args:
        game_date: Fecha en formato "YYYY-MM-DD" (macro {{ ds }} de Airflow).

    Returns:
        Lista de strings con los game_id del día (vacía si no hay partidos).
    """
    logger.info("Extrayendo partidos para fecha: %s", game_date)

    api_key = _get_api_key()
    games = _fetch_games(game_date, api_key)

    if not games:
        logger.warning("Sin partidos para %s. Se escribe parquet vacío.", game_date)
        _validate_and_write(_empty_df(), RAW_GAMES_PATH)
        return []

    records = [_build_record(g, game_date) for g in games]

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["game_id"], keep="first")

    _validate_and_write(df, RAW_GAMES_PATH)

    game_ids = df["game_id"].tolist()
    logger.info("Partidos extraídos (%d): %s", len(game_ids), game_ids)
    return game_ids


# ---------------------------------------------------------------------------
# Funciones internas
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    key = os.environ.get("BALLDONTLIE_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "Variable de entorno BALLDONTLIE_API_KEY no encontrada. "
            "Agrégala al .env y reinicia los contenedores."
        )
    return key


def _fetch_games(game_date: str, api_key: str) -> list:
    """
    Llama a GET /v1/games filtrando por fecha.
    balldontlie usa cursor-based pagination; per_page=100 cubre cualquier día.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/games",
            headers={"Authorization": api_key},
            params={"dates[]": game_date, "per_page": 100},
            timeout=30,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Error llamando a balldontlie /games ({game_date}): {exc}") from exc

    return resp.json().get("data", [])


def _build_record(game: dict, game_date: str) -> dict:
    """Mapea un objeto game de la API al esquema del contrato."""
    home_score = game.get("home_team_score")
    away_score = game.get("visitor_team_score")

    return {
        "game_id": str(game["id"]),
        "date": game_date,
        "home_team": game["home_team"]["abbreviation"],
        "away_team": game["visitor_team"]["abbreviation"],
        # Si el partido no terminó la API devuelve 0; lo tratamos como None
        "home_score": int(home_score) if home_score else None,
        "away_score": int(away_score) if away_score else None,
    }


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SCHEMA.keys()))


def _validate_and_write(df: pd.DataFrame, path: Path) -> None:
    """Valida el esquema y escribe parquet. Falla antes de escribir datos corruptos."""
    missing = [c for c in _SCHEMA if c not in df.columns]
    if missing:
        raise ValueError(f"raw_games no cumple el contrato. Columnas faltantes: {missing}")

    df = df[list(_SCHEMA.keys())].copy()

    if not df.empty:
        df["game_id"] = df["game_id"].astype(str)
        df["date"] = df["date"].astype(str)
        df["home_team"] = df["home_team"].astype(str)
        df["away_team"] = df["away_team"].astype(str)
        df["home_score"] = df["home_score"].astype("Int64")
        df["away_score"] = df["away_score"].astype("Int64")

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("Escrito: %s (%d filas)", path, len(df))


# ---------------------------------------------------------------------------
# Prueba local sin Airflow: python src/extract_games.py 2024-01-15
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Cargar .env local si existe
    _env = Path(".env")
    if _env.exists():
        for _line in _env.read_text().splitlines():
            if "=" in _line and not _line.startswith("#"):
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

    DATA_DIR = Path("data")
    RAW_GAMES_PATH = DATA_DIR / "raw_games.parquet"

    fecha = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"
    ids = extract_games(fecha)
    print(f"\ngame_ids: {ids}")
    print(pd.read_parquet(RAW_GAMES_PATH).to_string())
