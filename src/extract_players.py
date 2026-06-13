"""
Extracción de estadísticas de jugadores NBA desde balldontlie.io (v1/stats).
Responsabilidad (Persona 2): dado un listado de game_ids, obtener las
estadísticas por jugador y persistirlas en raw_players.parquet.

API: https://api.balldontlie.io/v1/stats
  Parámetros: game_ids[]=<id>, per_page=100, cursor=<next_cursor>
  Auth header: Authorization: <API_KEY>
"""
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.balldontlie.io/v1"
DATA_DIR = Path("/opt/airflow/data")
RAW_PLAYERS_PATH = DATA_DIR / "raw_players.parquet"

# Pausa entre partidos para respetar el rate limit (30 req/min en free tier)
_SLEEP_SECS = 0.6

# Contrato de datos de salida
_SCHEMA = {
    "player_id": "int64",
    "game_id": "object",
    "player_name": "object",
    "points": "int64",
}


# ---------------------------------------------------------------------------
# API pública del módulo
# ---------------------------------------------------------------------------

def extract_players(game_ids: list) -> None:
    """
    Extrae estadísticas de jugadores para cada game_id y escribe raw_players.parquet.

    Args:
        game_ids: Lista de strings con los IDs de partidos del día.
                  Proviene de extract_games vía XCom de Airflow.
    """
    if not game_ids:
        logger.warning("Lista de game_ids vacía. Se escribe parquet vacío.")
        _validate_and_write(_empty_df(), RAW_PLAYERS_PATH)
        return

    logger.info("Extrayendo stats para %d partido(s): %s", len(game_ids), game_ids)

    api_key = _get_api_key()
    all_records: list[dict] = []

    for i, game_id in enumerate(game_ids):
        if i > 0:
            time.sleep(_SLEEP_SECS)
        records = _fetch_player_stats(game_id, api_key)
        all_records.extend(records)
        logger.info("game_id=%s → %d jugadores", game_id, len(records))

    df = pd.DataFrame(all_records) if all_records else _empty_df()

    if not df.empty:
        antes = len(df)
        df = df.drop_duplicates(subset=["player_id", "game_id"], keep="first")
        if len(df) < antes:
            logger.info("Deduplicadas %d filas.", antes - len(df))

    _validate_and_write(df, RAW_PLAYERS_PATH)


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


def _fetch_player_stats(game_id: str, api_key: str) -> list[dict]:
    """
    Obtiene stats de jugadores para un partido con paginación de cursor.
    balldontlie puede retornar múltiples páginas si hay muchos jugadores.
    """
    logger.info("Consultando /v1/stats para game_id=%s", game_id)
    headers = {"Authorization": api_key}
    records = []
    cursor = None

    while True:
        params: dict = {"game_ids[]": game_id, "per_page": 100}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(
                f"{BASE_URL}/stats",
                headers=headers,
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as exc:
            logger.error("Error obteniendo stats para game_id=%s: %s", game_id, exc)
            break

        for stat in data.get("data", []):
            player = stat.get("player", {})
            nombre = f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            records.append({
                "player_id": int(player.get("id", 0)),
                "game_id": game_id,
                "player_name": nombre,
                # balldontlie devuelve None si el jugador no jugó (DNP) → 0
                "points": int(stat.get("pts") or 0),
            })

        # Avanzar a la siguiente página si existe
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

        time.sleep(0.3)  # pequeña pausa entre páginas

    return records


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SCHEMA.keys()))


def _validate_and_write(df: pd.DataFrame, path: Path) -> None:
    """Valida el esquema y escribe parquet. Falla antes de escribir datos corruptos."""
    missing = [c for c in _SCHEMA if c not in df.columns]
    if missing:
        raise ValueError(f"raw_players no cumple el contrato. Columnas faltantes: {missing}")

    df = df[list(_SCHEMA.keys())].copy()

    if not df.empty:
        df["player_id"] = df["player_id"].astype("int64")
        df["game_id"] = df["game_id"].astype(str)
        df["player_name"] = df["player_name"].astype(str)
        df["points"] = df["points"].astype("int64")

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("Escrito: %s (%d filas)", path, len(df))


# ---------------------------------------------------------------------------
# Prueba local sin Airflow: python src/extract_players.py 15612731 15612732
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
    RAW_PLAYERS_PATH = DATA_DIR / "raw_players.parquet"

    ids = sys.argv[1:] if len(sys.argv) > 1 else ["15612731"]
    extract_players(ids)
    print(pd.read_parquet(RAW_PLAYERS_PATH).head(20).to_string())
