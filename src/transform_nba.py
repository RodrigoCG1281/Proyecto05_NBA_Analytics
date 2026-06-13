"""
Transformación de datos NBA — Persona 3.

Responsabilidad: consumir los parquets crudos generados por Persona 2,
limpiar, enriquecer y validar los datos, y producir parquets limpios
listos para la carga a PostgreSQL (Persona 4).

Dependencia: solo del esquema (contrato) definido por Persona 2.
No requiere que el DAG de ingesta exista; puede trabajar con mocks.

Flujo:
    raw_games.parquet   -> transform_games()   -> games_clean.parquet
    raw_players.parquet -> transform_players() -> players_clean.parquet
    games_clean.parquet + players_clean.parquet -> validate_data()
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
RAW_GAMES_PATH = DATA_DIR / "raw_games.parquet"
RAW_PLAYERS_PATH = DATA_DIR / "raw_players.parquet"
CLEAN_GAMES_PATH = DATA_DIR / "games_clean.parquet"
CLEAN_PLAYERS_PATH = DATA_DIR / "players_clean.parquet"

_GAMES_CLEAN_SCHEMA = {
    "game_id": "object",
    "date": "object",
    "winner": "object",
    "home_team": "object",
    "away_team": "object",
}

_PLAYERS_CLEAN_SCHEMA = {
    "player_id": "int64",
    "game_id": "object",
    "player_name": "object",
    "points": "int64",
}


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def transform_games() -> int:
    """
    Lee raw_games.parquet, calcula el ganador, escribe games_clean.parquet.

    Reglas:
        - winner = home_team si home_score > away_score
        - winner = away_team si away_score > home_score
        - winner = None si scores nulos o empate

    Returns:
        Número de filas escritas.
    """
    logger.info("Transformando games...")

    if not RAW_GAMES_PATH.exists():
        logger.warning("No se encuentra %%s. Se escribe parquet vacio.", RAW_GAMES_PATH)
        _write_empty(CLEAN_GAMES_PATH, _GAMES_CLEAN_SCHEMA)
        return 0

    df = pd.read_parquet(RAW_GAMES_PATH)

    if df.empty:
        logger.warning("raw_games.parquet vacio. Se escribe parquet vacio.")
        _write_empty(CLEAN_GAMES_PATH, _GAMES_CLEAN_SCHEMA)
        return 0

    _compute_winner(df)

    out = df[list(_GAMES_CLEAN_SCHEMA.keys())].copy()
    _validate_and_write(out, CLEAN_GAMES_PATH, _GAMES_CLEAN_SCHEMA)

    return len(out)


def transform_players() -> int:
    """
    Lee raw_players.parquet, limpia nombres, escribe players_clean.parquet.

    Limpieza de nombres:
        - Title case via str.title()
        - Corrige prefijos Mc/Mac (McDonald, MacArthur)
        - Corrige prefijo O' (O'Neal)
        - Corrige partes hipenadas (Jean-Pierre)

    Returns:
        Numero de filas escritas.
    """
    logger.info("Transformando players...")

    if not RAW_PLAYERS_PATH.exists():
        logger.warning("No se encuentra %%s. Se escribe parquet vacio.", RAW_PLAYERS_PATH)
        _write_empty(CLEAN_PLAYERS_PATH, _PLAYERS_CLEAN_SCHEMA)
        return 0

    df = pd.read_parquet(RAW_PLAYERS_PATH)

    if df.empty:
        logger.warning("raw_players.parquet vacio. Se escribe parquet vacio.")
        _write_empty(CLEAN_PLAYERS_PATH, _PLAYERS_CLEAN_SCHEMA)
        return 0

    df["player_name"] = df["player_name"].apply(_clean_name)

    out = df[list(_PLAYERS_CLEAN_SCHEMA.keys())].copy()
    _validate_and_write(out, CLEAN_PLAYERS_PATH, _PLAYERS_CLEAN_SCHEMA)

    return len(out)


def validate_data() -> dict:
    """
    Valida la calidad de los datos transformados.

    Verificaciones:
        - points >= 0 en todos los registros de jugadores
        - game_id no nulo en ambos datasets

    Returns:
        Dict con resultados de validacion (passed: bool).
    """
    logger.info("Validando datos transformados...")

    results: dict[str, bool | dict] = {
        "games_clean": {},
        "players_clean": {},
        "passed": True,
    }

    # ── games_clean ──
    if CLEAN_GAMES_PATH.exists():
        games = pd.read_parquet(CLEAN_GAMES_PATH)
        null_gids = int(games["game_id"].isna().sum())
        assert null_gids == 0, f"games_clean: {null_gids} game_id(s) nulo(s)"
        results["games_clean"]["filas"] = len(games)
        results["games_clean"]["game_id_null"] = null_gids
        logger.info("games_clean: %d filas, 0 game_id nulos", len(games))
    else:
        logger.warning("games_clean.parquet no encontrado.")
        results["games_clean"]["error"] = "archivo no encontrado"

    # ── players_clean ──
    if CLEAN_PLAYERS_PATH.exists():
        players = pd.read_parquet(CLEAN_PLAYERS_PATH)

        null_gids = int(players["game_id"].isna().sum())
        assert null_gids == 0, f"players_clean: {null_gids} game_id(s) nulo(s)"

        neg_pts = int((players["points"] < 0).sum())
        assert neg_pts == 0, f"players_clean: {neg_pts} registro(s) con points < 0"

        results["players_clean"]["filas"] = len(players)
        results["players_clean"]["game_id_null"] = null_gids
        results["players_clean"]["points_negative"] = neg_pts
        logger.info(
            "players_clean: %d filas, 0 nulos, 0 puntos negativos", len(players)
        )
    else:
        logger.warning("players_clean.parquet no encontrado.")
        results["players_clean"]["error"] = "archivo no encontrado"

    results["passed"] = all(
        "error" not in v for v in results.values() if isinstance(v, dict)
    )

    if results["passed"]:
        logger.info("Validacion exitosa.")
    else:
        logger.error("Validacion fallida.")

    return results


# ---------------------------------------------------------------------------
# Funciones internas
# ---------------------------------------------------------------------------

def _compute_winner(df: pd.DataFrame) -> None:
    """Agrega columna 'winner' en base a home_score vs away_score."""
    has_scores = df["home_score"].notna() & df["away_score"].notna()

    df["winner"] = None

    df.loc[has_scores & (df["home_score"] > df["away_score"]), "winner"] = df[
        "home_team"
    ]
    df.loc[has_scores & (df["away_score"] > df["home_score"]), "winner"] = df[
        "away_team"
    ]


def _clean_name(name: str) -> str:
    """
    Normaliza el casing de un nombre de jugador.

    Ejemplos:
        "lebron james"  -> "LeBron James"
        "mcdonald"      -> "McDonald"
        "o'neal"        -> "O'Neal"
        "jean-pierre"   -> "Jean-Pierre"
    """
    if not isinstance(name, str) or not name.strip():
        return name

    name = name.strip()

    # title() maneja la mayoria
    name = name.title()

    # Mc + mayuscula (McDonald, McAdoo…)
    name = re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), name)

    # Mac + mayuscula (MacArthur…)
    name = re.sub(r"\bMac([a-z])", lambda m: "Mac" + m.group(1).upper(), name)

    # O' + mayuscula (O'Neal, O'Brien…)
    name = re.sub(r"\bO'([a-z])", lambda m: "O'" + m.group(1).upper(), name)

    # Partes hipenadas (Jean-Pierre, Joe-John…)
    name = re.sub(r"-([a-z])", lambda m: "-" + m.group(1).upper(), name)

    return name


def _validate_and_write(df: pd.DataFrame, path: Path, schema: dict) -> None:
    """Valida columnas contra esquema y escribe parquet."""
    missing = [c for c in schema if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en {path.name}: {missing}")

    df = df[list(schema.keys())].copy()

    if not df.empty:
        for col, dtype in schema.items():
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"Error casteando columna '{col}' a {dtype}: {exc}"
                ) from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("Escrito: %s (%d filas)", path, len(df))


def _write_empty(path: Path, schema: dict) -> None:
    """Escribe un parquet vacio con el esquema correcto."""
    df = pd.DataFrame(columns=list(schema.keys()))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("Escrito (vacio): %s", path)


# ---------------------------------------------------------------------------
# Prueba local: python src/transform_nba.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    # ── Mocks para pruebas locales ──
    DATA_DIR = Path("data")
    RAW_GAMES_PATH = DATA_DIR / "raw_games.parquet"
    RAW_PLAYERS_PATH = DATA_DIR / "raw_players.parquet"
    CLEAN_GAMES_PATH = DATA_DIR / "games_clean.parquet"
    CLEAN_PLAYERS_PATH = DATA_DIR / "players_clean.parquet"

    # Si no hay datos reales, crear mocks
    if not RAW_GAMES_PATH.exists():
        logger.info("Creando mocks para pruebas locales...")
        mock_games = pd.DataFrame([
            {"game_id": "1001", "date": "2026-01-15", "home_team": "LAL",
             "away_team": "BOS", "home_score": 112, "away_score": 98},
            {"game_id": "1002", "date": "2026-01-15", "home_team": "GSW",
             "away_team": "MIA", "home_score": 105, "away_score": 110},
        ])
        RAW_GAMES_PATH.parent.mkdir(parents=True, exist_ok=True)
        mock_games.to_parquet(RAW_GAMES_PATH, index=False)

        mock_players = pd.DataFrame([
            {"player_id": 1, "game_id": "1001", "player_name": "lebron james", "points": 28},
            {"player_id": 2, "game_id": "1001", "player_name": "anthony davis", "points": 22},
            {"player_id": 3, "game_id": "1002", "player_name": "stephen curry", "points": 35},
            {"player_id": 4, "game_id": "1002", "player_name": "jimmy butler", "points": 27},
        ])
        mock_players.to_parquet(RAW_PLAYERS_PATH, index=False)
        logger.info("Mocks creados.")

    n = transform_games()
    print(f"\ngames_clean: {n} filas")
    if CLEAN_GAMES_PATH.exists():
        print(pd.read_parquet(CLEAN_GAMES_PATH).to_string())

    m = transform_players()
    print(f"\nplayers_clean: {m} filas")
    if CLEAN_PLAYERS_PATH.exists():
        print(pd.read_parquet(CLEAN_PLAYERS_PATH).to_string())

    results = validate_data()
    print(f"\nValidacion: {'PASO' if results['passed'] else 'FALLO'}")
