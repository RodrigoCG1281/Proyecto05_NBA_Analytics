"""
DAG de CARGA/INTEGRACIÓN de datos NBA — Persona 4.

Lee los parquets transformados producidos por Persona 3,
construye el modelo analítico (dimensiones y hechos) y carga
los datos en PostgreSQL usando el Hook de Airflow.

Salida esperada (parquets intermedios):
    /opt/airflow/data/dim_game.parquet
    /opt/airflow/data/fact_player_stats.parquet
"""
from __future__ import annotations

import datetime
import logging
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
GAMES_CLEAN = DATA_DIR / "games_clean.parquet"
PLAYERS_CLEAN = DATA_DIR / "players_clean.parquet"
DIM_GAME_PQ = DATA_DIR / "dim_game.parquet"
FACT_PLAYER_PQ = DATA_DIR / "fact_player_stats.parquet"

_DEFAULT_ARGS = {
    "owner": "persona4",
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=30),
}


@dag(
    dag_id="load_nba",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["nba", "load", "persona4"],
    doc_md=r"""
## DAG de Carga NBA (Persona 4)

Construye `dim_game` y `fact_player_stats` a partir de los parquets limpios
y carga las tablas en PostgreSQL usando la conexión `postgres_nba`.
""",
)
def load_nba():
    """Orquesta la combinación y carga de los datasets transformados."""

    @task(task_id="combine_data")
    def _combine_data() -> dict:
        """Lee `games_clean` y `players_clean`, construye dimensiones y hechos,
        y escribe parquets intermedios listos para carga.

        Returns:
            dict: rutas escritas y filas por cada parquet.
        """
        import pandas as pd
        import sys

        logger.info("Combinando datasets limpios...")

        # leer parquets (si faltan, lanzar excepción para que Airflow marque fallo)
        if not GAMES_CLEAN.exists():
            raise FileNotFoundError(f"{GAMES_CLEAN} no encontrado")
        if not PLAYERS_CLEAN.exists():
            raise FileNotFoundError(f"{PLAYERS_CLEAN} no encontrado")

        games = pd.read_parquet(GAMES_CLEAN)
        players = pd.read_parquet(PLAYERS_CLEAN)

        # dim_game: una fila por game_id
        dim_game_cols = ["game_id", "date", "winner", "home_team", "away_team"]
        dim_game = games.copy()
        missing = [c for c in dim_game_cols if c not in dim_game.columns]
        if missing:
            raise ValueError(f"Faltan columnas en games_clean: {missing}")
        dim_game = dim_game[dim_game_cols].drop_duplicates(subset=["game_id"]).reset_index(drop=True)

        # fact_player_stats: una fila por player-game
        fact_cols = ["player_id", "game_id", "player_name", "points"]
        fact = players.copy()
        # Asegurar que existan las columnas requeridas; si faltan, crear con 0/NA
        if "player_id" not in fact.columns or "game_id" not in fact.columns:
            raise ValueError("players_clean debe contener 'player_id' y 'game_id'")
        if "player_name" not in fact.columns:
            raise ValueError("players_clean debe contener 'player_name'")
        if "points" not in fact.columns:
            fact["points"] = 0

        fact = fact[[c for c in fact_cols]]

        # Escribir parquets intermedios
        dim_game.to_parquet(DIM_GAME_PQ, index=False)
        fact.to_parquet(FACT_PLAYER_PQ, index=False)

        logger.info("Escritos: %s (%d filas), %s (%d filas)", DIM_GAME_PQ, len(dim_game), FACT_PLAYER_PQ, len(fact))

        return {
            "dim_game": {"path": str(DIM_GAME_PQ), "rows": int(len(dim_game))},
            "fact_player_stats": {"path": str(FACT_PLAYER_PQ), "rows": int(len(fact))},
        }

    @task(task_id="load_postgres")
    def _load_postgres(_meta: dict) -> dict:
        """Carga los parquets intermedios en PostgreSQL usando `postgres_nba`.

        Escribe primero `dim_game` y luego `fact_player_stats`.
        """
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger.info("Cargando datos a PostgreSQL via PostgresHook...")

        pg_hook = PostgresHook(postgres_conn_id="postgres_nba")
        engine = pg_hook.get_sqlalchemy_engine()

        # Leer parquets creados por combine_data
        dim = pd.read_parquet(DIM_GAME_PQ)
        fact = pd.read_parquet(FACT_PLAYER_PQ)

        # Cargar dimensiones primero
        dim.to_sql("dim_game", con=engine, if_exists="append", index=False)
        logger.info("Cargado dim_game (%d filas)", len(dim))

        # Luego hechos
        fact.to_sql("fact_player_stats", con=engine, if_exists="append", index=False)
        logger.info("Cargado fact_player_stats (%d filas)", len(fact))

        return {"dim_game_rows": int(len(dim)), "fact_rows": int(len(fact))}

    meta = _combine_data()
    load = _load_postgres(meta)

    meta >> load


load_nba()
