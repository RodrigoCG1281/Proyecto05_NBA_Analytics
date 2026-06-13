"""
DAG de INGESTA + TRANSFORMACIÓN de datos NBA — Personas 2 y 3.

Responsabilidad: extraer datos crudos desde balldontlie.io, transformarlos
(calcular ganador, limpiar nombres, validar calidad) y producir parquets
limpios listos para la carga a PostgreSQL (Persona 4).

Flujo:
    extract_games({{ ds }})  →  extract_players(game_ids)
                                      ↓
                              [transform_games, transform_players]
                                      ↓
                                   validate_data

Salida:
    /opt/airflow/data/raw_games.parquet
    /opt/airflow/data/raw_players.parquet
    /opt/airflow/data/games_clean.parquet
    /opt/airflow/data/players_clean.parquet
"""
from __future__ import annotations

import datetime

import pendulum
from airflow.decorators import dag, task

# ─── Configuración por defecto compartida por todas las tareas ──────────────
_DEFAULT_ARGS = {
    "owner": "persona2",
    "retries": 3,
    "retry_exponential_backoff": True,
    # Primer reintento a los 30 s; luego 60 s, 120 s (backoff exponencial)
    "retry_delay": datetime.timedelta(seconds=30),
}


# ─── Definición del DAG ─────────────────────────────────────────────────────
@dag(
    dag_id="ingest_nba",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["nba", "ingesta", "transform", "persona2", "persona3"],
    doc_md="""
## DAG de Ingesta + Transformación NBA (Personas 2 y 3)
Extrae datos crudos de balldontlie.io, los transforma y valida.

| Archivo | Descripción |
|---|---|
| `raw_games.parquet` | Partidos del día (crudo) |
| `raw_players.parquet` | Estadísticas por jugador (crudo) |
| `games_clean.parquet` | Partidos con ganador calculado |
| `players_clean.parquet` | Jugadores con nombres normalizados |

La macro `{{ ds }}` garantiza idempotencia.
    """,
)
def ingest_nba():
    """Orquesta extracción, transformación y validación. La lógica vive en src/."""

    @task(task_id="extract_games")
    def extract_games() -> list:
        """
        Llama a GET /v1/games de balldontlie.io para la fecha del DAG run
        y escribe raw_games.parquet. Devuelve la lista de game_ids para
        pasarla a la siguiente tarea vía XCom.

        Los módulos se importan aquí (dentro de la función) para que
        Airflow no los cargue durante el parseo continuo del archivo DAG.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from extract_games import extract_games as _fn  # noqa: PLC0415

        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        game_date: str = ctx["ds"]

        return _fn(game_date)

    @task(task_id="extract_players")
    def extract_players(game_ids: list) -> None:
        """
        Llama a GET /v1/stats de balldontlie.io para cada game_id y escribe
        raw_players.parquet. Incluye rate limiting (0.6 s entre llamadas)
        para respetar el límite de 30 req/min del free tier.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from extract_players import extract_players as _fn  # noqa: PLC0415

        _fn(game_ids)

    @task(task_id="transform_games")
    def transform_games() -> int:
        """Lee raw_games.parquet, calcula winner, escribe games_clean.parquet."""
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from transform_nba import transform_games as _fn  # noqa: PLC0415

        return _fn()

    @task(task_id="transform_players")
    def transform_players() -> int:
        """Lee raw_players.parquet, normaliza nombres, escribe players_clean.parquet."""
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from transform_nba import transform_players as _fn  # noqa: PLC0415

        return _fn()

    @task(task_id="validate_data")
    def validate_data() -> dict:
        """Valida points >= 0 y game_id NOT NULL en ambos datasets limpios."""
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from transform_nba import validate_data as _fn  # noqa: PLC0415

        return _fn()

    # ── Dependencias ──
    game_ids = extract_games()
    players_done = extract_players(game_ids=game_ids)

    t_games = transform_games()
    t_players = transform_players()

    players_done >> t_games
    players_done >> t_players

    [t_games, t_players] >> validate_data()


# Instanciar el DAG (requerido por Airflow para registrarlo)
ingest_nba()
