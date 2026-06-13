"""
DAG de INGESTA de datos NBA — Persona 2.

Responsabilidad: extraer datos crudos desde balldontlie.io y guardarlos
como archivos parquet. No transforma ni carga a PostgreSQL.

Flujo:
    extract_games({{ ds }})  →  extract_players(game_ids)

Salida:
    /opt/airflow/data/raw_games.parquet
    /opt/airflow/data/raw_players.parquet
"""
from __future__ import annotations

import datetime

import pendulum
from airflow.decorators import dag, task

# ─── Configuración por defecto compartida por ambas tareas ──────────────────
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
    tags=["nba", "ingesta", "raw", "persona2"],
    doc_md="""
## DAG de Ingesta NBA (Persona 2)
Extrae datos crudos de la API de balldontlie.io y los guarda en parquet.

| Archivo de salida | Descripción |
|---|---|
| `raw_games.parquet` | Un registro por partido del día |
| `raw_players.parquet` | Un registro por jugador por partido |

La macro `{{ ds }}` garantiza idempotencia: reprocesar la misma fecha
sobreescribe el parquet con los mismos datos.
    """,
)
def ingest_nba():
    """Orquesta las dos tareas de extracción. Toda la lógica vive en src/."""

    @task(task_id="extract_games")
    def extract_games() -> list:
        """
        Llama a GET /v1/games de balldontlie.io para la fecha del DAG run
        y escribe raw_games.parquet. Devuelve la lista de game_ids para
        pasarla a la siguiente tarea vía XCom.

        Los módulos se importan aquí (dentro de la función) para que
        Airflow no los cargue durante el parseo continuo del archivo DAG.
        """
        # Importaciones pesadas solo cuando la tarea realmente corre
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from extract_games import extract_games as _fn  # noqa: PLC0415

        # get_current_context() es la forma estándar de leer macros en TaskFlow
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        game_date: str = ctx["ds"]  # formato "YYYY-MM-DD"

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

    # ── Dependencia explícita: los game_ids viajan de extract_games a extract_players
    game_ids = extract_games()
    extract_players(game_ids=game_ids)


# Instanciar el DAG (requerido por Airflow para registrarlo)
ingest_nba()
