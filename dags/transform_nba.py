"""
DAG de TRANSFORMACION de datos NBA — Persona 3.

Consume los parquets crudos generados por Persona 2 (ingesta),
limpia, enriquece y valida los datos, y produce parquets limpios
listos para la carga a PostgreSQL (Persona 4).

Flujo:
    transform_games() ----+
                          +---- validate_data()
    transform_players() --+

Entrada:
    /opt/airflow/data/raw_games.parquet
    /opt/airflow/data/raw_players.parquet

Salida:
    /opt/airflow/data/games_clean.parquet
    /opt/airflow/data/players_clean.parquet

Dependencia:
    Solo del esquema (contrato) definido por Persona 2.
    No requiere que el DAG de ingesta exista; puede trabajar con mocks.
"""
from __future__ import annotations

import datetime

import pendulum
from airflow.decorators import dag, task

_DEFAULT_ARGS = {
    "owner": "persona3",
    "retries": 2,
    "retry_delay": datetime.timedelta(seconds=30),
}


@dag(
    dag_id="transform_nba",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["nba", "transform", "clean", "persona3"],
    doc_md=r"""
## DAG de Transformacion NBA (Persona 3)

Consume los parquets crudos de Persona 2 y produce datos limpios.

| Entrada | Salida |
|---|---|
| `raw_games.parquet` | `games_clean.parquet` |
| `raw_players.parquet` | `players_clean.parquet` |

### Tareas
- **transform_games**: calcula ganador, limpia formato
- **transform_players**: normaliza nombres de jugadores
- **validate_data**: verifica integridad (points >= 0, game_id no nulo)
    """,
)
def transform_nba():
    """Orquesta las tareas de transformacion. La logica vive en src/."""

    @task(task_id="transform_games")
    def _transform_games() -> int:
        import sys

        sys.path.insert(0, "/opt/airflow/src")
        from transform_nba import transform_games as _fn  # noqa: PLC0415

        return _fn()

    @task(task_id="transform_players")
    def _transform_players() -> int:
        import sys

        sys.path.insert(0, "/opt/airflow/src")
        from transform_nba import transform_players as _fn  # noqa: PLC0415

        return _fn()

    @task(task_id="validate_data")
    def _validate_data() -> dict:
        import sys

        sys.path.insert(0, "/opt/airflow/src")
        from transform_nba import validate_data as _fn  # noqa: PLC0415

        return _fn()

    t_games = _transform_games()
    t_players = _transform_players()
    validation = _validate_data()

    # Transform en paralelo, luego validacion
    t_games >> validation
    t_players >> validation


transform_nba()
