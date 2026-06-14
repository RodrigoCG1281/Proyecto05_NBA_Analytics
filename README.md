# Proyecto 5: Analítica de la NBA
Construir un pipeline de analítica deportiva usando datos de la NBA.

## Integrantes:
* **Apaza Vilca Tania Pamela**
* **Avila Agramonte Edgar**
* **Condori Gutierrez Rodrigo Bernardo**
* **Mamani Humpiri Isaac Joel**
* **Ramos Vargas Jaqueline Rocio**

---

## 1. Despliegue del Entorno (Linux-WSL2-Gitbash)

Abre tu terminal en la raíz del proyecto y ejecuta el script de automatización.

```bash
./manage.sh
```

### Accesos rapidos
Airflow Webserver: http://localhost:8080 (admin / admin)

pgAdmin (GUI Opcional de Postgres): http://localhost:5050 (admin@nba.com / admin)

### Verificación del Entorno por Consola
1. Ingresa a Airflow, enciende y ejecuta el DAG test_infraestructura_nba.
2. Cuando las 3 tareas terminen en verde, ejecuta el siguiente comando en tu terminal para verificar la inserción real en la base de datos sin usar entornos gráficos:

#### Verificar en WSL2 - Linux
```bash
docker compose exec postgres psql -U postgres -d nba_analytics -c "SELECT * FROM dim_game"
```

#### Verificar en Gitbash
```bash
MSYS_NO_PATHCONV=1 docker compose exec postgres psql -U postgres -d nba_analytics -c "SELECT * FROM dim_game;"
```


### Resultado esperado

| game_id | date | winner | home_team | away_team |
| :--- | :--- | :--- | :--- | :--- |
| 123 | 2026-06-13 | LAL | LAL | BOS |

*(1 row)*

---

## 2. DAG de Ingesta (`ingest_nba`)

Extrae datos crudos de la NBA desde [balldontlie.io](https://www.balldontlie.io) y los guarda como parquet. Se ejecuta diariamente de forma automática via Airflow.

```
extract_games(ds) ──> raw_games.parquet
                 └──> [game_ids] ──> extract_players ──> raw_players.parquet
```

### Archivos

| Archivo | Descripción |
| :--- | :--- |
| `dags/ingest_nba.py` | DAG de Airflow — orquesta las dos tareas |
| `src/extract_games.py` | Extrae partidos del día via `GET /v1/games` |
| `src/extract_players.py` | Extrae estadísticas de jugadores via `GET /v1/stats` |

### Contrato de datos (salida para etapas siguientes)

`data/raw_games.parquet`

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `game_id` | string | ID único del partido |
| `date` | string `YYYY-MM-DD` | Fecha del partido |
| `home_team` | string | Abreviación equipo local |
| `away_team` | string | Abreviación equipo visitante |
| `home_score` | Int64 (nullable) | Puntos local (`null` si no terminó) |
| `away_score` | Int64 (nullable) | Puntos visitante (`null` si no terminó) |

`data/raw_players.parquet`

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `player_id` | int64 | ID único del jugador |
| `game_id` | string | ID del partido |
| `player_name` | string | Nombre completo |
| `points` | int64 | Puntos anotados (`0` si no jugó) |

> Los archivos se escriben en `./data/`, montada en Docker como `/opt/airflow/data/`.

---

## 3. Cómo ejecutar la ingesta

### Paso 1 — Configurar la API Key

1. Regístrate gratis en [balldontlie.io](https://www.balldontlie.io) y obtén tu API key.
2. Edita el archivo `.env` de la raíz y agrega tu key:

```
BALLDONTLIE_API_KEY=tu_key_aqui
```

3. Aplica la configuración según tu situación:

**Si aún no levantaste el entorno** (es decir no corriste `./manage.sh` del paso 1). Ejecuta el siguiente comando:
```bash
./manage.sh
```

**Si el entorno ya está corriendo** (ya corriste `./manage.sh`). Ejecuta el siguiente comando:
```bash
docker compose up -d
```

`docker compose up -d` es seguro de correr varias veces — solo recrea los contenedores con la nueva variable sin perder datos.

### Paso 2 — Verificar que el DAG está registrado

1. Abre [http://localhost:8080](http://localhost:8080) (admin / admin)
2. Busca el DAG `ingest_nba`
3. Confirma que **Has import errors: false** en la columna de estado

### Paso 3 — Ejecutar

Activa el toggle del DAG y haz clic en ▶ **Trigger DAG** para ejecutar con la fecha de hoy.

Para una fecha específica (temporada 2023-2024):

```bash
docker compose exec airflow-scheduler airflow dags trigger ingest_nba \
  --logical-date 2024-01-15T00:00:00+00:00
```

Fechas con partidos garantizados: cualquier día entre **oct 2023 – jun 2024**.
Si no hay partidos ese día, los parquets se escriben vacíos sin error.

Para cargar una **temporada completa** (necesario para el análisis histórico), usa el comando de backfill. Airflow ejecuta el pipeline para cada día del rango en orden:

```bash
# Temporada 2023-2024 completa (regular season + playoffs)
docker compose exec airflow-scheduler airflow dags backfill ingest_nba \
  --start-date 2023-10-24 \
  --end-date 2024-06-17
```

> **Nota:** el backfill extrae cada día y lo pasa al siguiente paso del pipeline. La acumulación histórica ocurre en PostgreSQL (etapa de Integración y Carga). Los parquets son staging temporal — solo contienen el último día procesado.

### Paso 4 — Verificar el resultado

Cuando ambas tareas estén en verde en la grilla, ejecuta:

```bash
docker compose exec airflow-scheduler python -c "
import pandas as pd
g = pd.read_parquet('/opt/airflow/data/raw_games.parquet')
p = pd.read_parquet('/opt/airflow/data/raw_players.parquet')
print(f'=== raw_games: {len(g)} partidos ===')
print(g.to_string(index=False))
print(f'\n=== raw_players: {len(p)} jugadores ===')
print(p.head(10).to_string(index=False))
"
```

Resultado esperado para `2024-01-15`:

```
=== raw_games: 11 partidos ===
    game_id        date home_team away_team  home_score  away_score
 0022300555  2024-01-15       PHI       HOU         124         115
 0022300556  2024-01-15       DAL       NOP         125         120
 ...

=== raw_players: 270+ jugadores ===
 player_id     game_id     player_name  points
   1628415  0022300555   Dillon Brooks      18
   1630224  0022300555     Jalen Green      20
 ...
```

---

## 4. DAG de Carga (`load_nba`)

Construye el modelo analítico final y carga los datos en PostgreSQL.

### Flujo

```
combine_data ──> dim_game.parquet
             └──> fact_player_stats.parquet
             └──> load_postgres ──> dim_game
                                 └──> fact_player_stats
                                 └──> fact_team_game
```

### Archivos importantes

| Archivo | Descripción |
| :--- | :--- |
| `dags/load_nba.py` | DAG de Airflow — construye y carga `dim_game`, `fact_player_stats` y `fact_team_game` |
| `scripts/init-db.sql` | Esquema de base de datos inicial con las tablas `dim_game`, `fact_team_game` y `fact_player_stats` |

### Tablas en PostgreSQL

| Tabla | Descripción |
| :--- | :--- |
| `dim_game` | Dimensión de partidos con `game_id`, `date`, `winner`, `home_team`, `away_team` |
| `fact_team_game` | Hecho de equipos por partido con `game_id`, `team`, `result`, `date` |
| `fact_player_stats` | Hecho de estadísticas de jugadores con `player_id`, `game_id`, `player_name`, `points` |

### Verificación

```bash
docker compose exec postgres psql -U postgres -d nba_analytics -c "SELECT * FROM dim_game LIMIT 5;"
docker compose exec postgres psql -U postgres -d nba_analytics -c "SELECT * FROM fact_team_game LIMIT 5;"
docker compose exec postgres psql -U postgres -d nba_analytics -c "SELECT * FROM fact_player_stats LIMIT 5;"
```
