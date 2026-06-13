from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def test_extract():
    # Simular datos en bruto de la API
    data = [{"game_id": "123", "date": "2026-06-13", "home_team": "LAL", "away_team": "BOS"}]
    df = pd.DataFrame(data)
    # Probar que el volumen /opt/airflow/data funciona
    df.to_parquet('/opt/airflow/data/raw_games.parquet', index=False)
    print("Extracción simulada y guardada en Parquet.")

def test_transform():
    # Leer el parquet generado en el paso anterior
    df = pd.read_parquet('/opt/airflow/data/raw_games.parquet')
    # Simular una transformación (agregar columna de ganador)
    df['winner'] = 'LAL'
    df.to_parquet('/opt/airflow/data/games_clean.parquet', index=False)
    print("Transformación simulada y guardada.")

def test_load_postgres():
    # Leer datos limpios
    df = pd.read_parquet('/opt/airflow/data/games_clean.parquet')
    
    # Probar la conexión automática a Postgres usando el Hook de Airflow
    # Usa 'postgres_nba' que es el ID que configuramos automáticamente
    pg_hook = PostgresHook(postgres_conn_id='postgres_nba')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Insertar en la tabla dim_game creada por tu init-db.sql
    df.to_sql('dim_game', con=engine, if_exists='append', index=False)
    print("Datos cargados con éxito en PostgreSQL.")

with DAG(
    'test_infraestructura_nba',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    step1 = PythonOperator(task_id='simular_ingesta', python_callable=test_extract)
    step2 = PythonOperator(task_id='simular_transformacion', python_callable=test_transform)
    step3 = PythonOperator(task_id='simular_carga_postgres', python_callable=test_load_postgres)

    step1 >> step2 >> step3