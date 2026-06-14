-- 1. Crear la base de datos interna que usará Airflow para funcionar
CREATE DATABASE airflow_db;

-- 2. Conectarse a la base de datos del proyecto NBA
\c nba_analytics;

-- 3. Crear la tabla de Dimensión de Partidos (Contrato para Persona 3 y 4)
CREATE TABLE IF NOT EXISTS dim_game (
    game_id VARCHAR(50) PRIMARY KEY,
    date DATE,
    winner VARCHAR(100),
    home_team VARCHAR(50),
    away_team VARCHAR(50)
);

-- 4. Crear la tabla de Hechos de Equipos por Partido
CREATE TABLE IF NOT EXISTS fact_team_game (
    game_id VARCHAR(50) NOT NULL,
    team VARCHAR(50) NOT NULL,
    result CHAR(1) NOT NULL,  -- 'W' = Win, 'L' = Loss
    date DATE NOT NULL,
    PRIMARY KEY (game_id, team),
    CONSTRAINT fk_game
        FOREIGN KEY (game_id)
        REFERENCES dim_game(game_id)
);

-- 5. Crear la tabla de Hechos de Estadísticas de Jugadores
CREATE TABLE IF NOT EXISTS fact_player_stats (
    player_id VARCHAR(50),
    game_id VARCHAR(50),
    player_name VARCHAR(150),
    points INT,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (game_id) REFERENCES dim_game(game_id)
);