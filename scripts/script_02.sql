--¿Qué jugadores están mejorando con el tiempo?

SELECT 
    player_name,
    g.date,
    points
FROM fact_player_stats f
JOIN dim_game g ON f.game_id = g.game_id
ORDER BY player_name, g.date;

