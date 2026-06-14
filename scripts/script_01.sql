--¿Qué jugador anota más puntos?

SELECT player_name, SUM(points) AS total_points
FROM fact_player_stats
GROUP BY player_name
ORDER BY total_points DESC;
