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