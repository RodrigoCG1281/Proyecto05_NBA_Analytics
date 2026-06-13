#!/bin/bash

# Colores para la terminal
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}==> Configurando el entorno de Analítica NBA...${NC}"

# 1. Crear el archivo .env si no existe
if [ ! -f .env ]; then
    echo -e "${YELLOW}[INFO] Creando archivo .env basado en .env.example...${NC}"
    cp .env.example .env
else
    echo -e "${GREEN}[OK] El archivo .env ya existe.${NC}"
fi

# 2. Crear carpetas locales para los DAGs y Datos
echo -e "${YELLOW}[INFO] Asegurando existencia de carpetas locales...${NC}"
mkdir -p dags data

# 3. Inicializar la base de datos de Airflow
echo -e "${YELLOW}[INFO] Inicializando la base de datos de Airflow (airflow_db)...${NC}"
docker compose run --rm airflow-webserver airflow db init

# 4. Crear el usuario Administrador en Airflow
echo -e "${YELLOW}[INFO] Creando usuario administrador en Airflow...${NC}"
docker compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Persona1 \
    --lastname Infra \
    --role Admin \
    --email admin@nba.com \
    --password admin

# 5. Levantar todos los servicios en segundo plano
echo -e "${GREEN}==> Levantando todos los servicios en Docker...${NC}"
docker compose up -d

echo -e "${GREEN}==================================================================${NC}"
echo -e "${GREEN}¡Todo listo, Persona 1! El ecosistema está corriendo.${NC}"
echo -e "  - Airflow Webserver: http://localhost:8080 (User: admin / Pass: admin)"
echo -e "  - pgAdmin (Postgres GUI): http://localhost:5050 (User: admin@nba.com / Pass: admin)"
echo -e "==================================================================${NC}"