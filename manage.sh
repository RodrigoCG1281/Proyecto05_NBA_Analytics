#!/bin/bash

# Colores para la terminal
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}==> Configurando el entorno de Analítica NBA...${NC}"

# 1. Detectar si estamos en Git Bash de Windows para aplicar el fix de rutas
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    echo -e "${YELLOW}[INFO] Entorno Windows detectado (Git Bash). Aplicando parches...${NC}"
    export MSYS_NO_PATHCONV=1
fi

# 2. Crear el archivo .env si no existe
if [ ! -f .env ]; then
    echo -e "${YELLOW}[INFO] Creando archivo .env basado en .env.example...${NC}"
    cp .env.example .env
fi

# 3. Crear carpetas locales de forma automática
mkdir -p dags data

# 4. Asegurar permisos totales en las carpetas para evitar bloqueos en Docker
chmod -R 777 dags data 2>/dev/null

# 5. Inicializar la base de datos interna de Airflow
echo -e "${YELLOW}[INFO] Inicializando la base de datos de Airflow...${NC}"
docker compose run --rm airflow-webserver airflow db init

# 6. Crear el usuario Administrador por defecto
echo -e "${YELLOW}[INFO] Creando usuario administrador (admin/admin)...${NC}"
docker compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Persona1 \
    --lastname Infra \
    --role Admin \
    --email admin@nba.com \
    --password admin

# 7. Levantar todos los servicios en segundo plano
echo -e "${GREEN}==> Levantando todos los servicios en Docker...${NC}"
docker compose up -d

echo -e "${GREEN}==================================================================${NC}"
echo -e "${GREEN}¡Todo listo! El ecosistema está corriendo sin configuraciones manuales.${NC}"
echo -e "  - Airflow Webserver: http://localhost:8080 (User: admin / Pass: admin)"
echo -e "  - pgAdmin (Postgres GUI): http://localhost:5050 (User: admin@nba.com / Pass: admin)"
echo -e "==================================================================${NC}"