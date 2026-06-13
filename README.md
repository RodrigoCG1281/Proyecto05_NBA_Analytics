# Proyecto 5: Analítica de la NBA

## Integrantes:
* **Apaza Vilca Tania Pamela** 
* **Avila Agramonte Edgar** 
* **Condori Gutierrez Rodrigo Bernardo** 
* **Mamani Humpiri Isaac Joel** 
* **Ramos Vargas Jaqueline Rocio** 

Este repositorio contiene la configuración de infraestructura centralizada y automatizada para el proyecto de analítica de la NBA. Utiliza **Docker Compose** para garantizar que todo el equipo trabaje exactamente bajo el mismo entorno controlado (Apache Airflow, PostgreSQL y pgAdmin) sin necesidad de instalaciones locales complejas.

---

## 1. Despliegue del Entorno

### Opción A: Si usas Linux o Windows con WSL2 (Ubuntu)
Abre tu terminal en la raíz del proyecto y ejecuta el script de automatización (asegúrate de darle permisos de ejecución la primera vez):

```bash
chmod +x manage.sh
./manage.sh
```

