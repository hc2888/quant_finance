#!/bin/bash

# reset_airflow.sh - Reset Airflow metadata DB, flush Redis, and restart services

set -e

COMPOSE_FILE="airflow-docker-compose.yaml"
SERVICE_NAME="airflow-apiserver"
REDIS_SERVICE_NAME="redis"

echo "🔄 RESETTING AIRFLOW METADATA DATABASE INSIDE '${SERVICE_NAME}' CONTAINER..."
docker compose -f ${COMPOSE_FILE} exec --user airflow "${SERVICE_NAME}" airflow db reset --yes

echo "🧹 FLUSHING REDIS DATABASE INSIDE '${REDIS_SERVICE_NAME}' CONTAINER..."
docker compose -f ${COMPOSE_FILE} exec "${REDIS_SERVICE_NAME}" redis-cli FLUSHALL

echo "🧹 SHUTTING DOWN ALL AIRFLOW CONTAINERS AND REMOVING VOLUMES..."
docker compose -f ${COMPOSE_FILE} down --volumes --remove-orphans

echo "🚀 RESTARTING AIRFLOW SERVICES..."
docker compose -f ${COMPOSE_FILE} up -d

echo "✅ AIRFLOW RESET, REDIS FLUSH, AND RESTART COMPLETE."

echo "⚠️ IF FOR SOME REASON YOU GET A FORBIDDEN ACCESS ERROR AFTER A RESET, JUST RESET AIRFLOW AGAIN."
