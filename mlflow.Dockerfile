FROM python:3.12.11

USER root

COPY ./requirements-mlflow.txt /requirements-mlflow.txt
# INCLUDED IN DOCKER IMAGE
RUN pip3 install --no-cache-dir --upgrade --force-reinstall --requirement /requirements-mlflow.txt

EXPOSE ${MLFLOW_PORT}

ENV DB_CREDENTIALS="${RDS_USERNAME}:${RDS_PASSWORD}"
ENV DB_HOST="${RDS_ENDPOINT}:${RDS_PORT}"
ENV DB_OPTIONS="?options=-csearch_path=${MLFLOW_TARGET_SCHEMA}"
ENV MLFLOW_DB_URI="postgresql://${DB_CREDENTIALS}@${DB_HOST}/${SQL_DATABASE_NAME}${DB_OPTIONS}"

CMD mlflow db upgrade ${MLFLOW_DB_URI} && \
    mlflow server \
    --host "0.0.0.0" \
    --port ${MLFLOW_PORT} \
    --default-artifact-root ${MLFLOW_FILES_LOCATION} \
    --artifacts-destination ${MLFLOW_FILES_LOCATION} \
    --backend-store-uri ${MLFLOW_DB_URI}
