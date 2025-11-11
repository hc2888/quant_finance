FROM apache/airflow:3.1.0-python3.12
# PYTHON VERSION 3.12.11
USER root

# SETTING NVIDIA CONFIGURATIONS
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

# NOTE: SOURCE `docker-compose.yaml` FILE
# https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
# NOTE: YAML ENV VARIABLES; REVIEW, DELETE, CHANGE etc. WITH EVERY AIRFLOW UPGRADE
# https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html

# Switch to the 'airflow' user so that all subsequent pip installs occur in the same environment
# where Apache Airflow itself is installed (/home/airflow/.local). Installing as `USER root` would place
# packages in a different site-packages directory (e.g., /usr/local/lib), causing Airflow to lose
# access to its dependencies at runtime when it runs under the 'airflow' user.
USER airflow

COPY ./requirements-airflow.txt /requirements-airflow.txt
RUN pip3 install --no-cache-dir --upgrade --force-reinstall --requirement /requirements-airflow.txt

# Copy the local vectorbt.pro source folder into the container under the airflow user's home directory.
# The --chown flag ensures all copied files are owned by the airflow user (so pip can read/write during install)
# and belong to the root group, preventing permission errors when building the package.
COPY --chown=airflow:root ./vectorbt.pro /home/airflow/vectorbt.pro
COPY ./requirements-constraints.txt /requirements-constraints.txt
# Recursively grant the airflow user read, write, and execute (for directories) permissions on the vectorbt.pro folder.
# This ensures pip can create build artifacts like vectorbtpro.egg-info during installation without permission errors.
RUN chmod -R u+rwX /home/airflow/vectorbt.pro
RUN pip3 install --no-cache-dir --upgrade --force-reinstall \
    --constraint /requirements-constraints.txt /home/airflow/vectorbt.pro
