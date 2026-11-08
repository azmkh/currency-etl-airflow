# ---------------------------------------------------------------------------
# Airflow image for the Currency Exchange ETL project.
# The base image is Astronomer's Astro Runtime (Apache Airflow 3).
# ---------------------------------------------------------------------------
FROM astrocrpublic.azurecr.io/runtime:3.3-12

# Meta details (labels) attached to the image.
# Inspect them with:  docker image inspect <image> --format "{{json .Config.Labels}}"
LABEL maintainer="madeeha.mailz@gmail.com" \
      org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Airflow ETL pipeline: Frankfurter API -> transform -> MongoDB" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.authors="madeeha.mailz@gmail.com" \
      org.opencontainers.image.vendor="MLOps Course - Task 3" \
      org.opencontainers.image.licenses="MIT" \
      project.component="airflow" \
      project.course="MLOps"
