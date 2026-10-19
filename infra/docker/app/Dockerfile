# CacheMind application image: Python 3.12 + JRE 17 for PySpark 4.0.4.
# Used for the streaming job, batch jobs, producer and dashboard in Profile C.
FROM eclipse-temurin:17-jre-noble

RUN apt-get update && apt-get install -y --no-install-recommends python3.12 python3.12-venv \
      ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --home-dir /home/cachemind cachemind \
 && mkdir -p /data /app && chown cachemind /data /app
USER cachemind
WORKDIR /app
ENV VIRTUAL_ENV=/app/.venv PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 \
    CACHEMIND_DATA_DIR=/data CACHEMIND_ARTIFACTS_DIR=/data/artifacts \
    CACHEMIND_REPORTS_DIR=/data/reports CACHEMIND_RESPECT_JAVA_HOME=1
COPY --chown=cachemind requirements.lock pyproject.toml README.md ./
RUN python3.12 -m venv .venv && pip install --no-cache-dir "pip==25.2" \
 && pip install --no-cache-dir -r requirements.lock
COPY --chown=cachemind src ./src
COPY --chown=cachemind sql ./sql
COPY --chown=cachemind infra ./infra
COPY --chown=cachemind .streamlit ./.streamlit
RUN pip install --no-cache-dir --no-deps -e . \
 # resolve the Spark Kafka connector once at build time (versions pinned to Spark's)
 && python -c "from cachemind.bigdata.spark_session import get_spark; get_spark('warmup', kafka=True).stop()"
CMD ["python", "-m", "cachemind", "--help"]
