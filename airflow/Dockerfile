FROM apache/airflow:2.10.2-python3.12

USER root
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 apt-transport-https ca-certificates \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 unixodbc-dev \
    && apt-get purge -y curl gnupg2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# mssql-tools18 (bcp) installs to /opt/mssql-tools18/bin, which is not on
# PATH by default — needed by generate_and_load in bulk_load_dimensions.py.
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY scripts /opt/airflow/scripts
