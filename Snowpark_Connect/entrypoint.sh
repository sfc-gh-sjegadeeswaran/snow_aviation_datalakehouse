#!/bin/bash
set -e

cat <<EOF > /app/.snowflake/connections.toml
[spark-connect]
host = "${SNOWFLAKE_HOST}"
port = "443"
protocol = "https"
account = "${SNOWFLAKE_ACCOUNT}"
authenticator = "oauth"
token = "$(cat /snowflake/session/token)"
warehouse = "${SNOWFLAKE_WAREHOUSE}"
database = "${SNOWFLAKE_DATABASE}"
schema = "${SNOWFLAKE_SCHEMA}"
role = "${SNOWFLAKE_ROLE}"
compute_pool = "${COMPUTE_POOL}"
client_session_keep_alive = true
EOF

chmod 600 /app/.snowflake/connections.toml

export SNOWFLAKE_HOME=/app/.snowflake
export SNOWFLAKE_DEFAULT_CONNECTION_NAME=spark-connect

snowpark-submit \
  --snowflake-connection-name spark-connect \
  /app/pyspark_qar_analytics.py \
  --name "QAR_ANALYTICS"
