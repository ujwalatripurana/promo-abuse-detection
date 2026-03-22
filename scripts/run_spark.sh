set -euo pipefail

# Submit the Spark Structured Streaming job inside the spark container with required packages.
# Kafka integration and Postgres JDBC driver are fetched automatically.
docker compose -f docker-compose.yml exec spark bash -lc '\
  /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    /opt/stream/main_stream.py \
'
