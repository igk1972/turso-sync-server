FROM docker.io/prom/prometheus:v3.15.0
COPY prometheus.yml /etc/prometheus/prometheus.yml
