FROM python:3.9-alpine

ENV MYSQL_USERNAME ""
ENV MYSQL_PASSWORD ""
ENV MYSQL_HOST ""
ENV MYSQL_DATABASE ""
ENV MYSQL_TABLE "queries"
ENV MYSQL_USERNAME ""
ENV PIHOLE_ENRICH_BATCH_SIZE 10000
ENV PIHOLE_ENRICH_CYCLE_INTERVAL 60
ENV PIHOLE_COUNTER_CYCLE_INTERVAL 60
ENV DEBUG false

RUN apk update && \
    apk upgrade && \
    apk add --no-cache nano && \
    pip install mysql-connector-python

COPY . /app/

ENTRYPOINT ["python3", "/app/entrypoint.py"]