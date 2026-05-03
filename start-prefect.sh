#!/bin/bash

PGDATA=/var/lib/postgresql/data
PGLOG=/tmp/postgresql.log
export PATH="/usr/lib/postgresql/$(ls /usr/lib/postgresql)/bin:$PATH"

postgres_is_up() {
    pg_isready -h localhost -U postgres 2>/dev/null
}

server_is_up() {
    python -c "import urllib.request; urllib.request.urlopen('http://localhost:4200/api/health')" 2>/dev/null
}

start_postgres() {
    if [ ! -f "$PGDATA/PG_VERSION" ]; then
        chown -R postgres:postgres "$PGDATA"
        runuser -u postgres -- initdb -D "$PGDATA"
    fi

    runuser -u postgres -- pg_ctl start -D "$PGDATA" -l "$PGLOG"
    until postgres_is_up; do sleep 1; done

    runuser -u postgres -- psql -c "CREATE ROLE prefect WITH LOGIN PASSWORD 'prefect'" 2>/dev/null || true
    runuser -u postgres -- createdb -O prefect prefect 2>/dev/null || true
}

start_postgres

until postgres_is_up; do
    echo "Aguardando PostgreSQL..."
    sleep 2
done

prefect server start --host 0.0.0.0 &

until server_is_up; do
    echo "Aguardando Prefect Server..."
    sleep 2
done

prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

prefect work-pool create --type process my-pool 2>/dev/null || true
prefect deploy --all

exec prefect worker start --pool "my-pool" --type "process"
