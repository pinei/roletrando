FROM prefecthq/prefect:3-latest

RUN apt-get update && apt-get install -y postgresql && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
RUN pip install . && pip install asyncpg

COPY start-prefect.sh /start-prefect.sh
RUN chmod +x /start-prefect.sh

ENV PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@localhost/prefect"

ENTRYPOINT ["/start-prefect.sh"]
