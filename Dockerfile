FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN addgroup --system gdpirate && adduser --system --ingroup gdpirate gdpirate

COPY pyproject.toml README.md alembic.ini ./
COPY alembic ./alembic
COPY config ./config
COPY src ./src

RUN pip install --no-cache-dir .

USER gdpirate

CMD ["gdpirate", "serve", "--host", "0.0.0.0", "--port", "8000"]
