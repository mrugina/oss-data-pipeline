FROM apache/superset

USER root

# Install the PostgreSQL driver into Superset's virtual environment.
# The image uses uv to manage /app/.venv, so we install with uv pip.
RUN uv pip install --python /app/.venv/bin/python3 psycopg2-binary

USER superset

