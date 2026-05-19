FROM apache/superset:latest

# Switch to root to ensure we have permissions
USER root

# Superset uses 'uv' instead of 'pip' to manage its virtual environment in modern builds
# We must use 'uv pip install' so it correctly targets /app/.venv
RUN uv pip install psycopg2-binary

# Switch back to the superset user
USER superset
