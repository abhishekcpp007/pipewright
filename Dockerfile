# Two stages. test runs the suite with no network at all, runtime ships the CLI.
FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PW_HOME=/app/data
COPY pyproject.toml README.md ./
COPY pw ./pw
RUN pip install --no-cache-dir -e .

FROM base AS test
RUN pip install --no-cache-dir "pytest>=8.3" "ruff>=0.7"
COPY tests ./tests
COPY bench ./bench
CMD ["pytest", "-q"]

FROM base AS runtime
# Build the warehouse at image build time so the container starts with something to read.
RUN python -m pw.cli gen && python -m pw.cli replay && python -m pw.cli test
ENTRYPOINT ["python", "-m", "pw.cli"]
CMD ["report"]
