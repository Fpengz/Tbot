FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY configs ./configs
COPY src ./src
RUN pip install --no-cache-dir .[data,live-data]
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "tbot", "runtime"]
