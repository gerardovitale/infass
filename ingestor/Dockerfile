FROM python:3.13-slim AS python-build
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


FROM python:3.13-slim AS final
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium-driver chromium \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/usr/lib/chromium/:$PATH"
VOLUME /dev/shm
COPY --from=python-build /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=python-build /usr/local/bin /usr/local/bin
COPY ./app/ ./
ENTRYPOINT ["python3", "main.py"]
