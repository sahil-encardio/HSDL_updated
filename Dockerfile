FROM python:3.11-slim

# Instalar dependencias necesarias para ZMQ
RUN apt-get update && apt-get install -y \
    libzmq3-dev \
    gpiod \
    && pip install pyzmq numpy paho-mqtt fastapi pydantic typing influxdb-client \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar binario y script Python (si aplica)
COPY SBus_python/ /app/SBus_python/
COPY main /app/main
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/main /app/entrypoint.sh

#ENTRYPOINT [ "bash" ]

ENTRYPOINT [ "/app/entrypoint.sh" ]