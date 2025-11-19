#!/bin/bash

# Ejecutar binario en background y silenciar logs
/app/main &
python /app/SBus_python/mqtt_pub.py --fs-grafana 10 --fs-influx 1200000000
