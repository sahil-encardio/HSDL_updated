import threading
import struct
import paho.mqtt.client as mqtt
from datetime import datetime
from zoneinfo import ZoneInfo
from SBus import SBus, Message, Observer
import numpy as np
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions
import json
from collections import deque
import argparse
import sys

# MQTT Local Configuration
BROKER = "localhost"
PORT = 1883
TOPIC_CH1 = "data/nexus/ch1"
TOPIC_CH2 = "data/nexus/ch2"
TOPIC_CH3 = "data/nexus/ch3"
TOPIC_CH4 = "data/nexus/ch4"

# MQTT Remote Configuration
REMOTE_BROKER = "emqx.demo.proqio.com"
REMOTE_PORT = 1883
REMOTE_TOPIC_PUB = "data/nexus1234/test"
REMOTE_USERNAME = "nexus1234"
REMOTE_PASSWORD = "nexus1234"

# InfluxDB
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "t9EpZY1hquEwHxXi2RTulLetUjIlhiUh6aQ4DjktZnFQN4OIIfM2gsCF_OOehCgnKgPdM5mqAHrYBwnKopQU3g=="
INFLUX_ORG = "encardio"
INFLUX_BUCKET = "nexus"

# Constants
SAMPLES_PER_CHANNEL = 1200
NUM_CHANNELS = 4
TOTAL_SAMPLES = SAMPLES_PER_CHANNEL * NUM_CHANNELS

# Cliente Influx
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1200, flush_interval=1000))

# Clientes MQTT
mqtt_client_local = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client_remote = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client_remote.username_pw_set(REMOTE_USERNAME, REMOTE_PASSWORD)

# Variables globales para parámetros
GF_CH1, GF_CH2, GF_CH3, GF_CH4 = 1.0, 1.0, 1.0, 1.0
OF_CH1, OF_CH2, OF_CH3, OF_CH4 = 0.0, 0.0, 0.0, 0.0
FS_SEND_REMOTE = 10
FS_SEND_GRAFANA = 100
FS_INFLUX = 1000
FEATURE_TYPE = "mean"
REMOTE_ENABLED = True
TIMEZONE = "America/Guayaquil"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# =============================================================================
# SISTEMA SIN COLAS - SIEMPRE ÚLTIMO MENSAJE
# =============================================================================

# Buffers compartidos
latest_mqtt_data = None
latest_influx_data = None
latest_remote_data = None
latest_lock = threading.Lock()

# Buffer circular de datos crudos
data_buffer = deque(maxlen=TOTAL_SAMPLES * 3)
buffer_lock = threading.Lock()

# Variables de control
last_time = None
shutdown_event = threading.Event()

def parse_arguments():
    parser = argparse.ArgumentParser(description="Sistema Nexus Tiempo Real - Último mensaje")
    parser.add_argument("--gf", nargs=4, type=float, default=[1.0, 1.0, 1.0, 1.0])
    parser.add_argument("--of", nargs=4, type=float, default=[0.0, 0.0, 0.0, 0.0])
    parser.add_argument("--fs-grafana", type=int, default=100)
    parser.add_argument("--fs-influx", type=int, default=1000)
    parser.add_argument("--fs-remote", type=int, default=10)
    parser.add_argument("--feature", type=str, choices=["mean", "max", "min"], default="mean")
    parser.add_argument("--no-remote", action="store_true")
    parser.add_argument("--timezone", type=str, default="America/Guayaquil")
    parser.add_argument("--timestamp-format", type=str, default="%Y-%m-%d %H:%M:%S")
    parser.add_argument("--show-config", action="store_true")
    return parser.parse_args()

def show_configuration():
    print("=" * 60)
    print("CONFIGURACIÓN DEL SISTEMA - ÚLTIMO MENSAJE")
    print("=" * 60)
    print(f"Gain Factors: {GF_CH1}, {GF_CH2}, {GF_CH3}, {GF_CH4}")
    print(f"Offsets: {OF_CH1}, {OF_CH2}, {OF_CH3}, {OF_CH4}")
    print(f"MQTT (Grafana): {FS_SEND_GRAFANA} Hz")
    print(f"InfluxDB: {FS_INFLUX} Hz")
    print(f"Remoto cada: {FS_SEND_REMOTE} s")
    print(f"Feature: {FEATURE_TYPE.upper()} | Remoto: {'HABILITADO' if REMOTE_ENABLED else 'DESHABILITADO'}")
    print(f"Zona Horaria: {TIMEZONE}")
    print("=" * 60)

def connect_mqtt():
    mqtt_client_local.connect(BROKER, PORT)
    mqtt_client_local.loop_start()
    if REMOTE_ENABLED:
        mqtt_client_remote.connect(REMOTE_BROKER, REMOTE_PORT)
        mqtt_client_remote.loop_start()
        print("MQTT Local + Remoto conectados")
    else:
        print("MQTT Local conectado (remoto deshabilitado)")

def convert_to_voltage(raw_data):
    scale = 0.04 / 2147483648.0
    offset = -0.02
    ADC1 = raw_data[0:1200]
    ADC2 = raw_data[1200:2400]
    ADC3 = raw_data[2400:3600]
    ADC4 = raw_data[3600:4800]
    v1 = [(x * scale + offset) * GF_CH1 + OF_CH1 for x in ADC1]
    v2 = [(x * scale + offset) * GF_CH2 + OF_CH2 for x in ADC2]
    v3 = [(x * scale + offset) * GF_CH3 + OF_CH3 for x in ADC3]
    v4 = [(x * scale + offset) * GF_CH4 + OF_CH4 for x in ADC4]
    return v1, v2, v3, v4

def calculate_feature(data_array, feature_type):
    if feature_type == "mean":
        return float(round(np.mean(data_array), 6))
    elif feature_type == "max":
        return float(round(np.max(data_array), 6))
    elif feature_type == "min":
        return float(round(np.min(data_array), 6))
    else:
        return float(round(np.mean(data_array), 6))

# -------------------------------
# PROCESADORES SIN COLAS
# -------------------------------

def mqtt_realtime_processor():
    print("Iniciando procesador MQTT Tiempo Real...")
    global latest_mqtt_data
    while not shutdown_event.is_set():
        try:
            with latest_lock:
                if latest_mqtt_data is None:
                    continue
                raw_data = latest_mqtt_data.copy()
                latest_mqtt_data = None

            v1, v2, v3, v4 = convert_to_voltage(raw_data)
            mqtt_factor = max(1, len(v1) // FS_SEND_GRAFANA)

            for idx in range(0, len(v1), mqtt_factor):
                mqtt_client_local.publish(TOPIC_CH1, f"{v1[idx]:.6f}", qos=0)
                mqtt_client_local.publish(TOPIC_CH2, f"{v2[idx]:.6f}", qos=0)
                mqtt_client_local.publish(TOPIC_CH3, f"{v3[idx]:.6f}", qos=0)
                mqtt_client_local.publish(TOPIC_CH4, f"{v4[idx]:.6f}", qos=0)

            print(f"[MQTT Tiempo Real] Enviado bloque ({len(v1)//mqtt_factor} muestras)")
            time.sleep(0.01)
        except Exception as e:
            print(f"Error en MQTT tiempo real: {e}")
            time.sleep(0.05)

def influx_background_processor():
    print("Iniciando procesador InfluxDB background...")
    global latest_influx_data
    while not shutdown_event.is_set():
        try:
            with latest_lock:
                if latest_influx_data is None:
                    continue
                raw_data = latest_influx_data.copy()
                latest_influx_data = None

            v1, v2, v3, v4 = convert_to_voltage(raw_data)
            factor = max(1, len(v1) // FS_INFLUX)
            points = []
            start_ts = int(time.time() * 1e9)
            dt = int(1e9 / FS_INFLUX)
            for i, idx in enumerate(range(0, len(v1), factor)):
                p = (Point("adc_measurements")
                     .time(start_ts + i * dt, WritePrecision.NS)
                     .field("ADC1", v1[idx])
                     .field("ADC2", v2[idx])
                     .field("ADC3", v3[idx])
                     .field("ADC4", v4[idx]))
                points.append(p)
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            print(f"[InfluxDB] {len(points)} puntos guardados")
            time.sleep(0.05)
        except Exception as e:
            print(f"Error InfluxDB: {e}")
            time.sleep(0.1)

def remote_processor():
    if not REMOTE_ENABLED:
        print("Procesador remoto deshabilitado")
        return
    print("Iniciando procesador remoto...")
    global latest_remote_data
    while not shutdown_event.is_set():
        try:
            with latest_lock:
                if latest_remote_data is None:
                    continue
                raw_data = latest_remote_data.copy()
                latest_remote_data = None

            v1, v2, v3, v4 = convert_to_voltage(raw_data)
            f1 = calculate_feature(v1, FEATURE_TYPE)
            f2 = calculate_feature(v2, FEATURE_TYPE)
            f3 = calculate_feature(v3, FEATURE_TYPE)
            f4 = calculate_feature(v4, FEATURE_TYPE)
            tz = ZoneInfo(TIMEZONE)
            timestamp = datetime.now(tz).strftime(TIMESTAMP_FORMAT)
            payload = {
                "Header": {"NexusID": 543, "DevicesConnected": ["Analog1.1"], "Feature": FEATURE_TYPE},
                "data": [{
                    "ID": 0, "Device": "Analog1.1", "SPS": 1200, "TimeStamp": timestamp,
                    "CH1": {"Units": "uS", "Gain": GF_CH1, "Strain": [f1]},
                    "CH2": {"Units": "uS", "Gain": GF_CH2, "Strain": [f2]},
                    "CH3": {"Units": "uS", "Gain": GF_CH3, "Strain": [f3]},
                    "CH4": {"Units": "uS", "Gain": GF_CH4, "Strain": [f4]}
                }]
            }
            mqtt_client_remote.publish(REMOTE_TOPIC_PUB, json.dumps(payload), qos=1)
            print(f"[MQTT Remoto] CH1={f1:.6f} CH2={f2:.6f} CH3={f3:.6f} CH4={f4:.6f}")
            time.sleep(FS_SEND_REMOTE)
        except Exception as e:
            print(f"Error remoto: {e}")
            time.sleep(0.5)

# -------------------------------
# DISTRIBUIDOR (actualiza buffers)
# -------------------------------
def data_distributor():
    print("Iniciando distribuidor sin colas (último mensaje)...")
    while not shutdown_event.is_set():
        try:
            with buffer_lock:
                if len(data_buffer) < TOTAL_SAMPLES:
                    time.sleep(0.02)
                    continue
                current_data = list(data_buffer)[-TOTAL_SAMPLES:]

            with latest_lock:
                global latest_mqtt_data, latest_influx_data, latest_remote_data
                latest_mqtt_data = current_data.copy()
                latest_influx_data = current_data.copy()
                if REMOTE_ENABLED:
                    latest_remote_data = current_data.copy()

            time.sleep(0.02)
        except Exception as e:
            print(f"Error distribuidor: {e}")
            time.sleep(0.1)

# -------------------------------
# RECEPCIÓN SBus
# -------------------------------
def message_handler(message: Message):
    global last_time
    now = datetime.now()
    if last_time is not None:
        delta = (now - last_time).total_seconds() * 1000.0
        if delta > 50:
            print(f"⚠️ Retraso: {delta:.1f} ms")
    last_time = now
    num_uint32 = message.length // 4
    int_values = list(struct.unpack(f"{num_uint32}I", message.data))
    with buffer_lock:
        data_buffer.extend(int_values)

def receive_loop():
    print("Iniciando receptor SBus...")
    while not shutdown_event.is_set():
        try:
            subscriber_sbus.receive_message()
        except Exception as e:
            print(f"Error receptor SBus: {e}")
            time.sleep(0.1)

# -------------------------------
# MAIN
# -------------------------------
def main():
    global GF_CH1, GF_CH2, GF_CH3, GF_CH4, OF_CH1, OF_CH2, OF_CH3, OF_CH4
    global FS_SEND_REMOTE, FS_SEND_GRAFANA, FS_INFLUX, FEATURE_TYPE, REMOTE_ENABLED
    global TIMEZONE, TIMESTAMP_FORMAT, subscriber_sbus

    args = parse_arguments()
    GF_CH1, GF_CH2, GF_CH3, GF_CH4 = args.gf
    OF_CH1, OF_CH2, OF_CH3, OF_CH4 = args.of
    FS_SEND_GRAFANA = args.fs_grafana
    FS_INFLUX = args.fs_influx
    FS_SEND_REMOTE = args.fs_remote
    FEATURE_TYPE = args.feature
    REMOTE_ENABLED = not args.no_remote
    TIMEZONE = args.timezone
    TIMESTAMP_FORMAT = args.timestamp_format

    show_configuration()
    if args.show_config:
        sys.exit(0)

    subscriber_sbus = SBus("sam_channel_raw_0")
    observer = Observer(name="TestObserver", callback=message_handler)
    subscriber_sbus.init_subscriber([observer])
    connect_mqtt()

    threads = [
        threading.Thread(target=receive_loop, daemon=True),
        threading.Thread(target=data_distributor, daemon=True),
        threading.Thread(target=mqtt_realtime_processor, daemon=True),
        threading.Thread(target=influx_background_processor, daemon=True)
    ]
    if REMOTE_ENABLED:
        threads.append(threading.Thread(target=remote_processor, daemon=True))

    for t in threads:
        t.start()
        print(f"✓ Hilo {t.name} iniciado")

    print("\n🚀 Sistema activo (Último mensaje). Ctrl+C para detener...\n")
    try:
        while True:
            time.sleep(5)
            with latest_lock:
                status = f"Buffer: {len(data_buffer)} | MQTT={'OK' if latest_mqtt_data else '-'} | Influx={'OK' if latest_influx_data else '-'}"
                if REMOTE_ENABLED:
                    status += f" | Remoto={'OK' if latest_remote_data else '-'}"
                print(f"📊 {status}")
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo sistema...")
        shutdown_event.set()
        subscriber_sbus.destroy()
        mqtt_client_local.loop_stop(); mqtt_client_local.disconnect()
        if REMOTE_ENABLED:
            mqtt_client_remote.loop_stop(); mqtt_client_remote.disconnect()
        influx_client.close()
        print("✓ Sistema detenido correctamente")

if __name__ == "__main__":
    main()
