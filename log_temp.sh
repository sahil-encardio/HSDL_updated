#!/bin/bash
LOGFILE="temperatura.log"

while true; do
    RAW=$(cat /sys/class/thermal/thermal_zone0/temp)
    DATE=$(date '+%Y-%m-%d %H:%M:%S')
    TEMP=$(awk "BEGIN {printf \"%.1f\", $RAW/1000}")
    echo "$DATE, ${TEMP}  C"
    echo "$DATE, ${TEMP}°C" >> "$LOGFILE"
    sleep 1
done
