#!/bin/bash

INPUT_FILE="masscan_results.txt"

while read -r line; do
    # Extract IP and Port using grep and awk
    IP=$(echo "$line" | grep -oP '(?<=Host: )[\d\.]+')
    PORT=$(echo "$line" | grep -oP '(?<=Ports: )\d+')

    # If IP and PORT exist, run hping3
    if [[ -n "$IP" && -n "$PORT" ]]; then
        echo "Checking port $PORT on $IP..."
        sudo hping3 -S -p $PORT $IP -c 1 2>/dev/null | grep "flags=SA" && echo "Port $PORT on $IP is OPEN" || echo "Port $PORT on $IP is CLOSED or FILTERED"
    fi
done < "$INPUT_FILE"
