#!/bin/bash

python ./src/detect_anomalies.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json
