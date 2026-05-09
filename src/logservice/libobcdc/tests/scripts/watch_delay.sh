#!/bin/bash

LOG_FILE=log/libobcdc.log

# HEARTBEAT lines: $12$13=DELAY $14=LS_COUNT $19$20=DATA_PROGRESS $21$22=DDL_PROGRESS
watch -n 1 "if [ -f $LOG_FILE ]; then grep MIN_DELAY $LOG_FILE | grep HEARTBEAT | awk '{print \$1, \$2, \$12, \$13, \$14, \$19, \$20}' | tail -n 3; fi"
