#!/bin/bash

LOG_FILE=log/libobcdc.log

# TPS_STAT lines: $11=NEXT_RECORD_TPS $12=RELEASE_RECORD_TPS $13=NEXT_RECORD_RPS $14=RELEASE_RECORD_RPS
watch -n 1 "if [ -f $LOG_FILE ]; then grep NEXT_RECORD_TPS $LOG_FILE | awk '{print \$1, \$2, \$11, \$12, \$13, \$14}' | tail -n 3; fi"
