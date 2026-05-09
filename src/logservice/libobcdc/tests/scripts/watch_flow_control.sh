#!/bin/bash

LOG_FILE=log/libobcdc.log
# [FLOW_CONTROL]: $11=NEED_SLOW_DOWN $12=PAUSED $13=MEM $14=AVAIL_MEM $15=READY_TO_SEQ $31=NEED_PAUSE_DISPATCH
watch -n 1 "if [ -f $LOG_FILE ]; then grep '\[FLOW_CONTROL\]' $LOG_FILE | awk '{print \$1, \$2, \$11, \$12, \$13, \$14, \$15, \$31}' | tail -n 3; fi"
