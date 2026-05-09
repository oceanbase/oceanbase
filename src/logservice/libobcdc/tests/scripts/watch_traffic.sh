#!/bin/bash

LOG=log/libobcdc.log

# FETCH_STREAM lines: $11$12=stream(split by space in ls_id) $13=traffic $14=log_size $15=size/rpc $17=rpc_cnt $19=rpc_time $20=svr_time
watch -n 1 "if [ -f $LOG ]; then grep traffic $LOG | grep -v 'traffic=0\.00B' | awk '{print \$1, \$2, \$13, \$14, \$15, \$17, \$19, \$11, \$12}' | tail -n 5; fi"
