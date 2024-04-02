
LOG_FILE=log/libobcdc.log

watch -n 1 "if [ -f $LOG_FILE ]; then grep MIN_DELAY $LOG_FILE | grep HEARTBEAT | awk '{print \$11, \$12, \$13}' | tail -n 3; fi"
