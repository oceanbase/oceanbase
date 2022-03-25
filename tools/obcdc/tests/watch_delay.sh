
LOG_FILE=log/liboblog.log

watch -n 1 "if [ -f $LOG_FILE ]; then grep MIN_DELAY $LOG_FILE | grep HEARTBEAT | awk '{print \$11, \$12}' | tail -n 3; fi"
