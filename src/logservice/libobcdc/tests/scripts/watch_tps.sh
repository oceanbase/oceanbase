
LOG_FILE=log/libobcdc.log

watch -n 1 "if [ -f $LOG_FILE ]; then grep NEXT_RECORD_TPS $LOG_FILE | awk '{print \$1, \$2, \$10, \$11, \$12, \$13}' | tail -n 3; fi"
