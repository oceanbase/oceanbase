LOG_FILE=log/liboblog.log

watch -n 1 "if [ -f $LOG_FILE ]; then grep NEED log/liboblog.log | awk '{print \$10, \$11, \$12, \$13, \$15}' | tail -n 5; fi"
