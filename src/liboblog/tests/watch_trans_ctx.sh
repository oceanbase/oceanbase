
LOG_FILE=log/liboblog.log

watch -n 1 "if [ -f $LOG_FILE ]; then grep PREPARED $LOG_FILE | awk '{print \$9, \$10, \$11, \$12, \$13, \$14, \$15}' | tail -n 5; fi"
