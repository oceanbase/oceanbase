
LOG_FILE=log/liboblog.log

watch -n 1 "if [ -f $LOG_FILE ]; then grep TRANS_TASK_POOL $LOG_FILE | grep STAT | awk '{print \$1, \$2, \$9, \$10, \$11, \$12, \$13, \$14, \$15}' | tail -n 5; fi"
