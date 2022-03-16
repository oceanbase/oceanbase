
LOG_FILE=log/liboblog.log

watch -n 1 "if [ -f $LOG_FILE ]; then grep 'update progress upper limit' $LOG_FILE | awk -F ' INFO |lower_limit=|upper_limit=|fetcher_delay=' '{printf \"%s lower:%s upper:%s delay:%s\n\", \$1, \$3, \$4, \$5}' | tail -n 5; fi"
