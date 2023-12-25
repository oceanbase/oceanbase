LOG=log/libobcdc.log

watch -n 1 "if [ -f $LOG ]; then grep traffic $LOG  | grep -v traffic=0.00B | awk '{printf(\"%-26s %-13s %-10s %-10s %-14s %-12s %-18s %-28s %-25s %-26s\n\", \$10, \$11, \$12, \$13, \$14, \$15, \$17, \$18, \$19, \$20);}' | tail -n 3; fi"
