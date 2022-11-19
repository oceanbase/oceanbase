#!/bin/bash
taskset 0xF ./test_dag_scheduler --gtest_filter=*stress_test -p 10000 -s 360000000 -l INFO &

while true
do
  grep "ERROR" test_dag_scheduler.log* -q
  if [ $? -eq 0 ]
  then
    echo "FATAL ERROR occured in test_dag_scheduler!!!" > test_result
    break
  fi
  ls test_dag_scheduler.log.* 2>/dev/null | head -n -1 > file_to_delete
  if [ -s file_to_delete ]
  then
    xargs rm < file_to_delete
  fi
  ps x | grep 'test_dag_scheduler' | grep -v grep > /dev/null 2>&1
  if [ $? -ne 0 ]
  then
    echo "test_dag_scheduler finished" > test_result
    break
  fi
  sleep 1
done
if [ -e file_to_delete ]
then
  rm -f file_to_delete
fi
