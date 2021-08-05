#!/bin/bash

CURRENT_DIR="$(cd $(dirname $0); pwd)"
test_file=${CURRENT_DIR}/ob_error_test.test
test_result_file=${CURRENT_DIR}/expect_result.result
result_file=${CURRENT_DIR}/test.result

rm -f $result_file

while read line
do
 echo "$"$line >> $result_file
 eval $line >> $result_file
done < $test_file

if cmp -s $result_file $test_result_file
then
    echo "test finish. success."
else
    echo "test finish. failed."
fi
