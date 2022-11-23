#!/bin/sh

make -j 16

while true;do
  rm -f *.log *.log.*
for i in `ls test*|grep -v '\.'`;do
#for i in test_ob_transaction;do
    echo $i
    ./$i
    if [ $? -ne 0 ];then
      exit $?
    fi
  done
done

#vim transaction.log
