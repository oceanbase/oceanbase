#!/bin/bash
#rm -f libobtable.log ; ./table_example '100.88.11.96' 50803 sys root '' test t2
#rm -f libobtable.log ; ./kvtable_example '100.88.11.96' 50803 sys root '' test t3
#rm -f libobtable.log ; ./pstore_example '100.88.11.96' 50803 sys root '' test t5

HOST=100.88.11.91
PORT=60809
RPCPORT=60808
THREAD=500
THREAD=200
ROWS=1000000
IO_THREAD=100
user='root@sys'
db=test
TABLENAME=hkvtable

VAL_LEN=1024
BATCH_SIZE=1

BATCH_SIZE=10
VAL_LEN=100

DURATION=1800



rm -f libobtable.log
mysql -h $HOST -P $PORT -u $user -e "drop table if exists $TABLENAME" $db
mysql -h $HOST -P $PORT -u $user -e "create table if not exists $TABLENAME (K varbinary(1024), Q varchar(256), T bigint, V varbinary(1024), primary key(K, Q, T))" $db
sleep 3

# /opt/rh/devtoolset-2/root/usr/bin/valgrind --leak-check=full 
./kvtable_bench $HOST $PORT sys root '' $db $TABLENAME $RPCPORT prepare 2 $THREAD $ROWS $IO_THREAD $VAL_LEN $BATCH_SIZE $DURATION
#./kvtable_bench $HOST $PORT sys root '' $db $TABLENAME $RPCPORT run_get 2 $THREAD $ROWS $IO_THREAD $VAL_LEN $BATCH_SIZE $DURATION
./kvtable_bench_s $HOST $PORT sys root '' $db $TABLENAME $RPCPORT run_put 2 $THREAD $ROWS $IO_THREAD $VAL_LEN $BATCH_SIZE $DURATION
