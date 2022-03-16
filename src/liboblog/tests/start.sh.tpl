#!/bin/bash

#TIMESTAMP=`date -d "2019-08-09 12:01:31" +%s`
TIMESTAMP=1611816878745567
TIMESTAMP=0
TIMESTAMP=${TIMESTAMP: 0 :10}

CONFIG=conf/liboblog.conf
DATA_FILE=data/data.log

RUN_TIME=60

LOG_DIR=./log

./kill_oblog.sh

rm -fr $LOG_DIR/ core.* ${DATA_FILE}*

export LD_LIBRARY_PATH=./lib/:$LD_LIBRARY_PATH
./oblog_tailf -v

## work in background
#./oblog_tailf -f $CONFIG -t $TIMESTAMP -d
./oblog_tailf -f $CONFIG -t $TIMESTAMP -d

# Timed runs in the background
#./oblog_tailf -R $RUN_TIME -f $CONFIG -t $TIMESTAMP -d

# output data
#./oblog_tailf -V -f $CONFIG -T $TIMESTAMP -D${DATA_FILE} 2>&1 | grep -v tid

# Timed runs with output data
#./oblog_tailf -x -o -R$RUN_TIME -f $CONFIG -t $TIMESTAMP -D${DATA_FILE} 2>&1 | grep -v tid
