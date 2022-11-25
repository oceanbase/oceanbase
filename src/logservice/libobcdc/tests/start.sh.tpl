#!/bin/bash

#TIMESTAMP=`date -d "2022-09-09 12:01:31" +%s`
TIMESTAMP=1611816878745567
TIMESTAMP=0
TIMESTAMP=${TIMESTAMP: 0 :10}
TIMESTAMP=0
TIMESTAMP=1663075700401542

CONFIG=conf/libobcdc.conf
DATA_FILE=data/data.log

RUN_TIME=60

LOG_DIR=./log

./kill_oblog.sh

rm -fr $LOG_DIR/ core.* ${DATA_FILE}*

export LD_LIBRARY_PATH=./lib/:$LD_LIBRARY_PATH
./obcdc_tailf -v

## work in background
#./obcdc_tailf -f $CONFIG -t $TIMESTAMP -d
./obcdc_tailf -f $CONFIG -T $TIMESTAMP -d
# ./obcdc_tailf -f $CONFIG -T $TIMESTAMP -D${DATA_FILE} -d

# Timed runs in the background
#./obcdc_tailf -R $RUN_TIME -f $CONFIG -t $TIMESTAMP -d

# output data
#./obcdc_tailf -V -f $CONFIG -T $TIMESTAMP -D${DATA_FILE} 2>&1 | grep -v tid

# Timed runs with output data
#./obcdc_tailf -x -o -R$RUN_TIME -f $CONFIG -t $TIMESTAMP -D${DATA_FILE} 2>&1 | grep -v tid
