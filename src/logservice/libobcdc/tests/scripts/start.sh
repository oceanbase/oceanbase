#!/bin/bash

TIMESTAMP=`date -d "2023-6-16 19:30:31" +%s`
TIMESTAMP=0

CONFIG=conf/libobcdc.conf
DATA_FILE=data/data.log

RUN_TIME=60

LOG_DIR=./log

./kill_obcdc.sh

rm -fr $LOG_DIR/ core.* ${DATA_FILE}*

export LD_LIBRARY_PATH=./lib64/:$LD_LIBRARY_PATH
./bin/obcdc_tailf -v
ulimit -c unlimited
## work in background
`pwd`/bin/obcdc_tailf -f $CONFIG -t $TIMESTAMP -d
#`pwd`/bin/obcdc_tailf -f $CONFIG -T $TIMESTAMP -d
#./bin/obcdc_tailf_static -f $CONFIG -T $TIMESTAMP -d
#`pwd`/bin/obcdc_tailf -f $CONFIG -T $TIMESTAMP -D${DATA_FILE} -d

# Timed runs in the background
# `pwd`/bin/obcdc_tailf -R $RUN_TIME -f $CONFIG -t $TIMESTAMP -d

# output data
# `pwd`/bin/obcdc_tailf -V -f $CONFIG -T $TIMESTAMP -D${DATA_FILE} 2>&1 | grep -v tid

# Timed runs with output data
# `pwd`/bin/obcdc_tailf -x -o -R$RUN_TIME -f $CONFIG -t $TIMESTAMP -D${DATA_FILE} 2>&1 | grep -v tid
