#!/bin/bash

server_list='127.0.0.1:8045,127.0.0.1:8046,127.0.0.1:8047'
leader=('127.0.0.1:8045' '127.0.0.1:8046' '127.0.0.1:8047')

leader_num=3
partition_num=4
log_num=10001

for ((i=0; i<$partition_num; i++)); do
  ((leader_idx=$i%$leader_num))
  ./post_msg_main add_partition $i $server_list ${leader[$leader_idx]}
done

for ((j=0; j<$log_num; j++)); do
  for ((i=0; i<$partition_num; i++)); do
    ((leader_idx=$i%$leader_num))
    ./post_msg_main submit_log $i 2 ${leader[$leader_idx]}
  done
done
