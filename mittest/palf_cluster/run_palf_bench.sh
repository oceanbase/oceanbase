#!/bin/sh
TEST_MACHINE1='SERVER_IP1'
TEST_MACHINE2='SERVER_IP2'
TEST_MACHINE3='SERVER_IP3'
USER_NAME='USERNAME'
# use your own data dir path
TARGET_PATH='/yourworkdir'

function kill_server_process
{
ssh $USERNAME@$TEST_MACHINE1 bash -s << EOF
    killall -9 test_palf_bench_server;
EOF
ssh $USERNAME@$TEST_MACHINE2 bash -s << EOF
    killall -9 test_palf_bench_server;
EOF
ssh $USERNAME@$TEST_MACHINE3 bash -s << EOF
    killall -9 test_palf_bench_server;
EOF
  echo "kill_server_process success"
}

function cleanenv
{
ssh $USERNAME@$TEST_MACHINE1 bash -s << EOF
    killall -9 test_palf_bench_server;
    cd $TARGET_PATH;
    rm -rf palf_cluster_bench_server/;
    rm -rf test_palf_bench_server;
    rm -rf *so;
EOF
ssh $USERNAME@$TEST_MACHINE2 bash -s << EOF
    killall -9 test_palf_bench_server;
    cd $TARGET_PATH;
    rm -rf palf_cluster_bench_server/;
    rm -rf test_palf_bench_server;
    rm -rf *so;
EOF
ssh $USERNAME@$TEST_MACHINE3 bash -s << EOF
    killall -9 test_palf_bench_server;
    cd $TARGET_PATH;
    rm -rf palf_cluster_bench_server/;
    rm -rf test_palf_bench_server;
    rm -rf *so;
EOF
  echo "cleanenv success"
}

function send_server_binary
{
  cleanenv
  find ../../ -name *.so | xargs -I {} scp {} $USERNAME@$TEST_MACHINE1:$TARGET_PATH
  scp test_palf_bench_server $USERNAME@$TEST_MACHINE1:$TARGET_PATH
ssh $USERNAME@$TEST_MACHINE1 bash -s << EOF
    cd $TARGET_PATH;
    scp *.so $USERNAME@$TEST_MACHINE2:$TARGET_PATH
    scp test_palf_bench_server $USERNAME@$TEST_MACHINE2:$TARGET_PATH
    scp *.so $USERNAME@$TEST_MACHINE3:$TARGET_PATH
    scp test_palf_bench_server $USERNAME@$TEST_MACHINE3:$TARGET_PATH
EOF
  echo "send_server_binary success"
}

# thread_num
# log_size
function startserver
{
ssh $USERNAME@$TEST_MACHINE1 bash -s << EOF
    export LD_LIBRARY_PATH=$TARGET_PATH;
    cd $TARGET_PATH;
    ./test_palf_bench_server $1 $2 > /dev/null 2>&1 &
EOF
ssh $USERNAME@$TEST_MACHINE2 bash -s << EOF
    export LD_LIBRARY_PATH=$TARGET_PATH;
    cd $TARGET_PATH;
    ./test_palf_bench_server $1 $2 > /dev/null 2>&1 &
EOF
ssh $USERNAME@$TEST_MACHINE3 bash -s << EOF
    export LD_LIBRARY_PATH=$TARGET_PATH;
    cd $TARGET_PATH;
    ./test_palf_bench_server $1 $2 > /dev/null 2>&1 &
EOF
  sleep 5
  echo "startserver success"
}

# thread_num
# log_size
# server no
function start_one_server
{
  SERVER=""
  if [[ $3 == '1' ]];
  then
    SERVER=TEST_MACHINE1
  elif [[ $3 == '2' ]];
  then
    SERVER=TEST_MACHINE2
  elif [[ $3 == '3' ]];
  then
    SERVER=TEST_MACHINE3
  else
    echo "invalid arguments"
  fi;
ssh $USERNAME@$SERVER bash -s << EOF
    export LD_LIBRARY_PATH=$TARGET_PATH;
    cd $TARGET_PATH;
    ./test_palf_bench_server $1 $2 > /dev/null 2>&1
EOF
}

function start_local_client()
{
  # $1 thread_number
  # $2 nbytes
  # $3 palf_group_number
  # $4 replica_num
  echo "start local client" $@
  ./test_palf_bench_client $1 $2 $3 $4
}


# $1 thread_number
# $2 nbytes
# $3 replica_num
# $4 freeze_us
# $5 experiment name
function generate_result
{
result_dir_name="palf_raw_result_"$5
append_result_name=$result_dir_name"/palf_append_"$1"_"$2"_"$3"_"$4".result"
io_result_name=$result_dir_name"/palf_io_"$1"_"$2"_"$3"_"$4".result"
group_result_name=$result_dir_name"/palf_group_"$1"_"$2"_"$3"_"$4".result"

ssh $USERNAME@$TEST_MACHINE1 bash -s << EOF
    cd $TARGET_PATH;
    mkdir -p $result_dir_name;
    grep l_append palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'append_cnt=[0-9],'  > $append_result_name;
    grep inner_write_impl_ palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'l_io_cnt=[0-9],'  > $io_result_name;
    grep 'GROUP LOG INFO' palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'total_group_log_cnt=[0-9],'  > $group_result_name;
EOF
ssh $USERNAME@$TEST_MACHINE2 bash -s << EOF
    cd $TARGET_PATH;
    mkdir -p $result_dir_name;
    grep l_append palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'append_cnt=[0-9],'  > $append_result_name;
    grep inner_write_impl_ palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'l_io_cnt=[0-9],'  > $io_result_name;
    grep 'GROUP LOG INFO' palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'total_group_log_cnt=[0-9],'  > $group_result_name;
EOF
ssh $USERNAME@$TEST_MACHINE3 bash -s << EOF
    cd $TARGET_PATH;
    mkdir -p $result_dir_name;
    grep l_append palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'append_cnt=[0-9],'  > $append_result_name;
    grep inner_write_impl_ palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'l_io_cnt=[0-9],'  > $io_result_name;
    grep 'GROUP LOG INFO' palf_cluster_bench_server/palf_cluster_bench_server.log | grep -v 'total_group_log_cnt=[0-9],'  > $group_result_name;
EOF
}



# $1 thread_number
# $2 nbytes
# $3 palf_group_number
# $4 replica_num
# $5 freeze_us
# $6 exp1,exp2
function run_experiment_once
{
  send_server_binary
  echo "start experiment: "$6", thread_num: " $1 "log_size: " $2 " freeze_us: " $5
  kill_server_process
  startserver $1 $2
  ./test_palf_bench_client $1 $2 $3 $4
  sleep 20
  kill_server_process
  generate_result $1 $2 $4 $5 $6
}

# $1 thread_number
# $2 nbytes
# $3 palf_group_number
# $4 replica_num
# $5 freeze_us
# $6 exp1,exp2
# $7 leader/follower
function run_experiment_once_with_failure
{
  # send_server_binary
  echo "start experiment: "$6", thread_num: " $1 "log_size: " $2 " freeze_us: " $5
  kill_server_process
  startserver $1 $2
  ./test_palf_bench_client $1 $2 $3 $4
  sleep 20
  FAIL_MACHINE=""
  if [[ $7 == 'leader' ]];
  then
    FAIL_MACHINE=$TEST_MACHINE1
  else
    FAIL_MACHINE=$TEST_MACHINE2
  fi;
ssh $USERNAME@$FAIL_MACHINE bash -s << EOF
    killall -9 test_palf_bench_server;
EOF
  sleep 30
  kill_server_process
  generate_result $1 $2 $4 $5 $6
}


# $1 thread_number
# $2 nbytes
# $3 palf_group_number
# $4 replica_num
# $5 freeze_us
# $6 exp1,exp2
# $7 leader/follower
function run_experiment_once_measure_reconfirm
{
  send_server_binary
  echo "start experiment: "$6", thread_num: " $1 "log_size: " $2 " freeze_us: " $5
  kill_server_process
  startserver $1 $2
  ./test_palf_bench_client $1 $2 $3 $4
  loop_count=100
  round_idx=0
  fail_server_idx=1
  while [ $round_idx -lt loop_count ];
  do
    sleep 10
    let round_idx++
  done;

  FAIL_MACHINE=""
  if [[ $7 == 'leader' ]];
  then
    FAIL_MACHINE=$TEST_MACHINE1
  else
    FAIL_MACHINE=$TEST_MACHINE2
  fi;
ssh $USERNAME@$FAIL_MACHINE bash -s << EOF
    killall -9 test_palf_bench_server;
EOF
  sleep 30
  kill_server_process
  generate_result $1 $2 $4 $5 $6
}

function experiment1
{
  log_size_array=(32 64 128 256 512 1024 2048 4096 8192)

  thread_num_n_max=17

  run_round=1
  thread_num_n=1
  while [ $thread_num_n -lt $thread_num_n_max ]
  do
    thread_num=1
    let thread_num="500*thread_num_n"
    let thread_num_n++
    for log_size in ${log_size_array[@]}
    do
      echo "start run experiment1, round: " $run_round ", thread_num: " $thread_num "log_size: " $log_size
      run_experiment_once $thread_num $log_size 1 3 1000 exp1_wo_reduce
      let run_round++
    done;
  done;
}

# test for figure, less clients
function experiment1_less_clients
{
  send_server_binary
  log_size_array=(512)
  thread_numbers=(1 2 5 10 20 50 100 200 500)

  run_round=1
  for thread_num in ${thread_numbers[@]}
  do
    for log_size in ${log_size_array[@]}
    do
      echo "start run experiment1_less_clients, round: " $run_round ", thread_num: " $thread_num "log_size: " $log_size
      run_experiment_once $thread_num $log_size 1 3 1000 exp1_less_clients
      let run_round++
    done;
  done;

}

# test freeze logs right now
function experiment3
{
  log_size_array=(512)

  thread_num_n_max=17

  run_round=1
  thread_num_n=3
  while [ $thread_num_n -lt $thread_num_n_max ]
  do
    thread_num=1
    let thread_num="500*thread_num_n"
    let thread_num_n++
    for log_size in ${log_size_array[@]}
    do
      echo "start run experiment3, round: " $run_round ", thread_num: " $thread_num "log_size: " $log_size
      run_experiment_once $thread_num $log_size 1 3 1000 exp3
      let run_round++
    done;
    break
  done;
}

# freeze immediately or freeze adaptively
function experiment4
{
  send_server_binary
  run_round=1

  log_size_array=(0)
  # log_size_array=(512)
  # thread_numbers=(1 2 5 10 20 50 100 200 500 1000 2000)
  thread_numbers=(500)

  for thread_num in ${thread_numbers[@]}
  do
    for log_size in ${log_size_array[@]}
    do
      echo "start run experiment4, round: " $run_round ", thread_num: " $thread_num "log_size: " $log_size
      # run_experiment_once $thread_num $log_size 1 3 1000 exp4-noaggre
      # run_experiment_once $thread_num $log_size 1 3 1000 exp4-period
      run_experiment_once $thread_num $log_size 1 3 1000 exp4-adaptively
      # run_experiment_once $thread_num $log_size 1 3 1000 exp4-feedback
      let run_round++
    done;
  done;
}

function experiment5
{
  send_server_binary
  run_round=1

  thread_num=5000
  log_size=512

  echo "start run experiment5, round: " $run_round ", thread_num: " $thread_num "log_size: " $log_size
  run_experiment_once_with_failure $thread_num $log_size 1 3 1000 exp5_leader_failure leader

  # echo "start run experiment5, round: " $run_round ", thread_num: " $thread_num "log_size: " $log_size
  # run_experiment_once_with_failure $thread_num $log_size 1 3 1000 exp5_follower_failure follower
}

function main
{
  # run_experiment_once 100 16 1 3 1000 exp_test_bw
  # experiment1
  # send_server_binary
  # experiment4
  # experiment1_less_clients
  # experiment5
  run_experiment_once 1500 512 1 3 1000 exp_test
}

main "$@"