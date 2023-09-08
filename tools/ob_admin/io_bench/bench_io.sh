#!/bin/bash

# check parameters
[ $# != 3 ] && echo "wrong parameters" && exit
bench_dir=$1
file_size=$2
output_dir=$3
echo "bench_dir=$bench_dir, file_size=$file_size, output_dir=$output_dir"

# prepare bench file
bench_file_name=$bench_dir/bench_chunk
prepare_cmd="fio -filename=$bench_file_name -filesize=$file_size -rw=randread -bs=1M -runtime=1 -name=prepare --fsync=1 2>&1 > /dev/null"
echo "prepare bench file"
echo "  prepare_cmd: $prepare_cmd"
echo ""
bash -c "$prepare_cmd"

# prepare result file
result_file=$output_dir/io_resource.conf
rm -rf $result_file
header_line=$(printf "%-10s %-15s %-15s %-10s" "io_type" "io_size_byte" "io_ps" "io_rt_us")
echo "$header_line" >> $result_file

# do benchmark
fio_output=$output_dir/bench.log
fio_mode_array=("randread" "randwrite")
bs_array=(   4096  8192  16384  32768  65536  131072  262144  524288  1048576 4096  8192  16384  32768  65536  131072  262144  524288  1048576 2097152)
mode_array=( 0     0     0      0      0      0       0       0       0       1     1     1      1      1      1       1       1       1       1 )
parse_iops_pos=( 8    49 )
parse_rt_pos=(   40   81 )
for (( i = 0; i < ${#bs_array[@]}; ++i ))
do
  bench_name=ob_io_bench_$i
  bench_mode=${mode_array[$i]}
  fio_mode=${fio_mode_array[$bench_mode]}
  block_size=${bs_array[$i]}
  iops_pos=${parse_iops_pos[$bench_mode]}
  rt_pos=${parse_rt_pos[$bench_mode]}
  bench_cmd="fio -filename=$bench_file_name -size=$file_size -numjobs=32 -thread -group_reporting -ioengine=libaio -direct=1 -iodepth=1 -rw=$fio_mode -bs=$block_size -runtime=10 -name=$bench_name --output-format=terse --terse-version=3 --output=$fio_output"
  parse_cmd_iops="tail -1 $fio_output | cut -d ';' -f $iops_pos"
  parse_cmd_rt="tail -1 $fio_output | cut -d ';' -f $rt_pos"
  echo "exec io bench $i: block_size=$block_size, bench_mode=$bench_mode"
  echo "  bench_cmd: $bench_cmd"
  echo "  parse_iops_cmd: $parse_cmd_iops,    parse_rt_cmd: $parse_cmd_rt"
  echo ""
  bash -c "$bench_cmd"
  iops=$(bash -c "$parse_cmd_iops")
  rt=$(bash -c "$parse_cmd_rt")
  echo -e "\n\e[1;32mmode=$fio_mode, size=$block_size, iops=$iops, rt=$rt\e[0m\n\n"
  result_line=$(printf "%-10d %-15ld %-15.2lf %-10.2lf" $bench_mode $block_size $iops $rt)
  echo "$result_line" >> $result_file
done

# clean up
rm -rf $bench_file_name
rm -rf $fio_output
