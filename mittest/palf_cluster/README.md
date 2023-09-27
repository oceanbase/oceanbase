# A Test Framework for PALF Cluster
The framework is designed and implentmented for evaluating PALF performance in a real cluster.

## Metrics
- Throughput
- Latency
- IOPS
...

## Usage

1. Use your own IP and wrok directory

**IP**
- `mittest/palf_cluster/test_palf_bench_server.cpp:45`
- `mittest/palf_cluster/test_palf_bench_client.cpp:45`
- `mittest/palf_cluster/run_palf_bench.sh:2`

**PORT**
default 53212
- `mittest/palf_cluster/env/ob_simple_log_cluster.h:92`

**workdir**
- `mittest/palf_cluster/run_palf_bench.sh:`

2. Apply some statistic logs and RPC definitions

```
cd oceanbase/
mv mittest/palf_cluster/palf-cluster.diff ./
git apply palf-cluster.diff
```

3. Build

    1. uncomment `mittest/CMakeLists.txt:5`
    2. remove `OB_BUILD_CLOSE_MODULES` in `CMakeLists.txt:162` and  `CMakeLists.txt:167` if you want to build in opensource mode
    3. build
    ```
    cd oceanbse/
    bash build.sh release --init --make
    ```

4. Run test
```
cd oceanbase/
cp mittest/palf_cluster/run_palf_bench.sh build_release/mittest/palf_cluster/
cd build_release/mittest/palf_cluster/
./run_palf_bench.sh
```

5. Check Result

check workdir of the server with minimal IP:PORT
```
$tail palf_raw_result_exp_test/palf_append_1500_512_3_1000.result -n 5
[2023-09-27 11:29:25.048444] EDIAG [CLOG] try_handle_cb_queue (ob_log_apply_service.cpp:505) [62033][T1_ApplySrv5][T1][Y0-0000000000000000-0-0] [lt=51][errcode=0] result:(l_append_cnt=602962, l_log_body_size=308716544, l_rt=1465294805, avg_body_size=512, avg_rt=2430) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d8dd1 0x55807b1d8a88 0x55807b1d8886 0x55807b1d867d 0xc9e7562 0xa5f6f30 0xa5f5d86 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:26.049100] EDIAG [CLOG] try_handle_cb_queue (ob_log_apply_service.cpp:505) [62037][T1_ApplySrv9][T1][Y0-0000000000000000-0-0] [lt=39][errcode=0] result:(l_append_cnt=590605, l_log_body_size=302389760, l_rt=1463385996, avg_body_size=512, avg_rt=2477) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d8dd1 0x55807b1d8a88 0x55807b1d8886 0x55807b1d867d 0xc9e7562 0xa5f6f30 0xa5f5d86 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:27.050335] EDIAG [CLOG] try_handle_cb_queue (ob_log_apply_service.cpp:505) [62033][T1_ApplySrv5][T1][Y0-0000000000000000-0-0] [lt=28][errcode=0] result:(l_append_cnt=604878, l_log_body_size=309697536, l_rt=1462297116, avg_body_size=512, avg_rt=2417) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d8dd1 0x55807b1d8a88 0x55807b1d8886 0x55807b1d867d 0xc9e7562 0xa5f6f30 0xa5f5d86 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:28.051049] EDIAG [CLOG] try_handle_cb_queue (ob_log_apply_service.cpp:505) [62033][T1_ApplySrv5][T1][Y0-0000000000000000-0-0] [lt=28][errcode=0] result:(l_append_cnt=593036, l_log_body_size=303634432, l_rt=1462893463, avg_body_size=512, avg_rt=2466) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d8dd1 0x55807b1d8a88 0x55807b1d8886 0x55807b1d867d 0xc9e7562 0xa5f6f30 0xa5f5d86 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:29.051212] EDIAG [CLOG] try_handle_cb_queue (ob_log_apply_service.cpp:505) [62034][T1_ApplySrv6][T1][Y0-0000000000000000-0-0] [lt=28][errcode=0] result:(l_append_cnt=596583, l_log_body_size=305449984, l_rt=1464339470, avg_body_size=511, avg_rt=2454) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d8dd1 0x55807b1d8a88 0x55807b1d8886 0x55807b1d867d 0xc9e7562 0xa5f6f30 0xa5f5d86 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d

$tail palf_raw_result_exp_test/palf_io_1500_512_3_1000.result -n 5
[2023-09-27 11:29:24.910336] EDIAG [CLOG] inner_write_impl_ (log_block_handler.cpp:458) [61989][T1_IOWorker][T1][Y0-0000000000000000-0-0] [lt=28][errcode=0] io result:(l_io_cnt=1438, l_log_size=333262848, avg_size=231754) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d3228 0x55807b1d2edf 0x55807b1d2cd8 0x55807b1d2ad7 0xca638f1 0xa73358f 0xa732cfa 0xa732761 0xa731caf 0xcab6358 0xa7310b4 0xa73012b 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:25.911515] EDIAG [CLOG] inner_write_impl_ (log_block_handler.cpp:458) [61989][T1_IOWorker][T1][Y0-0000000000000000-0-0] [lt=19][errcode=0] io result:(l_io_cnt=1441, l_log_size=330817536, avg_size=229574) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d3228 0x55807b1d2edf 0x55807b1d2cd8 0x55807b1d2ad7 0xca638f1 0xa73358f 0xa732cfa 0xa732761 0xa731caf 0xcab6358 0xa7310b4 0xa73012b 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:26.911259] EDIAG [CLOG] inner_write_impl_ (log_block_handler.cpp:458) [61989][T1_IOWorker][T1][Y0-0000000000000000-0-0] [lt=22][errcode=0] io result:(l_io_cnt=1425, l_log_size=331571200, avg_size=232681) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d3228 0x55807b1d2edf 0x55807b1d2cd8 0x55807b1d2ad7 0xca638f1 0xa73358f 0xa732cfa 0xa732761 0xa731caf 0xcab6358 0xa7310b4 0xa73012b 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:27.912235] EDIAG [CLOG] inner_write_impl_ (log_block_handler.cpp:458) [61989][T1_IOWorker][T1][Y0-0000000000000000-0-0] [lt=21][errcode=0] io result:(l_io_cnt=1406, l_log_size=328134656, avg_size=233381) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d3228 0x55807b1d2edf 0x55807b1d2cd8 0x55807b1d2ad7 0xca638f1 0xa73358f 0xa732cfa 0xa732761 0xa731caf 0xcab6358 0xa7310b4 0xa73012b 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d
[2023-09-27 11:29:28.912356] EDIAG [CLOG] inner_write_impl_ (log_block_handler.cpp:458) [61989][T1_IOWorker][T1][Y0-0000000000000000-0-0] [lt=19][errcode=0] io result:(l_io_cnt=1413, l_log_size=332730368, avg_size=235477) BACKTRACE:0xde38230 0xdaa64e6 0x55807b1d3228 0x55807b1d2edf 0x55807b1d2cd8 0x55807b1d2ad7 0xca638f1 0xa73358f 0xa732cfa 0xa732761 0xa731caf 0xcab6358 0xa7310b4 0xa73012b 0xe111e5a 0xe10e640 0x7ff51add6e25 0x7ff51a1d4f1d


$tail palf_raw_result_exp_test/palf_group_1500_512_3_1000.result -n 5
[2023-09-27 11:29:25.042978] INFO  [PALF] handle_next_submit_log_ (log_sliding_window.cpp:1056) [61992][T1_LogLoop][T1][Y0-0000000000000000-0-0] [lt=41] [PALF STAT GROUP LOG INFO](palf_id=1, self="SERVER_IP1:53212", role="LEADER", total_group_log_cnt=1667, avg_log_batch_cnt=361, total_group_log_size=327750672, avg_group_log_size=196611)
[2023-09-27 11:29:26.046598] INFO  [PALF] handle_next_submit_log_ (log_sliding_window.cpp:1056) [62250][][T0][Y0-0000000000000000-0-0] [lt=44] [PALF STAT GROUP LOG INFO](palf_id=1, self="SERVER_IP1:53212", role="LEADER", total_group_log_cnt=1613, avg_log_batch_cnt=367, total_group_log_size=322325464, avg_group_log_size=199829)
[2023-09-27 11:29:27.046847] INFO  [PALF] handle_next_submit_log_ (log_sliding_window.cpp:1056) [61992][T1_LogLoop][T1][Y0-0000000000000000-0-0] [lt=21] [PALF STAT GROUP LOG INFO](palf_id=1, self="SERVER_IP1:53212", role="LEADER", total_group_log_cnt=1680, avg_log_batch_cnt=360, total_group_log_size=329294128, avg_group_log_size=196008)
[2023-09-27 11:29:28.048014] INFO  [PALF] handle_next_submit_log_ (log_sliding_window.cpp:1056) [61992][T1_LogLoop][T1][Y0-0000000000000000-0-0] [lt=22] [PALF STAT GROUP LOG INFO](palf_id=1, self="SERVER_IP1:53212", role="LEADER", total_group_log_cnt=1607, avg_log_batch_cnt=369, total_group_log_size=322903656, avg_group_log_size=200935)
[2023-09-27 11:29:29.048263] INFO  [PALF] handle_next_submit_log_ (log_sliding_window.cpp:1056) [61992][T1_LogLoop][T1][Y0-0000000000000000-0-0] [lt=56] [PALF STAT GROUP LOG INFO](palf_id=1, self="SERVER_IP1:53212", role="LEADER", total_group_log_cnt=1611, avg_log_batch_cnt=370, total_group_log_size=324553496, avg_group_log_size=201460)
```
