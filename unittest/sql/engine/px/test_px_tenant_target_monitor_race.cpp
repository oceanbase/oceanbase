/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include "share/ob_rpc_share.h"
#include "sql/engine/px/ob_px_tenant_target_monitor.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

// 每个线程跑多少轮 apply_target+release_target
static const int64_t LOOPS_PER_THREAD = 300000;
// 并发线程数，建议不少于机器核数，这样才容易真正并发命中同一条 ++/-- 指令
static const int THREAD_COUNT = 32;

TEST(ObPxTenantTargetMonitorRaceTest, ParallelSessionCountLostUpdate)
{
  // apply_target()/release_target() 内部会 init 一个 ObRpcProxy(rpc_proxy_)，
  // 该 proxy 只是被存了个指针，本测试自始至终不会真正发起 RPC，
  // 所以这里塞一个非空的假指针即可让 init() 通过参数校验。
  share::set_obrpc_transport(reinterpret_cast<rpc::frame::ObReqTransport *>(0x1));

  ObAddr server;
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 1234));

  ObPxTenantTargetMonitor monitor;
  ASSERT_EQ(OB_SUCCESS, monitor.init(/*tenant_id=*/1, server));
  // 设成足够大，保证每次 apply_target 都能准入成功，不会走 need_wait 分支，
  // 这样才能让所有线程都真正执行到 parallel_session_count_++ / --。
  monitor.set_parallel_servers_target(INT64_MAX);

  std::vector<std::thread> workers;
  for (int i = 0; i < THREAD_COUNT; ++i) {
    workers.emplace_back([&monitor, &server]() {
      for (int64_t j = 0; j < LOOPS_PER_THREAD; ++j) {
        hash::ObHashMap<ObAddr, int64_t> worker_map;
        worker_map.create(10, "TEST", "TEST");
        worker_map.set_refactored(server, 1);

        int64_t admit_cnt = 0;
        uint64_t admit_version = 0;
        bool session_count_inc = false;
        monitor.apply_target(worker_map, /*wait_time_us=*/0, /*session_target=*/INT64_MAX,
                              /*req_cnt=*/1, admit_cnt, admit_version, session_count_inc);
        if (admit_cnt > 0) {
          monitor.release_target(worker_map, admit_version, session_count_inc);
        }
      }
    });
  }
  for (auto &t : workers) {
    t.join();
  }

  int64_t final_count = monitor.get_parallel_session_count();
  fprintf(stderr, "[race-repro] final parallel_session_count_ = %ld (expected 0)\n", final_count);
  // 有 bug 时：由于丢失更新，这里大概率不为 0（可能为正也可能为负）。
  // 打了 ATOMIC_INC/ATOMIC_DEC 补丁之后，这里应该总是精确等于 0。
  EXPECT_EQ(0, final_count);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
