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

#include <gtest/gtest.h>
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/random/ob_random.h"
#include "lib/container/ob_se_array.h"
#include "lib/coro/testing.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

const int64_t update_cnt = 100000;
class TestSessionMultiThread: public Threads
{
public:
  TestSessionMultiThread() {}
  virtual ~TestSessionMultiThread() {}
  virtual void run(int64_t idx)
  {
    uint64_t session_id = (long)idx % 2 + 1;
    ObSessionStatEstGuard session_guard(1, session_id, true /*is_multi_thread_plan*/);
    ObDiagnoseSessionInfo *di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    ASSERT_TRUE(NULL != di);
    for (int i = 0; i < update_cnt; i++) {
      di->update_stat(ObStatEventIds::IO_READ_COUNT, 1);
    }
  }
};

TEST(ObDICache, multithread)
{
  bool stop = false;
  int64_t round = 0;
  cotesting::FlexPool pool1([&stop]{
    while (!stop) {
      uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
      ObSEArray<std::pair<uint64_t, ObDISessionCollect*>, OB_MAX_SERVER_SESSION_CNT+1> session_status;
      ObDISessionCache::get_instance().get_all_diag_info(session_status);
      uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
      printf("session cnt: %ld, scan session time: %ld us\n", session_status.count(), (end_time - begin_time));

      sleep(1);
    }
  }, 1);
  pool1.start(false);
  cotesting::FlexPool pool2([&stop, &round]{
    ObSessionStatEstGuard guard(1,1);
    while (!stop) {
      //uint32_t tenant_id = (uint32_t)rand.get(1+(uint64_t)arg*100, 100+(uint64_t)arg*100);
      for (int64_t i = 0; i < 1000; ++i)  {
        EVENT_INC(REQUEST_ENQUEUE_COUNT);
      }

      ATOMIC_INC(&round);
    }
  }, 1);
  pool2.start(false);

  int64_t qps = 0;
  int64_t value = 0;
  for (int64_t i = 0; i < 2; ++i) {
    qps = round - value;
    value = round;
    printf("qps: %ld\n", qps);
    sleep(1);
  }
  stop = true;
  pool1.wait();
  pool2.wait();
}


TEST(ObDICache, session_multi_threads)
{
  ObDISessionCollect *collect;
  TestSessionMultiThread tester;
  ObSEArray<std::pair<uint64_t, ObDISessionCollect *>, 10> session_status;
  const int64_t th_cnt = 10;
  tester.set_thread_count(th_cnt);
  tester.start();
  tester.wait();
  ObDISessionCache::get_instance().get_the_diag_info(1, collect);
  ASSERT_EQ(th_cnt * update_cnt / 2, collect->base_value_.get_add_stat_stats().get(ObStatEventIds::IO_READ_COUNT)->stat_value_);
  ObDISessionCache::get_instance().get_all_diag_info(session_status);
  ASSERT_EQ(2, session_status.count());
  for (int i = 0; i < session_status.count(); i++) {
    collect = session_status.at(i).second;
    ASSERT_EQ(th_cnt * update_cnt / 2, collect->base_value_.get_add_stat_stats().get(ObStatEventIds::IO_READ_COUNT)->stat_value_);
  }
}

TEST(ObDICache, tenant)
{
  int64_t tenant_id = 1000;
  ObDiagnoseTenantInfo diag_info;
  bool stop = false;

  cotesting::FlexPool pool([&stop]{
    while (!stop) {
      for (uint64_t i = 1; i < 100000; i++) {
        ObSessionStatEstGuard guard(ObRandom::rand(1, 100), i);
        EVENT_ADD(RPC_PACKET_IN, 1);
      }
      sleep(1);
    }
  }, 4);

  pool.start(false);
  for (int64_t i = 0; i < 2; ++i) {
    tenant_id = 1000 + ObRandom::rand(0, 100);
    ObDIGlobalTenantCache::get_instance().get_the_diag_info(tenant_id, diag_info);
    sleep(1);
  }
  stop = true;
  pool.wait();
}


TEST(ObDISessionCache, multithread)
{
  bool stop = false;
  std::thread ths[4];
  for (uint64_t i = 1; i < 4; i++) {
    ths[i] = std::thread([&]() {
      while (!stop) {
        for (uint64_t i = 1; i < 100000; i++) {
          ObSessionStatEstGuard guard(1,i);
          EVENT_ADD(RPC_PACKET_IN, 1);
        }
        sleep(1);
      }});
  }

  sleep(5);
  stop = true;
  for (uint64_t i = 1; i < 4; i++) {
    ths[i].join();
  }

  common::ObArenaAllocator allocator;
  common::ObSEArray<std::pair<uint64_t, common::ObDiagnoseTenantInfo*>, common::OB_MAX_SERVER_TENANT_CNT> tenant_dis;
  ASSERT_EQ(OB_SUCCESS, common::ObDIGlobalTenantCache::get_instance().get_all_stat_event(allocator, tenant_dis));
  int64_t count = 0;
  for (int64_t i = 0; i < tenant_dis.count(); ++i) {
    ObStatEventAddStat *stat = tenant_dis.at(i).second->get_add_stat_stats().get(ObStatEventIds::RPC_PACKET_IN);
    if (NULL != stat) {
      count += stat->stat_value_;
    }
  }
  ASSERT_GT(count, 0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
