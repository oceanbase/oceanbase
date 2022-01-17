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
#include <vector>

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"

#include "liboblog/src/ob_log_fetcher_svr_finder.h"

#include "test_log_fetcher_common_utils.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace fetcher;
using namespace transaction;
using namespace storage;
using namespace clog;

namespace oceanbase
{
namespace unittest
{

/*
 * SvrFinder Tests.
 *
 */
/*
 * Basic function test 1.
 *  - N thread & M requests for each thread
 */
namespace basic_func_test_1
{
class MockSystableHelper : public ObILogSysTableHelper
{
public:
  virtual int query_all_clog_history_info_by_log_id_1(const common::ObPartitionKey &pkey, const uint64_t log_id,
                                                      AllClogHistoryInfos &records) {
    // Generate random results.
    int ret = OB_SUCCESS;
    int64_t seed = get_timestamp();
    records.reset();
    AllClogHistoryInfoRecord rec;
    const int64_t cnt = 1 + (seed % 6);
    for (int64_t idx = 0; idx < cnt; ++idx) {
      rec.reset();
      rec.table_id_ = (uint64_t)(pkey.table_id_);
      rec.partition_idx_ = (int32_t)(pkey.get_partition_id());
      rec.partition_cnt_ = pkey.get_partition_cnt();
      rec.start_log_id_ = log_id;
      rec.end_log_id_ = log_id + 10000;
      rec.start_log_timestamp_ = seed - (1 * _HOUR_);
      rec.end_log_timestamp_ = seed + (1 * _HOUR_);
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", (seed % 128));
      rec.svr_port_ = 8888;
      records.push_back(rec);
      seed += 13;
    }
    return ret;
  }

  virtual int query_all_clog_history_info_by_timestamp_1(const common::ObPartitionKey &pkey, const int64_t timestamp,
                                                         AllClogHistoryInfos &records) {
    // Generate random results.
    int ret = OB_SUCCESS;
    int64_t seed = get_timestamp();
    records.reset();
    AllClogHistoryInfoRecord rec;
    const int64_t cnt = 1 + (seed % 6);
    for (int64_t idx = 0; idx < cnt; ++idx) {
      rec.reset();
      rec.table_id_ = (uint64_t)(pkey.table_id_);
      rec.partition_idx_ = (int32_t)(pkey.get_partition_id());
      rec.partition_cnt_ = (int32_t)(pkey.get_partition_cnt());
      rec.start_log_id_ = 0;
      rec.end_log_id_ = 65536;
      rec.start_log_timestamp_ = timestamp - (1 * _HOUR_);
      rec.end_log_timestamp_ = timestamp + (1 * _HOUR_);
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", (seed % 128));
      rec.svr_port_ = 8888;
      records.push_back(rec);
      seed += 13;
    }
    return ret;
  }
  virtual int query_all_meta_table_1(const common::ObPartitionKey &pkey, AllMetaTableRecords &records) {
    // Generate random results.
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    int64_t seed = get_timestamp();
    records.reset();
    AllMetaTableRecord rec;
    const int64_t cnt = 1 + (seed % 6);
    for (int64_t idx = 0; idx < cnt; ++idx) {
      rec.reset();
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", (seed % 128));
      rec.svr_port_ = 8888;
      rec.role_ = (0 == idx) ? LEADER : FOLLOWER;
      records.push_back(rec);
      seed += 13;
    }
    return ret;
  }
  virtual int query_all_meta_table_for_leader(
    const common::ObPartitionKey &pkey,
    bool &has_leader,
    common::ObAddr &leader)
  {
    UNUSED(pkey);
    has_leader = true;
    leader.set_ip_addr("127.0.0.1", 8888);
    return OB_SUCCESS;
  }
  virtual int query_all_server_table_1(AllServerTableRecords &records) {
    int ret = OB_SUCCESS;
    records.reset();
    AllServerTableRecord rec;
    for (int64_t idx = 0; idx < 128; ++idx) {
      rec.reset();
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", (idx));
      rec.svr_port_ = 8888;
      records.push_back(rec);
    }
    return ret;
  }
};

/*
 * Worker.
 */
class TestWorker : public Runnable
{
public:
  SvrFinder *svrfinder_;
  virtual int routine()
  {
    // Build requests.
    const int64_t RequestCnt = 10 * 10000;
    SvrFindReq *request_array = new SvrFindReq[RequestCnt];
    for (int64_t idx = 0, cnt = RequestCnt; idx < cnt; ++idx) {
      SvrFindReq &r = request_array[idx];
      r.reset();
      r.pkey_ = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);
      const int64_t seed = get_timestamp();
      if ((seed % 100) < 50) {
        r.req_by_start_tstamp_ = true;
        r.start_tstamp_ = seed;
      }
      else {
        r.req_by_next_log_id_ = true;
        r.next_log_id_ = (uint64_t)(seed % 65536);
      }
    }
    // Push requests into svrfinder.
    for (int64_t idx = 0, cnt = RequestCnt; idx < cnt; ++idx) {
      SvrFindReq &r = request_array[idx];
      EXPECT_EQ(OB_SUCCESS, svrfinder_->async_svr_find_req(&r));
      if (0 == (idx % 1000)) {
        usec_sleep(10 * _MSEC_);
      }
    }
    // Wait for requests end. Max test time should set.
    int64_t end_request_cnt = 0;
    const int64_t TestTimeLimit = 10 * _MIN_;
    const int64_t start_test_tstamp = get_timestamp();
    while (((get_timestamp() - start_test_tstamp) < TestTimeLimit)
           && (end_request_cnt < RequestCnt)) {
      for (int64_t idx = 0, cnt = RequestCnt; idx < cnt; ++idx) {
        SvrFindReq &r = request_array[idx];
        if (SvrFindReq::DONE == r.get_state()) {
          end_request_cnt += 1;
          _E_(">>> svr list size", "size", r.svr_list_.count());
          r.set_state_idle();
        }
      }
      usec_sleep(100 * _MSEC_);
    }
    // Assert if test cannot finish.
    EXPECT_EQ(RequestCnt, end_request_cnt);
    delete[] request_array;
    return OB_SUCCESS;
  }
};

TEST(DISABLED_SvrFinder, BasicFuncTest1)
{
  MockFetcherErrHandler1 err_handler1;
  MockSystableHelper systable_helper;
  FixedJobPerWorkerPool worker_pool;
  SvrFinder svrfinder;

  int err = OB_SUCCESS;
  err = worker_pool.init(1);
  EXPECT_EQ(OB_SUCCESS, err);

  err = svrfinder.init(&systable_helper, &err_handler1, &worker_pool, 3);
  EXPECT_EQ(OB_SUCCESS, err);

  const int64_t TestWorkerCnt = 3;
  TestWorker workers[TestWorkerCnt];
  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
    w.svrfinder_ = &svrfinder;
    w.create();
  }

  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
    w.join();
  }

  err = svrfinder.destroy();
  EXPECT_EQ(OB_SUCCESS, err);

  err = worker_pool.destroy();
  EXPECT_EQ(OB_SUCCESS, err);
}

}

}
}

int main(int argc, char **argv)
{
  //ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
