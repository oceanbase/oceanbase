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
#define private public
#define protected public
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#define USING_LOG_PREFIX TRANS
#include "tx_node.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;
class ObTestTxPerf : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    ObClockGenerator::init();
  }
  virtual void TearDown()
  {
    TRANS_LOG(INFO, "tearDown");
    ObClockGenerator::destroy();
  }
  MsgBus bus_;
};


int worker_cnt = 200;
int test_time = 10;

TEST_F(ObTestTxPerf, test_sp_trans_perf)
{
  // 创建节点
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);

  DEFER(delete(n1));
  // 启动节点
  ASSERT_EQ(OB_SUCCESS, n1->start());

  std::vector<std::thread> ths;
  bool g_stop = false;
  uint64_t pk = 0;
  uint64_t req = 0;

  auto do_worker = [&] () {
    int ret = OB_SUCCESS;
    while (!g_stop) {
      int64_t now =  ObTimeUtility::current_time();
      int64_t timeout = now + 10 * 1000 * 1000;
      uint64_t trace_id_val = now;
      int64_t sp1 = 0;
      ObCurTraceId::set(&trace_id_val);
      LOG_INFO("start trans");
      ObTxDescGuard tx_guard = n1->get_tx_guard();
      if (!tx_guard.is_valid()) {
        LOG_ERROR("acquire_tx", K(ret));
        break;
      }
      auto &tx = tx_guard.get_tx_desc();
      ObTxParam tx_param;
      tx_param.timeout_us_ = timeout;
      tx_param.access_mode_ = ObTxAccessMode::RW;
      tx_param.isolation_ = ObTxIsolationLevel::RC;
      tx_param.cluster_id_ = 100;
      { // prepare snapshot for write
        ObTxReadSnapshot snapshot;
        if (OB_FAIL(n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             timeout,
                             snapshot))) {
          LOG_ERROR("get_read_snapshot", K(ret));
          break;
        }
        tx.sub_tx_.snapshot_ = snapshot;
      }
      if (OB_FAIL(n1->create_implicit_savepoint(tx, tx_param, sp1))) {
        LOG_ERROR("create_implicit_savepoint", K(ret));
        break;
      }

      uint64_t new_pk = ATOMIC_AAF(&pk, 1);
      if (OB_FAIL(n1->write(tx, new_pk, new_pk))) {
        LOG_ERROR("write", K(ret));
        break;
      }
      if (OB_FAIL(n1->commit_tx(tx, timeout))) {
        LOG_ERROR("commit_tx", K(ret));
        break;
      }
      ATOMIC_INC(&req);
      LOG_INFO("end trans");
    }
    if (OB_FAIL(ret)) {
      LOG_INFO("worker quit ret", K(ret));
      std::cout << "worker quit ret " << ret << std::endl;
    }
  };
  for (int i = 0; i < worker_cnt; i++) {
    std::thread th(do_worker);
    ths.push_back(std::move(th));
  }

  uint64_t last_req = 0;
  for (int i=0;i<test_time;i++) {
    uint64_t curr_req = ATOMIC_LOAD(&req);
    std::cout << "tps:" << curr_req - last_req << std::endl;
    last_req = curr_req;
    ::sleep(1);
  }
  g_stop = true;

  for (auto &th : ths) {
    th.join();
  }
  std::cout << req << std::endl;
}

} // oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_tx_perf.log*");
  if (argc > 1) {
    oceanbase::worker_cnt = atoi(argv[1]);
  }
  if (argc > 2) {
    oceanbase::test_time = atoi(argv[2]);
  }
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tx_perf.log", true, false,
                       "test_tx_perf.log", // rs
                       "test_tx_perf.log", // election
                       "test_tx_perf.log"); // audit
  OB_LOGGER.set_log_level("ERROR");
  STORAGE_LOG(INFO, "begin unittest: test tx perf");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
