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

#include "storage/tx/ob_trans_timer.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_rpc.h"
#include "../mockcontainer/mock_ob_location_adapter.h"
#include "storage/ob_partition_service.h"
#include "storage/tx/ob_trans_sche_ctx.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace memtable;
using namespace storage;

namespace unittest
{
class TestObTransTimer : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;

  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  static const char *LOCAL_IP;
  static const int64_t TENANT_ID = 1001;
};
ObTransService trans_service;
const char *TestObTransTimer::LOCAL_IP = "127.0.0.1";

//////////////////////basic function test//////////////////////////////////////////
// init of ObTransTimeoutTask
// reset of ObTransTimeoutTask
// destroy of ObTransTimeoutTask
TEST_F(TestObTransTimer, init_reset_destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  //init of ObTransTimeoutTask
  ObScheTransCtxMgr sche_trans_ctx_mgr;
  ObAddr observer(TestObTransTimer::IP_TYPE, TestObTransTimer::LOCAL_IP, TestObTransTimer::PORT);
  ObTransID trans_id(TestObTransTimer::PORT);
  ObTransTimer timer;
  EXPECT_EQ(OB_SUCCESS, timer.init());
  ObTransCtx ctx;
  ObTransTimeoutTask task;
  EXPECT_EQ(OB_SUCCESS, task.init(&ctx));
  task.reset();
  task.destroy();
}
// runTimerTask of obtranstimeouttask
TEST_F(TestObTransTimer, run_timer_task)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObAddr observer(TestObTransTimer::IP_TYPE, TestObTransTimer::LOCAL_IP, TestObTransTimer::PORT);
  ObTransID trans_id(TestObTransTimer::PORT);
  ObTransTimer timer;
  EXPECT_EQ(OB_SUCCESS, timer.init());
  EXPECT_EQ(OB_SUCCESS, timer.start());
  // when executing timeout task, the task needs to get the trans ctx first
  // and to call the handle_timeout function
  // next, after add_partition, create the trans ctx
  ObPartTransCtxMgr part_trans_ctx_mgr;
  bool alloc = true;
  ObTransCtx *ctx = NULL;

  EXPECT_EQ(OB_SUCCESS, part_trans_ctx_mgr.init(&trans_service));
  EXPECT_EQ(OB_SUCCESS, part_trans_ctx_mgr.start());
  /*
  EXPECT_TRUE(OB_SUCCESS == part_trans_ctx_mgr.add_partition(partition_key, ObVersion(100, 100)));
  EXPECT_TRUE(OB_SUCCESS == part_trans_ctx_mgr.leader_takeover(partition_key));
  EXPECT_TRUE(OB_SUCCESS == part_trans_ctx_mgr.leader_active(partition_key));
  EXPECT_EQ(OB_SUCCESS, part_trans_ctx_mgr.get_trans_ctx(partition_key, trans_id,
      false, false, alloc, ctx));
  */

  ObPartTransCtx *part_trans_ctx = static_cast<ObPartTransCtx *>(ctx);
  // ObTransRpc rpc;
  // common::hash::ObHashMap<common::ObPartitionKey, common::ObAddr> ps_map;
  // EXPECT_EQ(OB_SUCCESS, ps_map.create(100,1));
  // MockObLocationAdapter location_adapter(&ps_map);
  ObClogAdapter clog_adapter;

  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  const uint64_t thread_id = 1000;
  const uint64_t cluster_version = 100;

  // ObAddr addr;
  // addr.set_ip_addr("127.0.0.1", 34506);
  ObMemtableCtxFactory mt_ctx_factory;
  ObPartitionService partition_service;
  ObTransService trans_service;
  //EXPECT_EQ(OB_ERR_UNEXPECTED, part_trans_ctx->init(TestObTransTimer::TENANT_ID, trans_id, ObClockGenerator::getClock() + 10000000,
  //     partition_key, &part_trans_ctx_mgr, parms, cluster_version, &trans_service, thread_id));

  // init for ObTransTimeoutTask
  ObTransTimeoutTask task;
  EXPECT_EQ(OB_SUCCESS, task.init(&part_trans_ctx));
  // when waiting the init of part_trans_ctx
  // if registered timer task is finished, the task can still be unregistered
  // note that if unregiter is noe called, core dumps sometimes 
  // ObClockGenerator::msleep(2000);
  task.runTimerTask();

  task.destroy();
}
//////////////////////boundary test for ObTransTimeoutTask//////////////////////////////////////////
// failed init, invalid input parameters, repeated init 
TEST_F(TestObTransTimer, init_invalid_args_repeat)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // init for ObTransTimeoutTask
  ObScheTransCtxMgr sche_trans_ctx_mgr;
  ObAddr observer(TestObTransTimer::IP_TYPE, TestObTransTimer::LOCAL_IP, TestObTransTimer::PORT);
  ObTransID trans_id(TestObTransTimer::PORT);
  ObTransTimer *timer = NULL;
  ObTransCtx ctx;
  ObTransTimeoutTask task;

  timer = new ObTransTimer();
  EXPECT_TRUE(NULL != timer);
  EXPECT_EQ(OB_SUCCESS, timer->init());

  EXPECT_EQ(OB_SUCCESS, task.init(&ctx));
  EXPECT_EQ(OB_INIT_TWICE, task.init(&ctx));

  task.reset();
  task.destroy();
  if (NULL != timer) {
    delete timer;
    timer = NULL;
  }
}
// failure test for run_timer_task of obtranstimeouttask
// 1. not init
// 2. set NULL for ctx_mgr
// 3. fail to get trans ctx
TEST_F(TestObTransTimer, run_timer_task_failure)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransTimeoutTask task;
  ObScheTransCtxMgr *sche_trans_ctx_mgr = NULL;
  ObAddr observer(TestObTransTimer::IP_TYPE, TestObTransTimer::LOCAL_IP, TestObTransTimer::PORT);
  ObTransID trans_id(TestObTransTimer::PORT);
  ObTransTimer timer;
  EXPECT_EQ(OB_SUCCESS, timer.init());

  // not init for task
  task.runTimerTask();
  ObTransCtx *ctx = NULL;
  EXPECT_EQ(OB_INVALID_ARGUMENT, task.init(ctx));
  sche_trans_ctx_mgr = new ObScheTransCtxMgr();
  EXPECT_TRUE(NULL != sche_trans_ctx_mgr);
  ctx = new ObTransCtx();
  EXPECT_EQ(OB_SUCCESS, task.init(ctx));
  EXPECT_EQ(OB_INIT_TWICE, task.init(ctx));
  // init task successfully, but fail to get trans ctx
  task.runTimerTask();

  if (NULL != sche_trans_ctx_mgr) {
    delete sche_trans_ctx_mgr;
    sche_trans_ctx_mgr = NULL;
    delete ctx;
    ctx = NULL;
  }
}
//////////////////////basic function test for ObTransTimer//////////////////////////////////////////
// init, register, unregister, destroy of ObTransTimer
TEST_F(TestObTransTimer, trans_timer_init_destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // init timer
  ObTransTimer timer;
  EXPECT_EQ(OB_SUCCESS, timer.init());
  EXPECT_EQ(OB_SUCCESS, timer.start());
  ObTransTimeoutTask task;
  const int64_t DELAY = 1000 * 1000;
  EXPECT_EQ(OB_SUCCESS, timer.register_timeout_task(task, DELAY));
  EXPECT_EQ(OB_SUCCESS, timer.unregister_timeout_task(task));
  EXPECT_EQ(OB_SUCCESS, timer.stop());
  EXPECT_EQ(OB_SUCCESS, timer.wait());
  timer.destroy();
}
// after registering a timeout task, destroy the task immediately  
TEST_F(TestObTransTimer, register_task_destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // init timer
  ObTransTimer timer;
  EXPECT_EQ(OB_SUCCESS, timer.init());
  EXPECT_EQ(OB_SUCCESS, timer.start());
  // create a timeout task
  ObTransTimeoutTask task;
  ObTransCtx ctx;
  const int64_t DELAY = 1000 * 1000;
  // init the task
  ObAddr observer(TestObTransTimer::IP_TYPE, TestObTransTimer::LOCAL_IP, TestObTransTimer::PORT);
  ObTransID trans_id(TestObTransTimer::PORT);
  ObPartTransCtxMgr part_trans_ctx_mgr;
  EXPECT_EQ(OB_SUCCESS, part_trans_ctx_mgr.init(&trans_service));
  EXPECT_EQ(OB_SUCCESS, task.init(&ctx));

  EXPECT_EQ(OB_SUCCESS, timer.register_timeout_task(task, DELAY));
  task.destroy();
  EXPECT_EQ(OB_SUCCESS, timer.stop());
  EXPECT_EQ(OB_SUCCESS, timer.wait());
}
//////////////////////boundary test for ObTransTimer//////////////////////////////////////////
// not init timer
// init timer repeatedly
// break the order in which the functions start, stop and wait are called
TEST_F(TestObTransTimer, timer_not_init_repeat)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransTimer timer;
  // create a timeout task
  ObTransTimeoutTask task;
  ObTransCtx ctx;
  const int64_t DELAY = 1000 * 1000;
  // init the task
  ObAddr observer(TestObTransTimer::IP_TYPE, TestObTransTimer::LOCAL_IP, TestObTransTimer::PORT);
  ObTransID trans_id(TestObTransTimer::PORT);
  ObPartTransCtxMgr part_trans_ctx_mgr;
  EXPECT_EQ(OB_SUCCESS, part_trans_ctx_mgr.init(&trans_service));
  EXPECT_EQ(OB_SUCCESS, task.init(&ctx));

  // not init
  EXPECT_EQ(OB_NOT_INIT, timer.start());
  EXPECT_EQ(OB_NOT_INIT, timer.register_timeout_task(task, DELAY));
  EXPECT_EQ(OB_NOT_INIT, timer.unregister_timeout_task(task));
  EXPECT_EQ(OB_NOT_INIT, timer.stop());
  EXPECT_EQ(OB_NOT_INIT, timer.wait());

  // after initing successfully, but not call start
  EXPECT_EQ(OB_SUCCESS, timer.init());
  EXPECT_EQ(OB_INIT_TWICE, timer.init());
  EXPECT_EQ(OB_NOT_RUNNING, timer.register_timeout_task(task, DELAY));
  EXPECT_EQ(OB_TIMER_TASK_HAS_NOT_SCHEDULED, timer.unregister_timeout_task(task));
  EXPECT_EQ(OB_NOT_RUNNING, timer.stop());

  // after start, wait is called before calling stop
  EXPECT_EQ(OB_SUCCESS, timer.start());
  EXPECT_EQ(OB_ERR_UNEXPECTED, timer.start());
  EXPECT_EQ(OB_SUCCESS, timer.register_timeout_task(task, DELAY));
  EXPECT_EQ(OB_SUCCESS, timer.unregister_timeout_task(task));
  EXPECT_EQ(OB_ERR_UNEXPECTED, timer.wait());
  EXPECT_EQ(OB_SUCCESS, timer.stop());
  EXPECT_EQ(OB_SUCCESS, timer.wait());
}
// register_timeout_task
// 1. register an invalid task with delay less than 0
// 2. register a task repeatedly
TEST_F(TestObTransTimer, register_task)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransTimer timer;
  EXPECT_EQ(OB_SUCCESS, timer.init());
  EXPECT_EQ(OB_SUCCESS, timer.start());
  // create a timeout task
  ObTransTimeoutTask task;
  ObTransCtx ctx;
  int64_t delay = -1000 * 1000;
  // init the tast
  ObAddr observer(TestObTransTimer::IP_TYPE, TestObTransTimer::LOCAL_IP, TestObTransTimer::PORT);
  ObTransID trans_id(TestObTransTimer::PORT);
  ObPartTransCtxMgr part_trans_ctx_mgr;
  EXPECT_EQ(OB_SUCCESS, part_trans_ctx_mgr.init(&trans_service));
  EXPECT_EQ(OB_SUCCESS, task.init(&ctx));
  EXPECT_EQ(OB_INVALID_ARGUMENT, timer.register_timeout_task(task, delay));

  delay = 1000 * 1000;
  EXPECT_EQ(OB_SUCCESS, timer.register_timeout_task(task, delay));
  // register a task repeatedly
  EXPECT_EQ(OB_TIMER_TASK_HAS_SCHEDULED, timer.register_timeout_task(task, delay));
  EXPECT_EQ(OB_SUCCESS, timer.stop());
  EXPECT_EQ(OB_SUCCESS, timer.wait());
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_timer.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  if (OB_SUCCESS != ObClockGenerator::init()) {
    TRANS_LOG(WARN, "clock generator init error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
