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
#include "lib/thread/thread_mgr.h"
#include "lib/task/ob_timer.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

namespace oceanbase
{
namespace unittest
{

// 模拟 ObMySQLConnectionPool 的关键行为
class MockConnectionPoolTask : public ObTimerTask
{
public:
  MockConnectionPoolTask() : is_stop_(false), tg_id_(-1) {}

  int start(int tg_id)
  {
    int ret = OB_SUCCESS;
    tg_id_ = tg_id;
    is_stop_ = false;

    // 调度第一个任务
    if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, 100000, false))) { // 100ms后执行
      LIB_LOG(ERROR, "fail to schedule first task", K(ret));
    } else {
      LIB_LOG(INFO, "task scheduled", K(tg_id_));
    }
    return ret;
  }

  void stop()
  {
    int ret = OB_SUCCESS;
    if (!is_stop_) {
      if (tg_id_ != -1) {
        LIB_LOG(INFO, "stopping", K(tg_id_));
        if (OB_FAIL(TG_CANCEL_ALL(tg_id_))) {
          LIB_LOG(ERROR, "fail to cancel timer task", K(ret));
        } else {
          TG_STOP(tg_id_);
          TG_WAIT(tg_id_);
          tg_id_ = -1;
          LIB_LOG(INFO, "stop completed");
        }
      }
      is_stop_ = true;
    }
  }

  void runTimerTask() override
  {
    int ret = OB_SUCCESS;
    LIB_LOG(INFO, "runTimerTask executing");

    // 模拟一些处理
    ::usleep(100000); // 100ms

    // 重新调度下一个任务（模拟原始代码的行为）
    if (!is_stop_) {
      if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, 100000, false))) {
        LIB_LOG(ERROR, "fail to re-schedule task", K(ret));
      } else {
        LIB_LOG(INFO, "task re-scheduled");
      }
    } else {
      LIB_LOG(INFO, "not re-scheduling, is_stop_ is true");
    }
  }

private:
  volatile bool is_stop_;
  int tg_id_;
};

class TestConnectionPoolDeadlock : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }

  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

  virtual void SetUp() override
  {
    int tg_id = lib::TGDefIDs::TEST1;
    if (TG_EXIST(tg_id)) {
      TG_STOP(tg_id);
      TG_WAIT(tg_id);
    }
  }

  virtual void TearDown() override
  {
    int tg_id = lib::TGDefIDs::TEST1;
    if (TG_EXIST(tg_id)) {
      TG_STOP(tg_id);
      TG_WAIT(tg_id);
    }
  }
};

// 简单的启动-停止测试
// 使用 TG_CANCEL_ALL 时可能死锁，改为 TG_CANCEL 后正常
TEST_F(TestConnectionPoolDeadlock, test_start_and_stop)
{
  int ret = OB_SUCCESS;
  UNUSED(ret);
  ObClockGenerator::get_instance().init();

  MockConnectionPoolTask task;
  int tg_id = lib::TGDefIDs::TEST1;

  LIB_LOG(INFO, "========== Test start ==========");

  // 启动定时器组
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));

  // 启动任务
  ASSERT_EQ(OB_SUCCESS, task.start(tg_id));

  // 等待任务运行一会儿
  LIB_LOG(INFO, "waiting for task to run...");
  ::usleep(200000); // 200ms，确保任务至少执行一次

  // 停止任务
  LIB_LOG(INFO, "calling stop...");
  task.stop();

  LIB_LOG(INFO, "========== Test completed ==========");

  TG_DESTROY(tg_id);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
