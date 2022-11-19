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
#include "share/ob_thread_mgr.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

class MyRunnable : public TGRunnable
{
public:
  void run1() override
  {
    run_count_++;
    while (!has_set_stop()) {
      ::usleep(50000);
    }
  }
  int64_t run_count_=0;
};

TEST(TG, ob_th)
{
  int tg_id = TGDefIDs::TEST_OB_TH;
  MyRunnable runnable;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE(tg_id, runnable));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(1, runnable.run_count_);

  // restart
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE(tg_id, runnable));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, runnable.run_count_);

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}

int main(int argc, char *argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
