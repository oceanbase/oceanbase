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
#include "all_mock.h"
#include "observer/omt/ob_th_worker.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_worker_processor.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::observer;

class TestWorker : public ObThWorker, public ::testing::Test {
public:
  TestWorker() : ObThWorker(procor_), omt_(procor_)
  {
    all_mock_init();
  }

  virtual void SetUp()
  {
    EXPECT_EQ(OB_SUCCESS, init());
    // EXPECT_EQ(1, start());
    EXPECT_EQ(0, start());
    usleep(100);
  }

  virtual void TearDown()
  {
    omt_.destroy();
    stop();
    resume();
    wait();
    destroy();
  }

protected:
  ObFakeWorkerProcessor procor_;
  ObMultiTenant omt_;
};

TEST_F(TestWorker, Init)
{
  EXPECT_FALSE(pause_flag_);
  // EXPECT_TRUE(th_ != NULL);
  EXPECT_TRUE(is_inited_);
  EXPECT_EQ(NULL, tenant_);
  EXPECT_FALSE(large_query_);
  EXPECT_LE(0, query_start_time_);
  EXPECT_LE(0, last_check_time_);
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
