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

#include "observer/omt/ob_worker_pool.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::observer;

class TestWorkerPool : public ::testing::Test {
public:
  TestWorkerPool() : pool_(procor_)
  {}

  virtual void SetUp()
  {
    ASSERT_EQ(OB_SUCCESS, pool_.init(1, 5, 10));
  }

  virtual void TearDown()
  {
    pool_.destroy();
  }

protected:
  ObFakeWorkerProcessor procor_;
  ObGlobalContext gctx_;
  ObWorkerPool pool_;
};

TEST_F(TestWorkerPool, TestName)
{
  ObThWorker* ws[10] = {NULL};
  for (int i = 0; i < 10; ++i) {
    ObThWorker* w = pool_.alloc();
    EXPECT_TRUE(w);
    ws[i] = w;
  }
  EXPECT_FALSE(pool_.alloc());
  for (int i = 0; i < 10; ++i) {
    pool_.free(ws[i]);
  }
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
