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
#include "ob_seq_thread.h"
namespace oceanbase
{
namespace common
{
class MyClass {};

class CThread : public ObSeqThread<256, MyClass>
{
public:
  CThread() {}
  virtual ~CThread() {}

public:
  virtual int handle(void *task, const int64_t task_seq, const int64_t thread_index, volatile bool &stop_flag)
  {
    if (! stop_flag) {
      EXPECT_EQ(task_seq + 1, (int64_t)task);
    }
    UNUSED(thread_index);
    return 0;
  }
};

class TestSeqThread : public ::testing::Test
{
public:
  TestSeqThread() {}
  ~TestSeqThread() {}

  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestSeqThread, basic)
{
  CThread thread;
  // Parameter not legal
  EXPECT_EQ(OB_INVALID_ARGUMENT, thread.init(257, 100));
  EXPECT_EQ(OB_INVALID_ARGUMENT, thread.init(0, 0));

  EXPECT_EQ(OB_SUCCESS, thread.init(256, 10000));
  EXPECT_EQ(OB_SUCCESS, thread.start());
  for (int64_t index = 0; index < 1000; index++) {
    EXPECT_EQ(OB_SUCCESS, thread.push((void*)(index + 1), index, 0));
  }
  sleep(1);
  thread.stop();
  EXPECT_EQ(true, thread.is_stoped());
}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
