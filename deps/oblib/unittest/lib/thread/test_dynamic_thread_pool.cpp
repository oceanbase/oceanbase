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

#include "lib/thread/ob_dynamic_thread_pool.h"
#include <gtest/gtest.h>
#include <malloc.h>
#include "lib/utility/ob_test_util.h"
#include "lib/restore/ob_storage.h"

using namespace oceanbase::common;
class TestObDynamicThreadPool: public ::testing::Test
{
public:
  TestObDynamicThreadPool() {}
  virtual ~TestObDynamicThreadPool(){}
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }

  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObDynamicThreadPool);
protected:
  // function members
protected:
};

class SimpleTask: public ObDynamicThreadTask
{
public:
  int process(const bool &is_stop)
  {
    UNUSED(is_stop);
    ATOMIC_INC(&count_);
    return OB_SUCCESS;
  }
  static int64_t count_;
};

int64_t SimpleTask::count_ = 0;


TEST_F(TestObDynamicThreadPool, normal)
{
  ObDynamicThreadPool pool;
  const int64_t task_count = 10000;
  SimpleTask task;
  SimpleTask::count_ = 0;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  ASSERT_EQ(OB_SUCCESS, pool.set_task_thread_num(10));

  for (int64_t i = 0; i < task_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, pool.add_task(&task));
  }
  sleep(1);
  pool.stop();
  pool.destroy();
  ASSERT_EQ(task_count, SimpleTask::count_);
}

TEST_F(TestObDynamicThreadPool, test_change_thread_num)
{
  ObDynamicThreadPool pool;
  const int64_t task_count = 10000;
  SimpleTask task;
  ObRandom rand;
  SimpleTask::count_ = 0;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  ASSERT_EQ(OB_SUCCESS, pool.set_task_thread_num(512));

  for (int64_t i = 0; i < task_count; ++i) {
    if (pool.get_task_count() < ObDynamicThreadPool::MAX_TASK_NUM) {
      ASSERT_EQ(OB_SUCCESS, pool.add_task(&task));
    }
    pool.set_task_thread_num(rand.rand(0, ObDynamicThreadPool::MAX_THREAD_NUM));
  }
  pool.set_task_thread_num(1);
  sleep(1);
  pool.stop();
  pool.destroy();
  ASSERT_EQ(task_count, SimpleTask::count_);

}

int main(int argc, char **argv)
{
  mallopt(M_ARENA_MAX, 1);  // disable malloc multiple arena pool
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_dynamic_thread_pool.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

