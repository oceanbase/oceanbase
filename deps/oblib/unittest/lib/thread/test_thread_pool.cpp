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
#include <iostream>
#include "lib/thread/thread_pool.h"
#include "lib/time/ob_time_utility.h"
#include "../coro/testing.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

TEST(TestThreadPool, DISABLED_Submit1)
{
  // construct thread pool.
  class : public ThreadPool
  {
    void run1() override
    {
      while (!has_set_stop()) {
        ::usleep(100L * 1000L);
      }
    }
  } tp;
  tp.init();
  tp.start();

  // submit would success since there's no tasks.
  int ret = OB_SUCCESS;
  ret = tp.submit([] {
    cout << "ok2" << endl;
    ::usleep(1 * 1000L * 1000L);
  });
  EXPECT_EQ(OB_SUCCESS, ret);

  // submit would return size overflow fail since the previous task
  // hasn't finished and max number of running task is 1.
  ret = tp.submit([] { cout << "ok3" << endl; });
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);

  // after 2s, previous task would be finished so that new task should
  // be accepted.
  ::usleep(2 * 1000L * 1000L);
  ret = tp.submit([] { cout << "ok4" << endl; });
  EXPECT_EQ(OB_SUCCESS, ret);

  ::usleep(1 * 1000L * 1000L);
  ret = tp.submit([] { cout << "ok5" << endl; });
  EXPECT_EQ(OB_SUCCESS, ret);

  ::usleep(1 * 1000L * 1000L);
  ret = tp.submit([] { cout << "ok6" << endl; });
  EXPECT_EQ(OB_SUCCESS, ret);

  tp.stop();
  tp.wait();
  tp.destroy();
}

TEST(TestThreadPool, DISABLED_SubmitX)
{
  // construct thread pool.
  class : public ThreadPool
  {
    void run1() override
    {
      while (!has_set_stop()) {
        ::usleep(100L * 1000L);
      }
    }
  } tp;
  tp.init();
  tp.start();

  TIME_LESS(100*1000L, [&tp] {
    ::usleep(10L * 1000L);
    cout << ObTimeUtility::current_time() << endl;
    tp.submit([] {
      cout << ObTimeUtility::current_time() << endl;
    });
  });

  tp.stop();
  tp.wait();
  tp.destroy();

}

TEST(TestThreadPool, DISABLED_Submit2)
{
  // construct thread pool.
  class : public ThreadPool
  {
    void run1() override
    {
      ::usleep(3 * 1000L * 1000L);
    }
  } tp;
  tp.init();
  tp.start();

  // submit would success since there's no tasks.
  int ret = OB_SUCCESS;
  ret = tp.submit([] {
    cout << "ok2" << endl;
    ::usleep(1 * 1000L * 1000L);
  });
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tp.submit([] {
    cout << "ok2" << endl;
    ::usleep(1 * 1000L * 1000L);
  });
  EXPECT_EQ(OB_SUCCESS, ret);

  // submit would return size overflow fail since the previous tasks
  // hasn't finished and max number of running task is 2.
  ret = tp.submit([] { cout << "ok3" << endl; });
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);

  // after 2s, previous task would be finished so that new task should
  // be accepted.
  ::usleep(2 * 1000L * 1000L);
  ret = tp.submit([] { cout << "ok4" << endl; });
  EXPECT_EQ(OB_SUCCESS, ret);

  // tp.stop();
  tp.wait();
  tp.destroy();
}

TEST(TestThreadPool, DISABLED_SubmitN)
{
  // construct thread pool.
  class : public ThreadPool
  {
    void run1() override
    {
      ::usleep(3 * 1000L * 1000L);
    }
  } tp;
  tp.init();
  tp.set_thread_count(4);
  tp.start();

  int ret = OB_SUCCESS;
  int i = 0;
  for (i = 0; i < 100; i++) {
    ret = tp.submit([] {
      cout << "ok2" << endl;
      ::usleep(1 * 1000L * 1000L);
    });
    if (OB_FAIL(ret)) {
      break;
    }
  }
  EXPECT_EQ(8, i); // 2 * 4

  // wait 2s for previous tasks finishing.
  ::usleep(2 * 1000L * 1000L);
  tp.set_thread_count(5);
  for (i = 0; i < 100; i++) {
    ret = tp.submit([] {
      cout << "ok2" << endl;
      ::usleep(1 * 1000L * 1000L);
    });
    if (OB_FAIL(ret)) {
      break;
    }
  }
  EXPECT_EQ(10, i); // 2 * 5

  // tp.stop();
  tp.wait();
  tp.destroy();
}

TEST(TestThreadPool, DISABLED_LoopCheckConcurrency)
{
  class : public ThreadPool
  {
    void run1() override
    {
      while (!has_set_stop()) {
        ::usleep(3 * 1000L * 1000L);
      }
    }
  } tp;

  tp.init();
  tp.set_thread_count(4);
  tp.start();

  for (int i = 0; i < 1000; i++) {
    int n = 0;
    ASSERT_EQ(OB_SUCCESS, tp.submit([&n] { ATOMIC_INC(&n); }));
    ASSERT_EQ(OB_SUCCESS, tp.submit([&n] { ATOMIC_INC(&n); }));
    ASSERT_EQ(OB_SUCCESS, tp.submit([&n] { ATOMIC_INC(&n); }));
    while (ATOMIC_LOAD(&n) != 3) {
      ::usleep(1*1000L);
    }
    cout << "loop: " << i << endl;
  }

  tp.stop();
  tp.wait();
  tp.destroy();
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
