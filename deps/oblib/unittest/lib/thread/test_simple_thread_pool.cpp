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
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/coro/testing.h"

using namespace oceanbase::common;

TEST(DISABLED_TestSimpleThreadPool, Basic)
{
  class : public ObSimpleThreadPool {
    void handle(void *task) {
      UNUSED(task);
      ATOMIC_INC(&handle_cnt_);
    }
  public:
    int handle_cnt_ = 0;
  } pool;
  EXPECT_NE(OB_SUCCESS, pool.init(0, 10));
  EXPECT_NE(OB_SUCCESS, pool.init(-1, 10));
  EXPECT_NE(OB_SUCCESS, pool.init(-1234567, 10));
  EXPECT_NE(OB_SUCCESS, pool.init(1, 0));
  EXPECT_NE(OB_SUCCESS, pool.init(1, -1));

  // When routine without coro push items into this pool, there may be
  // at most 100ms(default timeout) before a item to be processing.
  //
  // Update: ob_futex has fixed problem of thread waking up co-routine
  TIME_LESS(10000, [&pool] {
    pool.handle_cnt_ = 0;
    ASSERT_EQ(OB_SUCCESS, pool.init(1, 10));
    ::usleep(1000);  // wait for handler waiting for queue
    ASSERT_EQ(OB_SUCCESS, pool.push((void*)1));
    ASSERT_EQ(OB_SUCCESS, pool.push((void*)1));
    ASSERT_EQ(OB_SUCCESS, pool.push((void*)1));
    for (int i = 0; i < 1000; i++) {
      if (pool.handle_cnt_ == 3) {
        break;
      }
      ::usleep(1000);
    }
    ASSERT_EQ(3, pool.handle_cnt_);
  });
  pool.destroy();

  // When routine with coro push items into this pool, it would be
  // processed ASAP.
  TIME_LESS(10000, [&pool] {
    cotesting::FlexPool([&pool] {
      pool.handle_cnt_ = 0;
      ASSERT_EQ(OB_SUCCESS, pool.init(1, 10));
      ::usleep(1000);  // wait for handler waiting for queue
      ASSERT_EQ(OB_SUCCESS, pool.push((void*)1));
      ASSERT_EQ(OB_SUCCESS, pool.push((void*)1));
      ASSERT_EQ(OB_SUCCESS, pool.push((void*)1));
      for (int i = 0; i < 1000; i++) {
        if (pool.handle_cnt_ == 3) {
          break;
        }
        ::usleep(1000);
      }
      ASSERT_EQ(3, pool.handle_cnt_);
    }, 1).start();
  });
  pool.destroy();

}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
