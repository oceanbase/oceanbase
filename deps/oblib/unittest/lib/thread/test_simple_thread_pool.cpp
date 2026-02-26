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
#include "lib/thread/ob_simple_thread_pool.h"
#undef private
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
TEST(TestSimpleThreadPool, test_dynamic_simple_thread_pool_bind)
{
  class ObTestSimpleThreadPool : public ObSimpleThreadPool {
    void handle(void *) {
    }
  };
  int ret = ObSimpleThreadPoolDynamicMgr::get_instance().init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ObTestSimpleThreadPool pool;
  ret = pool.set_adaptive_thread(1, 3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(pool.has_bind_);

  cotesting::FlexPool bg_thread([&pool] {
    {
      SpinWLockGuard guard(ObSimpleThreadPoolDynamicMgr::get_instance().simple_thread_pool_list_lock_);
      pool.inc_ref();
    }
    usleep(10 * 1000);
    pool.dec_ref();
  }, 1);
  bg_thread.start(false);
  usleep(1000);
  pool.stop();
  ASSERT_FALSE(pool.has_bind_);
  pool.wait();
  ASSERT_EQ(pool.get_ref_cnt(), 0);
  bg_thread.wait();
}

TEST(TestSimpleThreadPool, DISABLED_test_dynamic_simple_thread_pool)
{
  class ObTestSimpleThreadPool : public ObSimpleThreadPool {
    void handle(void *task) {
      int64_t time = reinterpret_cast<int64_t>(task);
      ::usleep(time);
      ATOMIC_INC(&handle_cnt_);
    }
  public:
    int handle_cnt_ = 0;
  };
  int ret = OB_SUCCESS;
  const int push_thread_count = 10;
  const int push_count = 100;
  const int64_t handle_time = 30000;

  const int task_cnt = push_thread_count * push_count;
  const int64_t min_thread_cnt = 0;
  const int64_t max_thread_cnt = 12;


  ObTestSimpleThreadPool *pool = new ObTestSimpleThreadPool();
  ret = ObSimpleThreadPoolDynamicMgr::get_instance().init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = pool->set_adaptive_thread(min_thread_cnt, max_thread_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(ObSimpleThreadPoolDynamicMgr::get_instance().get_pool_num(), 1);
  ret = pool->init(max_thread_cnt, 20000, "qth");
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(min_thread_cnt, pool->get_thread_count());
  int64_t total_push_time = 0;

  // start push task
  int64_t total_handle_time = ObTimeUtility::current_time();
  int64_t push_err_cnt = 0;
  cotesting::FlexPool([&pool, &total_push_time, &push_err_cnt] {
    int64_t cur_us = ObTimeUtility::current_time();
    for (int i = 0; i < push_count; i++) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(pool->push(reinterpret_cast<void *>(handle_time)))) {
        fprintf(stderr, "push failed, i = %d\n", i);
        ATOMIC_AAF(&push_err_cnt, 1);
        break;
      }
    }
    ATOMIC_AAF(&total_push_time, ObTimeUtility::current_time() - cur_us);
  }, push_thread_count).start();
  ASSERT_EQ(ATOMIC_LOAD(&push_err_cnt), 0);
  ::usleep(10000);
  ASSERT_EQ(max_thread_cnt, pool->get_thread_count());
  // wait task handle
  do {
    ::usleep(100);
  } while (ATOMIC_LOAD(&pool->handle_cnt_) < task_cnt);
  total_handle_time = ObTimeUtility::current_time() - total_handle_time;
  ASSERT_EQ(max_thread_cnt, pool->get_thread_count());

  // wait to thread pool shrink to min_thread_cnt
  int64_t wait_time = (max_thread_cnt - min_thread_cnt) * ObSimpleThreadPoolDynamicMgr::SHRINK_INTERVAL_US + 2000000;
  ::usleep(wait_time);
  ASSERT_EQ(min_thread_cnt, pool->get_thread_count());
  pool->destroy();
  delete pool;
  ASSERT_EQ(ObSimpleThreadPoolDynamicMgr::get_instance().get_pool_num(), 0);

  // compare to normal thread
  int64_t total_push_time2 = 0;
  int64_t total_handle_time2 = 0;
  ObTestSimpleThreadPool *pool2 = new ObTestSimpleThreadPool();
  ret = pool2->init(max_thread_cnt, 20000, "nqth");
  ASSERT_EQ(ret, OB_SUCCESS);
  // start push task
  total_handle_time2 = ObTimeUtility::current_time();
  push_err_cnt = 0;
  cotesting::FlexPool([&pool2, &total_push_time2, &push_err_cnt] {
    int64_t cur_us = ObTimeUtility::current_time();
    for (int i = 0; i < push_count; i++) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(pool2->push(reinterpret_cast<void *>(handle_time)))) {
        fprintf(stderr, "push failed, i = %d\n", i);
        ATOMIC_AAF(&push_err_cnt, 1);
        break;
      }
    }
    ATOMIC_AAF(&total_push_time2, ObTimeUtility::current_time() - cur_us);
  }, push_thread_count).start();
  ASSERT_EQ(ATOMIC_LOAD(&push_err_cnt), 0);
  // wait task handle
  do {
    ::usleep(100);
  } while (ATOMIC_LOAD(&pool2->handle_cnt_) < task_cnt);
  total_handle_time2 = ObTimeUtility::current_time() - total_handle_time2;

  double ratio = total_handle_time * 1.0 / total_handle_time2;
  fprintf(stdout, "pool1 push: %ld, handle: %ld\n", total_push_time, total_handle_time);
  fprintf(stdout, "pool2 push: %ld, handle: %ld, ratio: %lf\n", total_push_time2, total_handle_time2, ratio);
  ASSERT_LT(ratio, 1.02);


  cotesting::FlexPool([&pool2] {
    int64_t cur_us = ObTimeUtility::current_time();
    for (int i = 0; i < push_count; i++) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(pool2->push(reinterpret_cast<void *>(handle_time)))) {
        fprintf(stderr, "push failed, i = %d\n", i);
        break;
      }
    }
  }, push_thread_count).start();
  ret = pool2->set_max_thread_count(0);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = pool2->set_max_thread_count(max_thread_cnt + 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(ObSimpleThreadPoolDynamicMgr::get_instance().get_pool_num(), 1);
  ::usleep(100000);
  ASSERT_EQ(max_thread_cnt + 1, pool2->get_thread_count());

  ret = pool2->set_max_thread_count(max_thread_cnt - 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(max_thread_cnt - 1, pool2->get_thread_count());
  ASSERT_EQ(ObSimpleThreadPoolDynamicMgr::get_instance().get_pool_num(), 0);
  pool2->destroy();
  delete pool2;
  ObSimpleThreadPoolDynamicMgr::get_instance().destroy();
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_simple_thread_pool.log", true);
  return RUN_ALL_TESTS();
}
