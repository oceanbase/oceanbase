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

#define UNITTEST_DEBUG

#include "lib/future/ob_future.h"
#include <gtest/gtest.h>
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;

class TestObFuture: public ::testing::Test
{
public:
  TestObFuture() {};
  virtual ~TestObFuture() {};
  virtual void SetUp() { };
  virtual void TearDown() {
    ASSERT_EQ(function::DefaultFunctionAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(future::DefaultFutureAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(guard::DefaultSharedGuardAllocator::get_default_allocator().total_alive_num, 0);
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObFuture);
};

TEST_F(TestObFuture, normal) {
  cout << "size:" << sizeof(ObFuture<int>) << "," << sizeof(ObPromise<int>) << endl;
  ObPromise<int> promise;
  ASSERT_EQ(promise.is_valid(), false);
  {
    ObFuture<int> future = promise.get_future();
    ASSERT_EQ(future.is_ready(), false);
    ASSERT_EQ(future.is_valid(), false);
  }
  ASSERT_EQ(promise.set(5), OB_NOT_INIT);
  ASSERT_EQ(promise.init(), OB_SUCCESS);
  ASSERT_EQ(promise.is_valid(), true);
  ASSERT_EQ(promise.init(), OB_INIT_TWICE);
  ObFuture<int> future = promise.get_future();
  ASSERT_EQ(future.is_ready(), false);
  ASSERT_EQ(future.is_valid(), true);
  thread t([future]() {
    auto start = std::chrono::system_clock::now();
    int *temp = nullptr;
    ASSERT_EQ(future.wait_for(30), OB_TIMEOUT);
    ASSERT_EQ(future.wait_for(30), OB_TIMEOUT);
    ASSERT_EQ(future.get(temp), OB_SUCCESS);
    auto end = std::chrono::system_clock::now();
    cout << "wait " << chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << endl;
    ASSERT_EQ(*temp, 5);
    ASSERT_EQ(future.is_ready(), true);
    ASSERT_EQ(future.is_valid(), true);
    ASSERT_EQ(future.get(temp), OB_SUCCESS);
  });
  std::this_thread::sleep_for(chrono::milliseconds(100));
  ASSERT_EQ(promise.set(5), OB_SUCCESS);
  ASSERT_EQ(promise.set(6), OB_OP_NOT_ALLOW);
  t.join();
}

TEST_F(TestObFuture, return_void) {
  cout << "size:" << sizeof(ObFuture<int>) << "," << sizeof(ObPromise<int>) << endl;
  ObPromise<void> promise;
  ASSERT_EQ(promise.is_valid(), false);
  {
    ObFuture<void> future = promise.get_future();
    ASSERT_EQ(future.is_ready(), false);
    ASSERT_EQ(future.is_valid(), false);
  }
  ASSERT_EQ(promise.init(), OB_SUCCESS);
  ASSERT_EQ(promise.is_valid(), true);
  ASSERT_EQ(promise.init(), OB_INIT_TWICE);
  ObFuture<void> future = promise.get_future();
  ASSERT_EQ(future.is_ready(), false);
  ASSERT_EQ(future.is_valid(), true);
  thread t([future]() {
    auto start = std::chrono::system_clock::now();
    ASSERT_EQ(future.wait_for(30), OB_TIMEOUT);
    ASSERT_EQ(future.wait_for(30), OB_TIMEOUT);
    ASSERT_EQ(future.wait(), OB_SUCCESS);
    auto end = std::chrono::system_clock::now();
    cout << "wait " << chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << endl;
    ASSERT_EQ(future.is_ready(), true);
    ASSERT_EQ(future.is_valid(), true);
    ASSERT_EQ(future.wait(), OB_SUCCESS);
  });
  std::this_thread::sleep_for(chrono::milliseconds(100));
  ASSERT_EQ(promise.set(), OB_SUCCESS);
  t.join();
}

TEST_F(TestObFuture, promise_destroy_first) {
  {
    ObFuture<void> future;
    {
      ObPromise<void> promise;
      ASSERT_EQ(promise.init(), OB_SUCCESS);
      future = promise.get_future();
      promise.stop_and_notify_all();
    }
    ASSERT_EQ(future.wait(), OB_NOT_RUNNING);
  }

  {
    ObFuture<void> future;
    std::thread *th;
    {
      ObPromise<void> promise;
      ASSERT_EQ(promise.init(), OB_SUCCESS);
      future = promise.get_future();
      th = new thread([promise]() mutable {
        this_thread::sleep_for(chrono::milliseconds(10));
        promise.stop_and_notify_all();
      });
    }
    ASSERT_EQ(future.wait(), OB_NOT_RUNNING);
    th->join();
  }

   {
    ObFuture<void> future;
    std::thread *th;
    {
      ObPromise<void> promise;
      ASSERT_EQ(promise.init(), OB_SUCCESS);
      future = promise.get_future();
      th = new thread([promise]() mutable {
        this_thread::sleep_for(chrono::milliseconds(10));
        promise.stop_and_notify_all();
      });
    }
    ASSERT_EQ(future.wait_for(100), OB_NOT_RUNNING);
    th->join();
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_future.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_future.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}