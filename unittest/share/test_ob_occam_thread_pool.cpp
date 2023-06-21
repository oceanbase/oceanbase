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

#include "share/ob_occam_thread_pool.h"
#include <gtest/gtest.h>
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "common/ob_clock_generator.h"

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;

class TestObOccamThreadPool: public ::testing::Test
{
public:
  TestObOccamThreadPool() : thread_pool(nullptr) {};
  virtual ~TestObOccamThreadPool() {};
  virtual void SetUp() {
    thread_pool = new ObOccamThreadPool();
    ASSERT_EQ(thread_pool->init_and_start(2, 1), OB_SUCCESS);
  };
  virtual void TearDown() {
    delete thread_pool;
    ASSERT_EQ(occam::DefaultAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(function::DefaultFunctionAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(future::DefaultFutureAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(guard::DefaultSharedGuardAllocator::get_default_allocator().total_alive_num, 0);
  };
  ObOccamThreadPool *thread_pool;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObOccamThreadPool);
};

TEST_F(TestObOccamThreadPool, ObOccamThread) {
  cout << "main thread id:" << this_thread::get_id() << endl;
  int ret = OB_SUCCESS;
  occam::ObOccamThread th;
  if (OB_FAIL(th.init_and_start([](){ cout << "th thread id:" << this_thread::get_id() << endl; }))) {}
  ASSERT_EQ(ret, OB_SUCCESS);
  th.wait();
  th.destroy();
}

struct TestInt {
  TestInt(int a) : data_(a) {}
  int data_;
};

struct TestBigInt {
  TestBigInt(int a) { data_[0] = a; }
  int data_[100];
};

TEST_F(TestObOccamThreadPool, thread_pool) {
  ObOccamThreadPool thread_pool_not_vaild;
  ASSERT_EQ(thread_pool_not_vaild.init_and_start(2, 17), OB_INVALID_ARGUMENT);
  int ret = OB_SUCCESS;
  {
    ObFuture<TestInt> result;
    int a = 3;
    int b = 4;
    cout << "origin b addr:" << &b << endl;
    ret = thread_pool->commit_task(result,
                                  [](int a, int &b)->TestInt{
                                    cout << "b addr:" << &b << endl;
                                    return TestInt(++a + ++b);
                                  },
                                  a,
                                  b);
    ASSERT_EQ(ret, OB_SUCCESS);
    TestInt *resultp;
    ASSERT_EQ(result.get(resultp), OB_SUCCESS);
    ASSERT_EQ(resultp->data_, 9);
    ASSERT_EQ(a, 3);
    ASSERT_EQ(b, 5);
  }
  ASSERT_EQ(ret, OB_SUCCESS);
}

TEST_F(TestObOccamThreadPool, thread_pool_big_obj) {
  int ret = OB_SUCCESS;
  {
    ObFuture<TestBigInt> result;
    int b = 4;
    cout << "origin b addr:" << &b << endl;
    ret = thread_pool->commit_task(result,
                                  [](const int a, int &b)->TestBigInt{
                                    cout << "b addr:" << &b << endl;
                                    return TestBigInt(a + ++b);
                                  },
                                  3,
                                  b);
    TestBigInt *resultp;
    result.get(resultp);
    ASSERT_EQ(resultp->data_[0], 8);
    ASSERT_EQ(b, 5);
  }
  ASSERT_EQ(ret, OB_SUCCESS);
}

TEST_F(TestObOccamThreadPool, queue_full) {
  // 线程池里有两个线程，队列长度是2，只能同事容纳两个等待执行的任务
  auto fun = []() { 
    this_thread::sleep_for(chrono::milliseconds(100));
    OB_LOG(DEBUG, "task done time", K(ObClockGenerator::getRealClock()));
  };
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "1", K(ObClockGenerator::getRealClock()));
  vector<ObFuture<void>> results;
  ObFuture<void> result;
  // start commit task
  ret = thread_pool->commit_task(result, fun);// 提交第一个任务
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  ret = thread_pool->commit_task(result, fun); // 提交第二个任务
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  OB_LOG(DEBUG, "2", K(ObClockGenerator::getRealClock()));
  this_thread::sleep_for(chrono::milliseconds(50));// 等待worker线程fetch任务，预期两个线程都把任务去出来了，现在任务队列空了
  // now all 2 threads are busy
  ret = thread_pool->commit_task(result, fun);// 提交第三个任务
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  ret = thread_pool->commit_task(result, fun);// 提交第四个任务
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  OB_LOG(DEBUG, "3", K(ObClockGenerator::getRealClock()));
  // now the queue is full
  ret = thread_pool->commit_task(result, fun);
  ASSERT_EQ(ret, OB_BUF_NOT_ENOUGH);// commit task failed
  this_thread::sleep_for(chrono::milliseconds(100));
  ret = thread_pool->commit_task(result, fun);
  OB_LOG(DEBUG, "4", K(ObClockGenerator::getRealClock()));
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  for (auto &result : results)
    result.wait();
}

TEST_F(TestObOccamThreadPool, task_priority) {
  ObOccamThreadPool *thread_pool = new ObOccamThreadPool();
  cout << "size of:" << sizeof(thread_pool) << endl;
  thread_pool->init_and_start(2);
  auto fun = []() { this_thread::sleep_for(chrono::milliseconds(100)); };
  int ret = OB_SUCCESS;
  vector<ObFuture<void>> results;
  ObFuture<void> result;
  // start commit task
  ret = thread_pool->commit_task(result, fun);
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  ret = thread_pool->commit_task(result, fun);
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  this_thread::sleep_for(chrono::milliseconds(1));
  // now all 2 threads are busy
  int a = 0;
  ret = thread_pool->commit_task(result, [&a](){ a = 3; });// commit first, but will execute later
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  ret = thread_pool->commit_task(result, [&a](){ a = 4; });// commit second, but will execute later
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  ret = thread_pool->commit_task<occam::TASK_PRIORITY::HIGH>(result, [&a](){ a = 2; });// last committed task, but will execute first
  ASSERT_EQ(ret, OB_SUCCESS);
  results.push_back(result);
  // now the queue is full
  for (auto &result : results)
    result.wait();
  ASSERT_NE(a, 2);
  delete thread_pool;
}

int64_t task(int64_t n)
{
  while(--n);
  return n;
}

vector<int64_t> run_all_without_thread_pool(vector<int64_t> &inputs)
{
  vector<int64_t> outputs;
  for (auto &input : inputs) {
    outputs.emplace_back(task(input));
  }
  return outputs;
}

vector<int64_t> run_all_with_thread_pool(vector<int64_t> &inputs, ObOccamThreadPool &th_pool)
{
  vector<int64_t> outputs;
  // 拆分提交异步任务
  vector<ObFuture<int64_t>> futures;
  ObFuture<int64_t> future;
  for (auto &input : inputs) {
    th_pool.commit_task(future, task, input);
    futures.push_back(future);
  }
  // 同步等待异步任务的结果
  int64_t *p_result;
  for (auto &future : futures) {
    future.get(p_result);
    outputs.push_back(*p_result);
  }
  return outputs;
}

bool compare_result_equal(const vector<int64_t> &v1, const vector<int64_t> &v2)
{
  bool ret = true;
  if (v1.size() != v2.size()) {
    ret = false;
  } else {
    for (size_t idx = 0; idx < v1.size() && ret; ++idx) {
      ret = (v1[idx] == v2[idx]);
    }
  }
  return ret;
}

TEST_F(TestObOccamThreadPool, heavy_task_sperate) {
  ObOccamThreadPool *thread_pool = new ObOccamThreadPool();
  thread_pool->init_and_start(2, 10);
  vector<int64_t> inputs(1000, 1000000);
  auto timepoint1 = chrono::steady_clock::now();
  vector<int64_t> outputs1 = run_all_without_thread_pool(inputs);
  auto timepoint2 = chrono::steady_clock::now();
  vector<int64_t> outputs2 = run_all_with_thread_pool(inputs, *thread_pool);
  auto timepoint3 = chrono::steady_clock::now();

  ASSERT_EQ(compare_result_equal(outputs1, outputs2), true);

  cout << "cost 1:" << chrono::duration_cast<chrono::milliseconds>(timepoint2 - timepoint1).count() << "ms" << endl;
  cout << "cost 2:" << chrono::duration_cast<chrono::milliseconds>(timepoint3 - timepoint2).count() << "ms" << endl;
  cout << "inputs[0]:" << inputs[0] << endl;
  delete thread_pool;
}

int test_f(int a, int b) {
  UNUSED(b);
  return a;
}

TEST_F(TestObOccamThreadPool, test_) {
  thread_pool->commit_task_ignore_ret(test_f, 1, 1);
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_occam_thread_pool.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_occam_thread_pool.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

