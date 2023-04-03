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
#include "share/ob_thread_pool.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/executor/ob_interm_result_pool.h"
#include "sql/ob_sql_init.h"
#include <stdlib.h>
#include <math.h>
#include <time.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ThreadContext
{
public:
  static const int64_t COL_NUM = 16;
  static const int64_t MEM_LIMIT = 2147483648;

  ThreadContext();
  virtual ~ThreadContext();

  int alloc_scanner(ObScanner *&scanner);
  int free_scanner(ObScanner *scanner);
private:
  ObScannerPool* scanner_pool_;
  oceanbase::common::ObNewRow row_;
  oceanbase::common::ObNewRow tmp_row_;
  ObObj objs_[COL_NUM];
  ObObj tmp_objs_[COL_NUM];
};

ThreadContext::ThreadContext()
{
  scanner_pool_ = ObScannerPool::get_instance();

  row_.count_ = COL_NUM;
  row_.cells_ = objs_;
  for (int64_t i = 0; i < COL_NUM; ++i) {
    row_.cells_[i].set_int((int) rand());
  } // end for

  tmp_row_.count_ = COL_NUM;
  tmp_row_.cells_ = tmp_objs_;
}

ThreadContext::~ThreadContext()
{
}

int ThreadContext::alloc_scanner(ObScanner *&scanner)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = scanner_pool_->alloc_scanner(OB_SYS_TENANT_ID, ThreadContext::MEM_LIMIT, scanner)))
  {
    _OB_LOG(WARN, "stress, fail to alloc scanner, ret:%d, scanner_pool remain:%ld", ret, scanner_pool_->remain());
  } else {
    //_OB_LOG(INFO, "succ alloc");
  }
  if (OB_FAIL(ret)) {
    //empty
  }
  return ret;
}

int ThreadContext::free_scanner(ObScanner *scanner)
{
  int ret = OB_SUCCESS;
  scanner->reuse();
  if (OB_SUCCESS != (ret = scanner_pool_->free_scanner(scanner))) {
    _OB_LOG(ERROR, "free scanner fail");
  } else {
    //empty
  }
  return ret;
}

class StressRunner : public share::ObThreadPool
{
public:
  static const int64_t IR_INFO_COUNT = 1024;
  static const int64_t THREAD_COUNT = 16;

  StressRunner();
  virtual ~StressRunner();

  void run1();

  volatile bool running_;
  volatile bool alloc_;
  volatile bool free_;
private:
  // disallow copy
  StressRunner(const StressRunner &other);
  StressRunner& operator=(const StressRunner &ohter);
private:
  int64_t last_time_;
  ThreadContext tc_[THREAD_COUNT];
};

StressRunner::StressRunner() : running_(true), alloc_(true), free_(true)
{
  srand((unsigned int)time(NULL));
  last_time_ = -1;
  set_thread_count(THREAD_COUNT);
}

StressRunner::~StressRunner()
{
}

void StressRunner::run1()
{

  long tc_num = (long) get_thread_idx();
  while(running_) {
    if (0 == tc_num) {
      int64_t cur_time = ::oceanbase::common::ObTimeUtility::current_time();
      if (last_time_ == -1 || cur_time - last_time_ > 1000000) {
        last_time_ = cur_time;
        ob_print_mod_memory_usage();
      }
    } else {
      //_OB_LOG(WARN, "tc_num=%ld, add_=%d", tc_num, (int) add_);
      ObScanner *scanner = NULL;
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = tc_[tc_num].alloc_scanner(scanner))) {
        //_OB_LOG(WARN, "alloc fail");
      } else {
        scanner->reuse();
        ret = tc_[tc_num].free_scanner(scanner);
        if (OB_FAIL(ret)) {
          _OB_LOG(ERROR, "free scanner fail, ret=%d", ret);
        }
      }
    }
  }
}

class ObIntermResultManagerTest : public ::testing::Test
{
public:
  StressRunner runner_;

  ObIntermResultManagerTest();
  virtual ~ObIntermResultManagerTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObIntermResultManagerTest(const ObIntermResultManagerTest &other);
  ObIntermResultManagerTest& operator=(const ObIntermResultManagerTest &other);
private:
};

ObIntermResultManagerTest::ObIntermResultManagerTest()
{
}

ObIntermResultManagerTest::~ObIntermResultManagerTest()
{
}

void ObIntermResultManagerTest::SetUp()
{
}

void ObIntermResultManagerTest::TearDown()
{
}

TEST_F(ObIntermResultManagerTest, basic_test)
{
  runner_.start();
  while(true) {
    sleep(15);
    runner_.alloc_ = false;
    sleep(15);
    runner_.free_ = false;
    sleep(15);
    runner_.alloc_ = true;
    sleep(15);
    runner_.free_ = true;
  }
  runner_.stop();
  runner_.wait();
}

int main(int argc, char **argv)
{
  oceanbase::sql::init_sql_executor_singletons();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
