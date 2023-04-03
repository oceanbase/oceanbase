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

  ThreadContext();
  virtual ~ThreadContext();

  int add_result(ObIntermResultInfo &ir_info);
  int read_result(ObIntermResultInfo &ir_info);
  int read_and_delete_result(ObIntermResultInfo &ir_info);
private:
  int alloc_result(ObIntermResult *&ir);
  int free_result(ObIntermResult *ir);
  ObIntermResultManager* ir_manager_;
  oceanbase::common::ObNewRow row_;
  oceanbase::common::ObNewRow tmp_row_;
  ObObj objs_[COL_NUM];
  ObObj tmp_objs_[COL_NUM];
};

ThreadContext::ThreadContext()
{
  ir_manager_ = ObIntermResultManager::get_instance();

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

int ThreadContext::add_result(ObIntermResultInfo &ir_info)
{
  int ret = OB_SUCCESS;
  ObIntermResult *ir = NULL;
  int64_t expire_time = ::oceanbase::common::ObTimeUtility::current_time() + 2000000;
  if (OB_SUCCESS != (ret = alloc_result(ir))) {
    //_OB_LOG(WARN, "stress, fail to alloc result, ret:%d, ir_pool remain:%ld", ret, ir_pool_->remain());
  }
  else if (OB_SUCCESS != (ret = ir_manager_->add_result(ir_info, ir, expire_time))) {
    free_result(ir);
  } else {
    //empty
  }
  return ret;
}

int ThreadContext::read_result(ObIntermResultInfo &ir_info)
{
  int ret = OB_SUCCESS;
  ObIntermResultIterator iter;
  if (OB_SUCCESS != (ret = ir_manager_->get_result(ir_info, iter))) {
    //empty
  } else {
    //empty
  }

  ObIIntermResultItem *ir_item = NULL;
  ObScanner scanner;
  ObScanner::Iterator scanner_iter;
  bool has_got_first_scanner = false;
  int64_t cur_row_num = 0;
  while (OB_SUCC(ret)) {
    bool should_get_next_item = false;
    if (!has_got_first_scanner) {
      should_get_next_item = true;
    } else {
      if (OB_FAIL(scanner_iter.get_next_row(tmp_row_))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          should_get_next_item = true;
        }
      } else {
        cur_row_num++;
      }
    }

    if (OB_SUCC(ret) && should_get_next_item) {
      if (OB_FAIL(iter.get_next_interm_result_item(ir_item))) {
      } else {
        EXPECT_TRUE(NULL != ir_item);
        scanner.reset();
        if (!scanner.is_inited() && OB_FAIL(scanner.init())) {
        } else if (OB_FAIL(static_cast<ObIntermResultItem *>(ir_item)->to_scanner(scanner))) {
        } else {
          scanner_iter = scanner.begin();
          has_got_first_scanner = true;
        }
      }
    }
  }

  EXPECT_EQ(ret, OB_ITER_END);
  EXPECT_EQ(cur_row_num, 10000);
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ThreadContext::read_and_delete_result(ObIntermResultInfo &ir_info)
{
  int ret = OB_SUCCESS;
  ObIntermResultIterator iter;
  if (OB_SUCCESS != (ret = ir_manager_->get_result(ir_info, iter))) {
    //empty
  } else {
    //empty
  }

  ObIIntermResultItem *ir_item = NULL;
  ObScanner scanner;
  ObScanner::Iterator scanner_iter;
  bool has_got_first_scanner = false;
  int64_t cur_row_num = 0;
  while (OB_SUCC(ret)) {
    bool should_get_next_item = false;
    if (!has_got_first_scanner) {
      should_get_next_item = true;
    } else {
      if (OB_FAIL(scanner_iter.get_next_row(tmp_row_))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          should_get_next_item = true;
        }
      } else {
        cur_row_num++;
      }
    }

    if (OB_SUCC(ret) && should_get_next_item) {
      if (OB_FAIL(iter.get_next_interm_result_item(ir_item))) {
      } else {
        EXPECT_TRUE(NULL != ir_item);
        scanner.reset();
        if (!scanner.is_inited() && OB_FAIL(scanner.init())) {
        } else if (OB_FAIL(static_cast<ObIntermResultItem *>(ir_item)->to_scanner(scanner))) {
        } else {
          scanner_iter = scanner.begin();
          has_got_first_scanner = true;
        }
      }
    }
  }

  EXPECT_EQ(ret, OB_ITER_END);
  EXPECT_EQ(cur_row_num, 10000);
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
    //empty
  } else {
    ret = ir_manager_->delete_result(iter);
  }
  return ret;
}

int ThreadContext::alloc_result(ObIntermResult *&interm_result)
{
  int ret = OB_SUCCESS;
  ObIntermResult *ir = NULL;

  // add row
  if (OB_SUCCESS != (ret = ir_manager_->alloc_result(ir))) {
    //_OB_LOG(WARN, "stress, fail to alloc interm result, ret:%d, ir_pool remain:%ld", ret, ir_pool_->remain());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < 10000; ++i) {
    ret = ir->add_row(OB_SYS_TENANT_ID, row_);
  }
  if (OB_FAIL(ret))
  {
    ir_manager_->free_result(ir);
    ir = NULL;
  } else {
    //empty
  }
  interm_result = ir;
  return ret;
}

int ThreadContext::free_result(ObIntermResult *interm_result)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ir_manager_->free_result(interm_result))) {
    _OB_LOG(ERROR, "free ir fail");
  } else {
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
  volatile bool add_;
  volatile bool read_;
  volatile bool read_and_delete_;
private:
  // disallow copy
  StressRunner(const StressRunner &other);
  StressRunner& operator=(const StressRunner &ohter);
private:
  int64_t last_time_;
  ObIntermResultInfo ir_info_[IR_INFO_COUNT];
  ThreadContext tc_[THREAD_COUNT];
};

StressRunner::StressRunner() : running_(true), add_(true), read_(true), read_and_delete_(true)
{
  srand((unsigned int)time(NULL));
  last_time_ = -1;
  ObAddr server;
  server.set_ip_addr("127.0.0.1", 8888);

  int64_t i = 0;
  for (; i < IR_INFO_COUNT / 2; ++i) {
    ObSliceID slice_id;
    slice_id.set_server(server);
    slice_id.set_execution_id(1);
    slice_id.set_job_id(i + 1);
    slice_id.set_task_id(i + 2);
    slice_id.set_slice_id(i + 3);
    ir_info_[i].init(slice_id);
  }
  for (; i < IR_INFO_COUNT; ++i) {
    ObSliceID slice_id;
    slice_id.set_server(server);
    slice_id.set_execution_id(1);
    slice_id.set_job_id(i + 1);
    slice_id.set_task_id(i + 1);
    slice_id.set_slice_id(i + 1);
    ir_info_[i].init(slice_id);
  }
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
      if (last_time_ == -1 || cur_time - last_time_ > 1000000)
      {
        last_time_ = cur_time;
        ob_print_mod_memory_usage();
      }
    } else {
      //_OB_LOG(WARN, "tc_num=%ld, add_=%d", tc_num, (int) add_);
      int64_t idx = rand() % IR_INFO_COUNT;
      int64_t op = rand() % 6;
      if (0 <= op && 2 >= op && read_) {
        tc_[tc_num].read_result(ir_info_[idx]);
      } else if (3 <= op && 4 >= op && add_) {
        int ret = tc_[tc_num].add_result(ir_info_[idx]);
        if (OB_SUCC(ret)) {
          //_OB_LOG(WARN, "add result succ");
        }
      } else if (5 == op && read_and_delete_) {
        tc_[tc_num].read_and_delete_result(ir_info_[idx]);
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
  //runner_.add_ = false;
  //runner_.read_ = false;
  //runner_.read_and_delete_ = false;
  runner_.start();
  while(true) {
    sleep(30);
    runner_.read_ = false;
    runner_.read_and_delete_ = false;
    runner_.add_ = false;
    while(true) {
      sleep(30);
      runner_.add_ = true;
      sleep(30);
      runner_.read_ = true;
      sleep(30);
      runner_.read_and_delete_ = true;
      sleep(30);
      runner_.add_ = false;
      runner_.read_ = false;
      runner_.read_and_delete_ = false;
    }
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
