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
#include <unistd.h>
#include "lib/container/ob_bit_set.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/ob_errno.h"
#include "lib/thread/thread_pool.h"

using namespace oceanbase;
using namespace common;

namespace test{
class ObLogTestThread: public lib::ThreadPool
{
public:
  static const int64_t NUM_OF_LOG = 500;
  void run1() final;
};

class ObLogTestThreadT: public lib::ThreadPool
{
public:
  static const int64_t NUM_OF_LOG = 500;
  void run1() final;
};
class ObLoggerTest: public ::testing::Test
{
public:
  static const int64_t BIG_LOG_FILE_SIZE = 256 * 1024 * 1024;
  static const int64_t S_LOG_FILE_SIZE = 16 * 1024;
  ObLoggerTest();
  void run_test();
  void run_test_async();
  void run_test_async_multi();
  void run_test_t();
  void run_test_t_async();
  void run_test_t_async_multi();
protected:
  ObLogTestThread thread_pool_;
  ObLogTestThreadT thread_pool_t_;
};

ObLoggerTest::ObLoggerTest()
{

}

void ObLogTestThread::run1()
{
  int ret = 0;
  OB_LOG(ERROR, "error log would print lbt");
  SET_OB_LOG_TRACE_MODE();
  PRINT_OB_LOG_TRACE_BUF(COMMON, INFO);
  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  ObString str("Our destiny offers not the cup of despair, but the chalice of opportunity. So let us seize it, not in fear, but in gladness.");
  for (int64_t i = 0; i < NUM_OF_LOG; i++) {
    OB_LOG(WARN, "oblog test", K(i), K(str));
    _OB_LOG(WARN, "oblog test %ld", i);
    OB_LOG(ERROR, "trace error log also would print lbt");
  }
  PRINT_OB_LOG_TRACE_BUF(FLT, INFO);
  CANCLE_OB_LOG_TRACE_MODE();
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "yangze one thread time","u_time", e_time - b_time);

}

void ObLogTestThreadT::run1()
{
  int ret = 0;
  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  {
    ObString str("Our destiny offers not the cup of despair, but the chalice of opportunity. So let us seize it, not in fear, but in gladness.");
    //LOG_USER_ERROR(OB_NOT_SUPPORTED, "Just test trace mode user msg");
    for (int64_t i = 0; i < NUM_OF_LOG; i++) {
      OB_LOG(WARN, "oblog test", K(i), K(str));
    }
    //LOG_USER_ERROR(OB_NOT_SUPPORTED, "Just test trace mode user msg the msg");
  }
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "yangze one thread time", "u_time", e_time - b_time);
}

void ObLoggerTest::run_test()
{
  int ret = 0;
  system("rm -rf s_log.log*");
  OB_LOGGER.set_log_level("TRACE", "WARN");
  OB_LOGGER.set_enable_async_log(false);
  //OB_LOGGER.set_max_file_size(BIG_LOG_FILE_SIZE);
  OB_LOGGER.set_file_name("s_log.log", false, true);
  // OB_LOGGER.set_max_file_size(64 << 20);
  thread_pool_.set_thread_count(20);

  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  thread_pool_.start();
  thread_pool_.wait();
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "yangze all thread time", "u_time", e_time - b_time);
}

void ObLoggerTest::run_test_t()
{
  int ret = 0;
  system("rm -rf t_log.log*");
  OB_LOGGER.set_log_level("TRACE", "WARN");
  OB_LOGGER.set_enable_async_log(false);
  //OB_LOGGER.set_max_file_size(BIG_LOG_FILE_SIZE);
  OB_LOGGER.set_file_name("t_log.log", false, true);
  thread_pool_t_.set_thread_count(20);

  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  thread_pool_t_.start();
  thread_pool_t_.wait();
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "T:yangze all thread time", "u_time", e_time - b_time);
}

void ObLoggerTest::run_test_async()
{
  int ret = 0;
  system("rm -rf async_log.log*");
  OB_LOGGER.set_log_level("TRACE", "WARN");
  //OB_LOGGER.set_max_file_size(BIG_LOG_FILE_SIZE);
  OB_LOGGER.set_file_name("async_log.log", true, true);
  OB_LOGGER.set_enable_async_log(true);
  OB_LOGGER.set_use_multi_flush(false);
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  // OB_LOGGER.set_max_file_size(64 << 20);
  thread_pool_.set_thread_count(20);

  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  thread_pool_.start();
  thread_pool_.wait();
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "jianhua all thread time", "u_time", e_time - b_time);
}

void ObLoggerTest::run_test_async_multi()
{
  int ret = 0;
  system("rm -rf async_multi_log.log*");
  OB_LOGGER.set_log_level("TRACE", "WARN");
  //OB_LOGGER.set_max_file_size(BIG_LOG_FILE_SIZE);
  OB_LOGGER.set_file_name("async_multi_log.log", false, true);
  OB_LOGGER.set_enable_async_log(true);
  OB_LOGGER.set_use_multi_flush(true);
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);

  // OB_LOGGER.set_max_file_size(64 << 20);
  thread_pool_.set_thread_count(20);

  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  thread_pool_.start();
  thread_pool_.wait();
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "jianhua all thread time", "u_time", e_time - b_time);
}

void ObLoggerTest::run_test_t_async()
{
  int ret = 0;
  system("rm -rf t_async_log.log*");
  OB_LOGGER.set_log_level("TRACE", "WARN");
  OB_LOGGER.set_enable_async_log(true);
  OB_LOGGER.set_use_multi_flush(false);
  //OB_LOGGER.set_max_file_size(BIG_LOG_FILE_SIZE);
  OB_LOGGER.set_file_name("t_async_log.log", false, true);
  thread_pool_t_.set_thread_count(20);

  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  thread_pool_t_.start();
  thread_pool_t_.wait();
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "T:yangze all thread time", "u_time", e_time - b_time);
}

void ObLoggerTest::run_test_t_async_multi()
{
  int ret = 0;
  system("rm -rf t_async_multi_log.log*");
  OB_LOGGER.set_log_level("TRACE", "WARN");
  OB_LOGGER.set_enable_async_log(true);
  OB_LOGGER.set_use_multi_flush(true);
  //OB_LOGGER.set_max_file_size(BIG_LOG_FILE_SIZE);
  OB_LOGGER.set_file_name("t_async_multi_log.log", false, true);
  thread_pool_t_.set_thread_count(20);

  int64_t b_time = ::oceanbase::common::ObTimeUtility::current_time();
  thread_pool_t_.start();
  thread_pool_t_.wait();
  int64_t e_time = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOG(WARN, "T:yangze all thread time", "u_time", e_time - b_time);
}


TEST_F(ObLoggerTest, DISABLED_performance_test)
{
  run_test();
}

TEST_F(ObLoggerTest, DISABLED_performance_test_async)
{
  run_test_async();
}

TEST_F(ObLoggerTest, DISABLED_performance_test_async_multi)
{
  run_test_async_multi();
}

TEST_F(ObLoggerTest, DISABLED_trace_test)
{
  run_test_t();
}

TEST_F(ObLoggerTest, DISABLED_trace_test_async)
{
  run_test_t_async();
}

TEST_F(ObLoggerTest, DISABLED_trace_test_async_multi)
{
  run_test_t_async_multi();
}


}
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
