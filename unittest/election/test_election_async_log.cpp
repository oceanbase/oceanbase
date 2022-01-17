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
#include "common/ob_clock_generator.h"
#include "election/ob_election_mgr.h"
#include "election/ob_election_msg.h"
#include "election/ob_election_async_log.h"

namespace oceanbase {
namespace unittest {
using namespace election;
using namespace common;

class TestElectionAsyncLog : public ::testing::Test {
public:
  TestElectionAsyncLog()
  {}
  virtual ~TestElectionAsyncLog()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

TEST_F(TestElectionAsyncLog, logfile_open_close)
{
  ObLogFile log_file;
  const char* file_name = "election.log";
  const char* file_name1 = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_file.open(NULL));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_file.open(file_name1));
  EXPECT_EQ(OB_SUCCESS, log_file.open(file_name));
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_file.open(file_name));
  EXPECT_EQ(OB_SUCCESS, log_file.close());
  EXPECT_EQ(OB_SUCCESS, log_file.close());
}

TEST_F(TestElectionAsyncLog, logfile_write)
{
  ObLogFile log_file;
  char buf[election::ObLogItem::MAX_LOG_SIZE];

  const char* file_name = "test_async_log_file.txt";
  system("rm -f test_async_log_file.txt");
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_file.write(NULL, 10));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_file.write(buf, 0));
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_file.write(buf, election::ObLogItem::MAX_LOG_SIZE));
  EXPECT_EQ(OB_SUCCESS, log_file.open(file_name));
  EXPECT_EQ(OB_SUCCESS, log_file.write(buf, election::ObLogItem::MAX_LOG_SIZE));
  EXPECT_EQ(OB_SUCCESS, log_file.close());
  system("rm -f test_async_log_file.txt");
}

TEST_F(TestElectionAsyncLog, logfile_set_max_file_size)
{
  ObLogFile log_file;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_file.set_max_file_size(0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_file.set_max_file_size(-10));
  EXPECT_EQ(OB_SUCCESS, log_file.set_max_file_size(10));
}

TEST_F(TestElectionAsyncLog, init)
{
  EXPECT_EQ(OB_INVALID_ARGUMENT, ASYNC_LOG_INIT(NULL, OB_LOG_LEVEL_INFO, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_DEBUG + 1, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_ERROR - 1, true));
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  EXPECT_EQ(OB_INIT_TWICE, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  EXPECT_EQ(OB_SUCCESS, ObAsyncLog::getLogger().set_max_file_size(256 * 1024));
  ASYNC_LOG_DESTROY();
  system("rm -f election.log");
  system("rm -f election.log.wf");
}

TEST_F(TestElectionAsyncLog, set_log_level)
{
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, false));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObAsyncLog::getLogger().set_log_level(OB_LOG_LEVEL_NONE));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObAsyncLog::getLogger().set_log_level(OB_LOG_LEVEL_NP));
  EXPECT_EQ(OB_SUCCESS, ObAsyncLog::getLogger().set_log_level(OB_LOG_LEVEL_INFO));
  ASYNC_LOG_DESTROY();
  system("rm -f election.log");
}

TEST_F(TestElectionAsyncLog, oblogitem_alloc_freeze)
{
  election::ObLogItem* item = election::ObLogItemFactory::alloc();
  EXPECT_EQ(1, election::ObLogItemFactory::alloc_count_);
  EXPECT_EQ(0, item->get_size());
  EXPECT_EQ(OB_LOG_LEVEL_NONE, item->get_log_level());
  election::ObLogItemFactory::release(NULL);
  EXPECT_EQ(0, election::ObLogItemFactory::release_count_);
  election::ObLogItemFactory::release(item);
  EXPECT_EQ(1, election::ObLogItemFactory::release_count_);
}

TEST_F(TestElectionAsyncLog, async_log_message_kv)
{
  ObAsyncLog::getLogger().async_log_message_kv(NULL, OB_LOG_LEVEL_NONE, NULL, NULL, 10, NULL, NULL);
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  ObAsyncLog::getLogger().async_log_message_kv(NULL, OB_LOG_LEVEL_NONE, NULL, NULL, 10, NULL, NULL);
  ObAsyncLog::getLogger().async_log_message_kv(NULL, OB_LOG_LEVEL_INFO, NULL, NULL, 10, NULL, NULL);

  ASYNC_LOG_DESTROY();
  system("rm -f election.log");
}

TEST_F(TestElectionAsyncLog, write)
{
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  ELECT_ASYNC_LOG(INFO, "test async log", "count", 4);
  ELECT_ASYNC_LOG(WARN, "test async warn log", "count", 4);
  const char* str = "xxx.xx";
  const int64_t year = 2015;
  const double d = 12.344;
  const char c = 'a';
  ELECT_ASYNC_LOG(WARN, "test async warn log", K(str), K(year), K(d), K(c));
  ASYNC_LOG_DESTROY();
  system("rm -f election.log");
  system("rm -f election.log.wf");
}

TEST_F(TestElectionAsyncLog, write_kv)
{
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  int ret = OB_SUCCESS;
  const char* str = "xxx.xx";
  const int64_t year = 2015;
  const double d = 12.344;
  const char c = 'a';
  ELECT_ASYNC_LOG(INFO, "test async kv log");
  ELECT_ASYNC_LOG(INFO, "test async kv log", K(ret));
  ELECT_ASYNC_LOG(WARN, "test async kv log", K(ret), K(str), K(year), K(d), K(c));
  ELECT_ASYNC_LOG(INFO, "test async kv log", K(ret));
  ASYNC_LOG_DESTROY();
  system("rm -f election.log");
  system("rm -f election.log.wf");
}

TEST_F(TestElectionAsyncLog, more_data)
{
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  char data[1024];
  ELECT_ASYNC_LOG(INFO, "test async more data", K(data), K(data), K(data), K(data), K(data), K(data), K(data), K(data));
  ASYNC_LOG_DESTROY();
  system("rm -f election.log");
  system("rm -f election.log.wf");
}

TEST_F(TestElectionAsyncLog, more_kv_log)
{
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  const char* str = "wo shi sunkang";
  const int64_t year = 2015334124;
  const double d = 12.3443498934;
  const char c = 'a';
  for (int i = 0; i < 1000; i++) {
    ELECT_ASYNC_LOG(INFO, "test async kv log");
    ELECT_ASYNC_LOG(INFO, "test async kv log", K(i));
    ELECT_ASYNC_LOG(WARN, "test async kv log", K(i), K(str), K(year), K(d), K(c));
  }
  ASYNC_LOG_DESTROY();
  system("rm -f election.log*");
  system("rm -f election.log.wf*");
}

TEST_F(TestElectionAsyncLog, complicated_kv_log)
{
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  const int64_t count = 1000;
  int64_t t1 = ::oceanbase::common::ObTimeUtility::current_time();
  int64_t cur_ts = t1 + 10;
  common::ObAddr self;
  EXPECT_EQ(true, self.set_ip_addr("127.0.0.1", 34506));

  ObElectionPriority p0;
  p0.init(true, 1000, 100, 0);
  ObElectionMsgDEPrepare msg(p0, t1, cur_ts, self, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);

  for (int i = 0; i < count; i++) {
    ELECT_ASYNC_LOG(INFO, "test async kv log", K(i), K(msg));
  }
  ASYNC_LOG_DESTROY();
  system("rm -f election.log*");
  system("rm -f election.log.wf*");
}

TEST_F(TestElectionAsyncLog, benchmark)
{
  EXPECT_EQ(OB_SUCCESS, ASYNC_LOG_INIT("election.log", OB_LOG_LEVEL_INFO, true));
  ObAsyncLog::getLogger().set_max_file_size(256 * 1024 * 1024);

  const int64_t async_log_num = 10000;
  ObElection* election = op_alloc(ObElection);
  EXPECT_TRUE(NULL != election);
  EXPECT_TRUE(0 < async_log_num);

  const int64_t start_time = ObClockGenerator::getClock();
  for (int64_t i = 0; i < async_log_num; ++i) {
    ELECT_ASYNC_LOG(INFO, "test async kv log", "election", *election);
    // ELECT_LOG(INFO, "test async kv log", "election", *election);
  }
  const int64_t end_time = ObClockGenerator::getClock() - start_time;
  ELECT_LOG(INFO, "election async benchmark", "total time", end_time, "time per log", (end_time / async_log_num));
  EXPECT_TRUE(NULL != election);
  op_free(election);
  ASYNC_LOG_DESTROY();
  // system("rm -f election.log*");
  // system("rm -f election.log.wf*");
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::common::ObLogger& logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_election_async_log.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  if (OB_FAIL(oceanbase::common::ObClockGenerator::init())) {
    ELECT_LOG(WARN, "clock generator init error.", K(ret));
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  (void)oceanbase::common::ObClockGenerator::destroy();

  return ret;
}
