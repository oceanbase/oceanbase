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
#include "lib/oblog/ob_easy_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/profile/ob_profile_fill_log.h"
#include "test_profile_utils.h"
namespace oceanbase
{
namespace common
{
class TestProfileFillLog : public ::testing::Test
  {
  public:
    TestProfileFillLog() {}
    virtual ~TestProfileFillLog() {}
    virtual void SetUp() {}
    virtual void TearDown() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(TestProfileFillLog);
  };

TEST(TestProfileFillLog, printlog)
{
  ObProfileFillLog::setLogDir("./");
  ObProfileFillLog::setFileName("test_ob_profile_fill_log.log");
  EXPECT_EQ(OB_SUCCESS, ObProfileFillLog::init());
  ObProfileFillLog::open();
  uint64_t trace_id[2] = {998, 999};
  uint64_t pcode = 4088;
  uint64_t wait_sql_queue_time = 10000000;
  int sql_queue_size = 10000;
  char sql_str[128] = "select * from test";
  int64_t channel_id = 1;
  PFILL_SET_TRACE_ID(trace_id);
  PFILL_SET_PCODE(pcode);
  PFILL_SET_QUEUE_SIZE(sql_queue_size);
  PFILL_SET_WAIT_SQL_QUEUE_TIME(wait_sql_queue_time);
  PFILL_SET_SQL(sql_str, static_cast<int>(strlen(sql_str)));
  PFILL_RPC_START(pcode, channel_id);
  PFILL_RPC_END(channel_id);
  PFILL_ITEM_START(logicalplan_to_physicalplan);
  PFILL_ITEM_END(logicalplan_to_physicalplan);
  PFILL_PRINT();
  PFILL_CLEAR_LOG();
  ObProfileFillLog::close();
  pcode = 4077;
  wait_sql_queue_time = 10000000;
  sql_queue_size = 10000;
  char sql_str2[128] = "insert into test values(1,2);";
  channel_id = 2;
  ObProfileFillLog::open();
  PFILL_SET_TRACE_ID(trace_id);
  PFILL_SET_PCODE(pcode);
  PFILL_SET_QUEUE_SIZE(sql_queue_size);
  PFILL_SET_WAIT_SQL_QUEUE_TIME(wait_sql_queue_time);
  PFILL_SET_SQL(sql_str2, static_cast<int>(strlen(sql_str2)));
  PFILL_RPC_START(pcode, channel_id);
  PFILL_RPC_END(channel_id);
  PFILL_ITEM_START(logicalplan_to_physicalplan);
  PFILL_ITEM_END(logicalplan_to_physicalplan);
  PFILL_PRINT();
  PFILL_CLEAR_LOG();
  ObProfileFillLog::close();
}
} //end common
} //end oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
