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

#include "storage/transaction/ob_trans_stat.h"
#include "storage/transaction/ob_trans_define.h"
#include "common/ob_clock_generator.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace unittest {
class TestObTransStat : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  static const char* LOCAL_IP;
  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;

  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
};
const char* TestObTransStat::LOCAL_IP = "127.0.0.1";

TEST_F(TestObTransStat, init_reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObAddr observer(TestObTransStat::IP_TYPE, TestObTransStat::LOCAL_IP, TestObTransStat::PORT);
  ObTransID trans_id(observer);
  const int64_t tenant_id = 1000;
  const bool is_exiting = false;
  const bool is_readonly = false;
  const bool has_decided = false;
  const bool is_dirty = false;
  ObPartitionKey coordinator(
      TestObTransStat::VALID_TABLE_ID, TestObTransStat::VALID_PARTITION_ID, TestObTransStat::VALID_PARTITION_COUNT);
  ObPartitionArray participants;
  EXPECT_EQ(OB_SUCCESS, participants.push_back(coordinator));

  // create an object of ObStartTransParm
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  const int64_t expired_time = 1000;
  const int64_t ctx_create_time = ObClockGenerator::getClock();
  const int64_t sql_no = 2;
  const int64_t refer = 2;
  const int64_t state = Ob2PCState::PREPARE;
  const uint32_t session_id = 1000;
  const uint64_t proxy_session_id = 1000;
  const int trans_type = 1;
  const int64_t part_trans_action = 1;
  const int64_t lock_for_read_retry_count = 0;
  const ObElrTransInfoArray trans_arr;
  const int64_t flushed_log_size = 0;
  const int64_t pending_log_size = 0;

  ObTransStat trans_stat;
  EXPECT_EQ(OB_SUCCESS,
      trans_stat.init(observer,
          trans_id,
          tenant_id,
          is_exiting,
          is_readonly,
          has_decided,
          is_dirty,
          coordinator,
          participants,
          parms,
          ctx_create_time,
          expired_time,
          refer,
          sql_no,
          state,
          session_id,
          proxy_session_id,
          trans_type,
          part_trans_action,
          lock_for_read_retry_count,
          0,
          trans_arr,
          trans_arr,
          0,
          1,
          pending_log_size,
          flushed_log_size));

  trans_stat.reset();
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_stat.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
