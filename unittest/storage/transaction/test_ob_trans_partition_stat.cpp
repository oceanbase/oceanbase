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

#include "storage/transaction/ob_trans_partition_stat.h"
#include "storage/transaction/ob_trans_ctx.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace unittest {
class TestObTransPartitionStat : public ::testing::Test {
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
const char* TestObTransPartitionStat::LOCAL_IP = "127.0.0.1";

TEST_F(TestObTransPartitionStat, init_reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObAddr observer(
      TestObTransPartitionStat::IP_TYPE, TestObTransPartitionStat::LOCAL_IP, TestObTransPartitionStat::PORT);
  ObPartitionKey partition(TestObTransPartitionStat::VALID_TABLE_ID,
      TestObTransPartitionStat::VALID_PARTITION_ID,
      TestObTransPartitionStat::VALID_PARTITION_COUNT);
  int64_t ctx_type = ObTransCtxType::PARTICIPANT;
  const bool is_master = true;
  const bool is_frozen = true;
  const bool is_stopped = false;
  const int64_t read_only_count = 100;
  const int64_t active_read_write_count = 100;

  ObTransPartitionStat part_stat;
  EXPECT_EQ(OB_SUCCESS,
      part_stat.init(observer,
          partition,
          ctx_type,
          is_master,
          is_frozen,
          is_stopped,
          read_only_count,
          active_read_write_count,
          0,
          0,
          0,
          0,
          0,
          0,
          0));
  part_stat.reset();
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_partition_stat.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
