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

#include "storage/tx/ob_trans_memory_stat.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{
class TestObTransMemStat : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static const char *LOCAL_IP;
  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;

  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
};
const char *TestObTransMemStat::LOCAL_IP = "127.0.0.1";

TEST_F(TestObTransMemStat, init_reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // create an obejct of ObTtransID
  const char *buffer = "ObTransCtx";
  const int64_t alloc_count = 100;
  const int64_t release_count = 100;
  ObAddr observer(TestObTransMemStat::IP_TYPE, TestObTransMemStat::LOCAL_IP, TestObTransMemStat::PORT);
  ObTransMemoryStat mem_stat;
  EXPECT_EQ(OB_SUCCESS, mem_stat.init(observer, buffer, alloc_count, release_count));
  ObTransMemoryStat mem_stat1(mem_stat);

  // test the correctness of copy assignment
  EXPECT_EQ(mem_stat.get_addr(), mem_stat1.get_addr());
  EXPECT_EQ(mem_stat.get_alloc_count(), mem_stat1.get_alloc_count());
  EXPECT_EQ(mem_stat.get_release_count(), mem_stat1.get_release_count());
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_mem_stat.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
