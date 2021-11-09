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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "gtest/gtest.h"

#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/transaction/ob_trans_log.h"
#include "ob_log_fetch_stat_info.h"

#define private public
#include "liboblog/src/ob_log_part_trans_resolver.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace transaction;
using namespace storage;
using namespace clog;

namespace oceanbase
{
namespace unittest
{

void call_sort_and_unique_missing_log_ids(IObLogPartTransResolver::ObLogMissingInfo &missing_info)
{
  LOG_INFO("MISSING LOG [BEGIN]", K(missing_info));
  EXPECT_EQ(OB_SUCCESS, missing_info.sort_and_unique_missing_log_ids());
  LOG_INFO("MISSING LOG [END]", K(missing_info));
}

TEST(ObLogPartTransResolver, Function1)
{
  int ret = OB_SUCCESS;
  IObLogPartTransResolver::ObLogMissingInfo missing_info;
  ObLogIdArray &missing_log_id = missing_info.missing_log_ids_;

  // 1. one miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_missing_log_count());

  // 2. two miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_missing_log_count());

  // 3. repeatable miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_missing_log_count());

  // 4. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(2, missing_info.get_missing_log_count());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }

  // 5. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(2, missing_info.get_missing_log_count());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }

  // 6. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(2, missing_info.get_missing_log_count());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }


  // 7. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(3));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(4, missing_info.get_missing_log_count());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }

  // 8. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(3));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(3));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(3));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(3));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(3));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(4, missing_info.get_missing_log_count());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }

  // 9. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(3));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_id(4));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(4, missing_info.get_missing_log_count());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }
}

}
}

int main(int argc, char **argv)
{
  //ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_part_trans_resolver.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
