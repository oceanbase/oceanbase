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

#define USING_LOG_PREFIX SHARE
#define FREEZE_INFO_UNITTEST_DEF

#include "share/ob_freeze_info_proxy.h"
#include "common/storage/ob_freeze_define.h"

#include <gtest/gtest.h>
#include "schema/db_initializer.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace storage;
using namespace share::schema;

class TestFreezeInfo : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  schema::DBInitializer initer_;
  ObFreezeInfoCache freeze_info_cache_;
};

void TestFreezeInfo::SetUp()
{
  int ret = initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = initer_.create_system_table(true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = freeze_info_cache_.init(&initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestFreezeInfo, common)
{
  ObFreezeInfoProxy freeze_info_proxy;
  ASSERT_EQ(OB_SUCCESS, freeze_info_proxy.set_init_value());
  int64_t try_frozen_version = 2;
  const int64_t FROZEN_VERSION_UPPER_LIMIT = 128;
  while (try_frozen_version <= FROZEN_VERSION_UPPER_LIMIT) {
    ObFrozenStatus empty_frozen_status;
    ObFrozenStatus prepare_frozen_status;
    ObFrozenStatus commit_frozen_status;
    // prepare frozen status
    prepare_frozen_status.frozen_version_ = try_frozen_version;
    prepare_frozen_status.status_ = PREPARED_SUCCEED;
    prepare_frozen_status.frozen_timestamp_ = ObTimeUtility::current_time();
    prepare_frozen_status.schema_version_ = try_frozen_version;
    // commit frozen status
    commit_frozen_status.frozen_version_ = try_frozen_version;
    commit_frozen_status.status_ = COMMIT_SUCCEED;
    commit_frozen_status.frozen_timestamp_ = prepare_frozen_status.frozen_timestamp_;
    commit_frozen_status.schema_version_ = try_frozen_version;
    ASSERT_EQ(OB_SUCCESS, freeze_info_proxy.set_freeze_info(
                              empty_frozen_status,
                              prepare_frozen_status));
    ASSERT_EQ(OB_SUCCESS, freeze_info_proxy.set_freeze_info(
                              prepare_frozen_status,
                              commit_frozen_status));
    ++try_frozen_version;
  }
  while (try_frozen_version < 2 * FROZEN_VERSION_UPPER_LIMIT) {
    ObFrozenStatus empty_frozen_status;
    ObFrozenStatus prepare_frozen_status;
    // prepare frozen status
    prepare_frozen_status.frozen_version_ = try_frozen_version;
    prepare_frozen_status.status_ = PREPARED_SUCCEED;
    prepare_frozen_status.frozen_timestamp_ = ObTimeUtility::current_time();
    prepare_frozen_status.schema_version_ = try_frozen_version;
    ASSERT_EQ(OB_SUCCESS, freeze_info_proxy.set_freeze_info(
                              empty_frozen_status,
                              prepare_frozen_status));
    ++try_frozen_version;
  }
  freeze_info_cache_.print_all_freeze_info();
  for (int64_t try_frozen_version_x = 2;
       try_frozen_version_x < 2 * FROZEN_VERSION_UPPER_LIMIT;
       ++try_frozen_version_x) {
    ObFreezeStatus freeze_status = FREEZE_STATUS_MAX;
    int64_t frozen_timestamp = -1;
    int64_t schema_version = -1;
    ASSERT_EQ(OB_SUCCESS, freeze_info_cache_.get_freeze_info(
                              try_frozen_version_x,
                              freeze_status,
                              frozen_timestamp,
                              schema_version));
    LOG_INFO("get freeze info", K(try_frozen_version_x),
                                K(freeze_status),
                                K(frozen_timestamp),
                                K(schema_version));
  }
  freeze_info_cache_.print_all_freeze_info();
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_freeze_info_proxy.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_freeze_info_proxy.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
