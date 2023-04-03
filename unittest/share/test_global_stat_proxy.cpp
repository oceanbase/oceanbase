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

#include "share/ob_global_stat_proxy.h"

#include <gtest/gtest.h>
#include "schema/db_initializer.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

class TestGlobalStatProxy : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  schema::DBInitializer initer_;
};

void TestGlobalStatProxy::SetUp()
{
  int ret = initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = initer_.create_system_table(true);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestGlobalStatProxy, common)
{
  ObGlobalStatProxy global_proxy(initer_.get_sql_proxy());
  int64_t core_schema_version = -1;
  int64_t try_frozen_version = -1;
  int64_t frozen_version = -1;
  ASSERT_EQ(OB_SUCCESS, global_proxy.set_init_value(2, 2, 2, 2));
  ASSERT_EQ(OB_SUCCESS, global_proxy.get_core_schema_version(-1, core_schema_version));
  ASSERT_EQ(2, core_schema_version);
  ASSERT_EQ(OB_SUCCESS, global_proxy.get_frozen_info(try_frozen_version, frozen_version));
  ASSERT_EQ(2, try_frozen_version);
  ASSERT_EQ(2, frozen_version);

  ASSERT_EQ(OB_SUCCESS, global_proxy.set_core_schema_version(3));
  ASSERT_EQ(OB_SUCCESS, global_proxy.get_core_schema_version(-1, core_schema_version));
  ASSERT_EQ(3, core_schema_version);

  ASSERT_EQ(OB_SUCCESS, global_proxy.set_try_frozen_version(4));
  ASSERT_EQ(OB_SUCCESS, global_proxy.set_frozen_version(3));

  ASSERT_EQ(OB_SUCCESS, global_proxy.get_frozen_info(try_frozen_version, frozen_version));
  ASSERT_EQ(4, try_frozen_version);
  ASSERT_EQ(3, frozen_version);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
