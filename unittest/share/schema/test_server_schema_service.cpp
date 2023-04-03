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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "share/schema/ob_server_schema_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;
class TestServerSchemaService : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}
};

void TestServerSchemaService::SetUp()
{
}

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

TEST_F(TestServerSchemaService, AllSchemaKey)
{
  int ret = OB_SUCCESS;

  ObServerSchemaService::AllSchemaKeys all_keys;
  ret = all_keys.create(50);
  ASSERT_EQ(OB_SUCCESS, ret);
  SchemaKey key;
  ret = all_keys.new_tenant_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.alter_tenant_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_tenant_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.new_user_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_user_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.new_database_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_database_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.new_tablegroup_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_tablegroup_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.new_table_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_table_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.new_outline_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_outline_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.new_db_priv_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_db_priv_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.new_table_priv_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = all_keys.del_table_priv_keys_.set_refactored(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(1, all_keys.new_tenant_keys_.size());
  ASSERT_EQ(1, all_keys.alter_tenant_keys_.size());
  ASSERT_EQ(1, all_keys.new_user_keys_.size());
  ASSERT_EQ(1, all_keys.new_database_keys_.size());
  ASSERT_EQ(1, all_keys.new_tablegroup_keys_.size());
  ASSERT_EQ(1, all_keys.new_table_keys_.size());
  ASSERT_EQ(1, all_keys.new_outline_keys_.size());
  ASSERT_EQ(1, all_keys.new_db_priv_keys_.size());
  ASSERT_EQ(1, all_keys.new_table_priv_keys_.size());
  ASSERT_EQ(1, all_keys.del_tenant_keys_.size());
  ASSERT_EQ(1, all_keys.del_user_keys_.size());
  ASSERT_EQ(1, all_keys.del_database_keys_.size());
  ASSERT_EQ(1, all_keys.del_tablegroup_keys_.size());
  ASSERT_EQ(1, all_keys.del_table_keys_.size());
  ASSERT_EQ(1, all_keys.del_outline_keys_.size());
  ASSERT_EQ(1, all_keys.del_db_priv_keys_.size());
  ASSERT_EQ(1, all_keys.del_table_priv_keys_.size());

  all_keys.reset();
  ASSERT_EQ(0, all_keys.new_tenant_keys_.size());
  ASSERT_EQ(0, all_keys.alter_tenant_keys_.size());
  ASSERT_EQ(0, all_keys.new_user_keys_.size());
  ASSERT_EQ(0, all_keys.new_database_keys_.size());
  ASSERT_EQ(0, all_keys.new_tablegroup_keys_.size());
  ASSERT_EQ(0, all_keys.new_table_keys_.size());
  ASSERT_EQ(0, all_keys.new_outline_keys_.size());
  ASSERT_EQ(0, all_keys.new_db_priv_keys_.size());
  ASSERT_EQ(0, all_keys.new_table_priv_keys_.size());
  ASSERT_EQ(0, all_keys.del_tenant_keys_.size());
  ASSERT_EQ(0, all_keys.del_user_keys_.size());
  ASSERT_EQ(0, all_keys.del_database_keys_.size());
  ASSERT_EQ(0, all_keys.del_tablegroup_keys_.size());
  ASSERT_EQ(0, all_keys.del_table_keys_.size());
  ASSERT_EQ(0, all_keys.del_outline_keys_.size());
  ASSERT_EQ(0, all_keys.del_db_priv_keys_.size());
  ASSERT_EQ(0, all_keys.del_table_priv_keys_.size());
}

TEST_F(TestServerSchemaService, table_priv_equal_to)
{
  SchemaKey key1;
  key1.tenant_id_ = 1;
  key1.user_id_ = 1;
  key1.database_name_ = "db";
  key1.table_name_ = "table";
  SchemaKey key2 = key1;
  ObServerSchemaService::table_priv_equal_to table_priv_equal_to;
  ASSERT_TRUE(table_priv_equal_to(key1, key2));
}

}//end namespace schema
}//end namespace share
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
