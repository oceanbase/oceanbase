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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include <gtest/gtest.h>
#include <iostream>
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_iter.h"
#include "mock_schema_service.h"
#include "schema_test_utils.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {
class TestTableIterator : public ::testing::Test {
public:
  virtual void SetUp();
  virtual void TearDown()
  {}
};

void TestTableIterator::SetUp()
{}

TEST_F(TestTableIterator, init)
{
  int ret = OB_SUCCESS;
  MockSchemaService schema_service;
  ObSchemaGetterGuard schema_guard;
  ret = schema_service.get_schema_guard(schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  // schema_service.init();
  // ObTenantSchema tenant_schema;
  // GEN_TENANT_SCHEMA(tenant_schema, 1, "sys", 0);
  // schema_service.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
  uint64_t table_id = OB_INVALID_ID;
  ObTableIterator table_iter;
  ret = table_iter.next(table_id);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = table_iter.init(NULL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_iter.init(&schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_iter.init(&schema_guard);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  ret = table_iter.next(table_id);
  ASSERT_EQ(OB_ITER_END, ret);
};

TEST_F(TestTableIterator, next)
{
  int ret = OB_SUCCESS;

  MockSchemaService schema_service;
  schema_service.init();
  ObSchemaGetterGuard schema_guard;
  ret = schema_service.get_schema_guard(schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantSchema tenant_schema;
  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys", 0);
  tenant_schema.set_locality("");
  tenant_schema.add_zone("zone");
  schema_service.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
  ObSysVariableSchema sys_variable;
  sys_variable.set_tenant_id(tenant_schema.get_tenant_id());
  sys_variable.set_schema_version(tenant_schema.get_schema_version());
  sys_variable.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
  ASSERT_EQ(OB_SUCCESS, schema_service.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version()));

  char table_name[20];
  for (int i = 0; i < 1025; ++i) {
    ObTableSchema table_schema;
    snprintf(table_name, 20, "table_%d", i + 1);
    GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, i + 1), table_name, USER_TABLE, 0);
    schema_service.add_table_schema(table_schema, table_schema.get_schema_version());
  }

  ObTableIterator table_iter;
  ret = table_iter.init(&schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  uint64_t table_id = OB_INVALID_ID;
  while (ret != OB_ITER_END) {
    ret = table_iter.next(table_id);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ITER_END == ret);
  }
  // to_string
  LOG_INFO("test ob_table_iter to_string", K(table_iter));
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_iter.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
