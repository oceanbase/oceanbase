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
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/random/ob_random.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/allocator/page_arena.h"
#define private public
#define protected public
#include "share/schema/ob_schema_utils.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "schema_test_utils.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {

class TestSchemaUtils : public ::testing::Test {
public:
  virtual void SetUp();

private:
};

void TestSchemaUtils::SetUp()
{}

TEST_F(TestSchemaUtils, UNDER_SYS_TENANT)
{
  ASSERT_TRUE(UNDER_SYS_TENANT(combine_id(1, 1)));
  ASSERT_FALSE(UNDER_SYS_TENANT(combine_id(2, 1)));
}

TEST_F(TestSchemaUtils, IS_TENANT_SPACE)
{
  bool res = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    uint64_t tid = tenant_space_tables[i];
    ASSERT_TRUE(is_tenant_table(combine_id(1, tid)));
  }
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    uint64_t tid = tenant_space_tables[i];
    ASSERT_TRUE(is_tenant_table(combine_id(2, tid)));
  }

  res = is_tenant_table(combine_id(1, 1));
  ASSERT_TRUE(!res);
  res = is_tenant_table(combine_id(1, 100));
  ASSERT_TRUE(!res);
  res = is_tenant_table(combine_id(1, 50001));
  ASSERT_TRUE(!res);
}

TEST_F(TestSchemaUtils, alloc_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  table_schema.set_tenant_id(1);
  table_schema.set_table_id(combine_id(1, 1));
  table_schema.set_database_id(combine_id(1, 1));
  table_schema.set_table_name("table_name");
  ObArenaAllocator allocator;
  ObTableSchema* new_table_schema = NULL;
  ret = ObSchemaUtils::alloc_schema(allocator, table_schema, new_table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != new_table_schema);
  ASSERT_TRUE(SchemaTestUtils::equal_table_schema(table_schema, *new_table_schema));
  ASSERT_EQ(new_table_schema->allocator_, &allocator);
}

TEST_F(TestSchemaUtils, alloc_new_var)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  table_schema.set_tenant_id(1);
  table_schema.set_table_id(combine_id(1, 1));
  table_schema.set_database_id(combine_id(1, 1));
  table_schema.set_table_name("fsdfds");
  ObArenaAllocator allocator;
  ObTableSchema* new_table_schema = NULL;
  ret = ObSchemaUtils::alloc_new_var(allocator, table_schema, new_table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != new_table_schema);
  ASSERT_TRUE(SchemaTestUtils::equal_table_schema(table_schema, *new_table_schema));
  ASSERT_NE(new_table_schema->allocator_, &allocator);
}

TEST_F(TestSchemaUtils, deep_copy_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  table_schema.set_tenant_id(1);
  table_schema.set_table_id(combine_id(1, 1));
  table_schema.set_database_id(combine_id(1, 1));
  table_schema.set_table_name("fsdfds");
  ObTableSchema* new_table_schema = NULL;
  ret = ObSchemaUtils::deep_copy_schema(NULL, table_schema, new_table_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  char* buf = new char[table_schema.get_convert_size() + sizeof(ObDataBuffer)];
  ret = ObSchemaUtils::deep_copy_schema(buf, table_schema, new_table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != new_table_schema);
  ASSERT_TRUE(SchemaTestUtils::equal_table_schema(table_schema, *new_table_schema));
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_schema_utils.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
