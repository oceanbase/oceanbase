/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_defer.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"
#include "sql/table_format/iceberg/spec/schema.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdio.h>

using namespace oceanbase;
using namespace oceanbase::share;
namespace iceberg = oceanbase::sql::iceberg;

class TestSchema : public ::testing::Test
{
public:
  TestSchema();
  ~TestSchema();
  virtual void SetUp();
  virtual void TearDown();
};

TestSchema::TestSchema() {}

TestSchema::~TestSchema() {}

void TestSchema::SetUp() {}

void TestSchema::TearDown() {
}

TEST_F(TestSchema, parse_complex_type)
{
  {
    // array<int>
    ObString array_json = R"({
                          "type": "list",
                          "element-id": 5,
                          "element": "int",
                          "element-required": false})";

    ObArenaAllocator allocator;
    iceberg::TableMetadata table_metadata(allocator);

    ObJsonNode *json_node = NULL;
    ObJsonParser::get_tree(&allocator, array_json, json_node);
    ObString result_type;
    ASSERT_EQ(OB_SUCCESS, iceberg::Schema::parse_complex_type(allocator, *json_node, result_type));
    ASSERT_TRUE(0 == ObString("ARRAY(INT)").case_compare(result_type));
  }

  {
    // array<decimal<10, 2>>
    ObString array_json = "{\"type\": \"list\",\"element-id\": 5,\"element\": \"decimal(10,2)\",\"element-required\": false}";

    ObArenaAllocator allocator;
    iceberg::TableMetadata table_metadata(allocator);

    ObJsonNode *json_node = NULL;
    ObJsonParser::get_tree(&allocator, array_json, json_node);
    ObString result_type;
    ASSERT_EQ(OB_SUCCESS, iceberg::Schema::parse_complex_type(allocator, *json_node, result_type));
    ASSERT_TRUE(0 == ObString("ARRAY(DECIMAL(10, 2))").case_compare(result_type));
  }

  {
    // array<boolean>
    ObString array_json = R"({
                          "type": "list",
                          "element-id": 5,
                          "element": "boolean",
                          "element-required": false})";

    ObArenaAllocator allocator;
    iceberg::TableMetadata table_metadata(allocator);

    ObJsonNode *json_node = NULL;
    ObJsonParser::get_tree(&allocator, array_json, json_node);
    ObString result_type;
    ASSERT_EQ(OB_SUCCESS, iceberg::Schema::parse_complex_type(allocator, *json_node, result_type));
    ASSERT_TRUE(0 == ObString("ARRAY(TINYINT)").case_compare(result_type));
  }

  {
    // array<map<string, string>>
    ObString array_json = R"({
                          "type": "list",
                          "element-id": 6,
                          "element": {
                              "type": "map",
                              "key-id": 7,
                              "key": "string",
                              "value-id": 8,
                              "value": "string",
                              "value-required": false
                          },
                          "element-required": false})";

    ObArenaAllocator allocator;
    iceberg::TableMetadata table_metadata(allocator);

    ObJsonNode *json_node = NULL;
    ObJsonParser::get_tree(&allocator, array_json, json_node);
    ObString result_type;
    ASSERT_EQ(OB_NOT_SUPPORTED, iceberg::Schema::parse_complex_type(allocator, *json_node, result_type));
  }

  {
    // struct<a: int>
    ObString array_json = R"({
                          "type": "struct",
                          "fields": [
                              {
                                  "id": 9,
                                  "name": "a",
                                  "required": false,
                                  "type": "int"
                              }
                            ]
                          })";

    ObArenaAllocator allocator;
    iceberg::TableMetadata table_metadata(allocator);

    ObJsonNode *json_node = NULL;
    ObJsonParser::get_tree(&allocator, array_json, json_node);
    ObString result_type;
    ASSERT_EQ(OB_NOT_SUPPORTED, iceberg::Schema::parse_complex_type(allocator, *json_node, result_type));
  }


  {
    // array<array<int>>
    ObString array_json = R"({
                          "type": "list",
                          "element-id": 10,
                          "element": {
                              "type": "list",
                              "element-id": 11,
                              "element": "int",
                              "element-required": false
                            },
                          "element-required": false
                          })";

    ObArenaAllocator allocator;
    iceberg::TableMetadata table_metadata(allocator);

    ObJsonNode *json_node = NULL;
    ObJsonParser::get_tree(&allocator, array_json, json_node);
    ObString result_type;
    ASSERT_EQ(OB_SUCCESS, iceberg::Schema::parse_complex_type(allocator, *json_node, result_type));
    ASSERT_TRUE(0 == ObString("ARRAY(ARRAY(INT))").case_compare(result_type));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}