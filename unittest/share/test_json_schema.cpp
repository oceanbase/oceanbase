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

#include <gtest/gtest.h>
#define private public
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/json_type/ob_json_schema.h"
#undef private

#include <sys/time.h>
using namespace std;
namespace oceanbase {
namespace common {

class TestJsonSchema : public ::testing::Test {
public:
  TestJsonSchema()
  {}
  ~TestJsonSchema()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestJsonSchema);
};

ObString j_schema_to_str("{\"$id\": \"httpexample.com/schemas/customer\", \"type\": \"object\", \"$defs\": {\"name\": {\"type\": \"string\"}}, \"required\": [\"first_name\", \"last_name\"], \"properties\": {\"last_name\": {\"$ref\": \"#/$defs/name\"}, \"first_name\": {\"$ref\": \"#/$defs/name\"}}}");
TEST_F(TestJsonSchema, test_parse_json_schema_ref)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"$id\": \"httpexample.com/schemas/customer\","
                          "\"type\": \"object\","
                          "\"properties\": {\"first_name\": { \"$ref\": \"#/$defs/name\" },"
                          "\"last_name\": { \"$ref\": \"#/$defs/name\" }},"
                          "\"required\": [\"first_name\", \"last_name\"],"
                          "\"$defs\": {\"name\": { \"type\": \"string\" }}}");

  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(j_schema_to_str, ObString(j_buf.length(), j_buf.ptr()));
  common::ObString j_text_wrong("{\"$id\": \"httpexample.com/schemas/customer\","
                                "\"type\": \"object\","
                                "\"properties\": {\"first_name\": { \"$ref\": \"#/$defs/name\" },"
                                "\"last_name\": { \"$ref\": \"/$defs/name\" }},"
                                "\"required\": [\"first_name\", \"last_name\"],"
                                "\"$defs\": {\"name\": { \"type\": \"string\" }}}");
  ASSERT_EQ(OB_ERR_UNSUPPROTED_REF_IN_JSON_SCHEMA, ObJsonBaseFactory::get_json_base(&allocator, j_text_wrong,
            ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
}

TEST_F(TestJsonSchema, test_parse_json_schema_dup_key)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"$id\": \"httpexample.com/schemas/customer\","
                          "\"type\": \"object\","
                          "\"type\": \"number\","
                          "\"properties\": {\"first_name\": { \"$ref\": \"#/$defs/name\" },"
                          "\"last_name\": { \"$ref\": \"#/$defs/name\" }},"
                          "\"required\": [\"first_name\", \"last_name\"],"
                          "\"required\": [\"test_name\"],"
                          "\"$defs\": {\"name\": { \"type\": \"string\" }}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ASSERT_EQ(OB_SUCCESS,j_tree->print(j_buf, false));
  ASSERT_EQ(j_schema_to_str, ObString(j_buf.length(), j_buf.ptr()));
  common::ObString j_text_wrong("{\"$id\": \"httpexample.com/schemas/customer\","
                                "\"type\": \"object\","
                                "\"properties\": {\"first_name\": { \"$ref\": \"#/$defs/name\" },"
                                "\"last_name\": { \"$ref\": \"#/$defs/name\" }},"
                                "\"last_name\": { \"$ref\": \"/$defs/name\" }},"
                                "\"required\": [\"first_name\", \"last_name\"],"
                                "\"$defs\": {\"name\": { \"type\": \"string\" }}}");
  ASSERT_EQ(OB_ERR_UNSUPPROTED_REF_IN_JSON_SCHEMA, ObJsonBaseFactory::get_json_base(&allocator, j_text_wrong,
            ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
}

# define STRING_TYPE_COUNT 2
ObString string_type_str[STRING_TYPE_COUNT] = {
  "{\"schema\": {\"type\": 4, \"pattern\": \"^S_\", \"maxLength\": 3, \"minLength\": 2}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_string_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"string\",\"pattern\":\"^S_\",\"minLength\": 2,\"maxLength\": 3}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(STRING_TYPE_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), string_type_str[i]);
  }
}

# define NUMBER_TYPE_COUNT 2
ObString number_type_str[NUMBER_TYPE_COUNT] = {
  "{\"schema\": {\"type\": 8, \"minimum\": 0, \"exclusiveMaximum\": 100}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_number_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"number\", \"minimum\": 0, \"maximum\": 100, \"exclusiveMinimum\": false, \"exclusiveMaximum\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(NUMBER_TYPE_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), number_type_str[i]);
  }
}

ObString integer_type_str[NUMBER_TYPE_COUNT] = {
  "{\"schema\": {\"type\": 16, \"minimum\": 0, \"exclusiveMaximum\": 100}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_integer_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"integer\",\"type\": \"number\", \"minimum\": 0, \"maximum\": 100,\"exclusiveMaximum\": true, \"exclusiveMinimum\": false}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(NUMBER_TYPE_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), integer_type_str[i]);
  }
}

# define NULL_TYPE_COUNT 2
ObString null_type_str[NULL_TYPE_COUNT] = {
  "{\"schema\": {\"enum\": [\"red\", \"amber\", \"green\"], \"type\": 1}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_null_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"null\", \"enum\": [\"red\", \"amber\", \"green\"], \"minimum\": 0, \"maximum\": 100,\"exclusiveMaximum\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(NULL_TYPE_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), null_type_str[i]);
  }
}

ObString boolean_type_str[NULL_TYPE_COUNT] = {
  "{\"schema\": {\"enum\": [\"red\", \"amber\", \"green\"], \"type\": 2}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_boolean_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"boolean\", \"enum\": [\"red\", \"amber\", \"green\"], \"minimum\": 0, \"maximum\": 100,\"exclusiveMaximum\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(NULL_TYPE_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), boolean_type_str[i]);
  }
}

# define PRO_COUNT 2
ObString pro_str[PRO_COUNT] = {
  "{\"schema\": {\"type\": 32}, \"properties\": {\"number\": {\"schema\": {\"type\": 8}}, \"street_name\": {\"schema\": {\"type\": 4}}, \"street_type\": {\"schema\": {\"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_property)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {"
    "\"number\": { \"type\": \"number\" },"
    "\"street_name\": { \"type\": \"string\" },"
    "\"street_type\": { \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(PRO_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), pro_str[i]);
  }
}

# define DEP_REUQUIRED_COUNT 2
ObString dep_required_str[DEP_REUQUIRED_COUNT] = {
  "{\"schema\": {\"type\": 32, \"dependentRequired\": {\"credit_card\": [\"billing_address\"]}}, \"properties\": {\"number\": {\"schema\": {\"type\": 8}}, \"street_name\": {\"schema\": {\"type\": 4}}, \"street_type\": {\"schema\": {\"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_dep_required)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {"
    "\"number\": { \"type\": \"number\" },"
    "\"street_name\": { \"type\": \"string\" },"
    "\"street_type\": { \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}},"
    "\"dependencies\": {\"credit_card\": [\"billing_address\"]}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(DEP_REUQUIRED_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), dep_required_str[i]);
  }
}

# define DEP_UNNESTED_COUNT 3
ObString dep_unnested_str[DEP_UNNESTED_COUNT] = {
  "{\"schema\": {\"type\": 32, \"required\": [\"name\"], \"dependentRequired\": {\"name\": [\"last_name\"]}}, \"properties\": {\"name\": {\"schema\": {\"type\": 4}}, \"credit_card\": {\"schema\": {\"type\": 8}}, \"billing_address\": {\"schema\": {\"type\": 4}, \"composition\": [2]}}, \"composition\": [], \"dependentSchemas\": {\"credit_card\": [2]}}",
  "{}",
  "{\"type\": 8}"
};
TEST_F(TestJsonSchema, test_parse_dep_unnested)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {"
    "\"name\": { \"type\": \"string\" },"
    "\"credit_card\": { \"type\": \"number\" },"
    "\"billing_address\": { \"type\": \"string\" }},"
  "\"required\": [\"name\"],"
  "\"dependencies\": {"
    "\"credit_card\": {"
      "\"properties\": {"
        "\"billing_address\": { \"type\": \"number\" }}},"
    "\"name\": [\"last_name\"]}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(DEP_UNNESTED_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), dep_unnested_str[i]);
  }
}

# define DEP_NESTED_COUNT 4
ObString dep_nested_str[DEP_NESTED_COUNT] = {
  "{\"schema\": {\"type\": 32, \"required\": [\"name\"], \"dependentRequired\": {\"name\": [\"billing_address\"]}}, \"properties\": {\"name\": {\"schema\": {\"type\": 4}}, \"credit_card\": {\"schema\": {\"type\": 8}}, \"billing_address\": {\"schema\": {\"type\": 4}, \"composition\": [2]}}, \"composition\": [3], \"dependentSchemas\": {\"credit_card\": [2, {\"dependentSchemas\": {\"billing_address\": [3]}}]}}",
  "{}",
  "{\"type\": 4}",
  "{\"required\": [\"credit_card\"]}"
};
TEST_F(TestJsonSchema, test_parse_dep_nested)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {"
    "\"name\": { \"type\": \"string\" },"
    "\"credit_card\": { \"type\": \"number\" },"
    "\"billing_address\": { \"type\": \"string\" }},"
  "\"required\": [\"name\"],"
  "\"dependencies\": {\"credit_card\": {\"properties\": {"
    "\"billing_address\": { \"type\": \"string\" }},"
    "\"dependencies\": {\"billing_address\": { \"required\": [\"credit_card\"]}}},"
    "\"name\": [\"billing_address\"]}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(DEP_NESTED_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), dep_nested_str[i]);
  }
}

# define PATTERN_COUNT 2
ObString pattern_pro_str[PATTERN_COUNT] = {
  "{\"schema\": {\"type\": 32}, \"patternProperties\": {\"^I_\": {\"schema\": {\"type\": 16}}, \"^S_\": {\"schema\": {\"type\": 4}}}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_pattern_pro)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"patternProperties\": {"
    "\"^S_\": { \"type\": \"string\" },"
    "\"^I_\": { \"type\": \"integer\" },"
    "\"^*\": { \"type\": \"boolean\" }}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(PATTERN_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), pattern_pro_str[i]);
  }
}

# define PATTERN_AND_PRO 2
ObString pattern_and_pro_str[PATTERN_AND_PRO] = {
  "{\"schema\": {\"type\": 32}, \"properties\": {\"I_0\": {\"schema\": {\"type\": 128, \"maxLength\": 20}}, \"S_25\": {\"schema\": {\"type\": 128, \"maxLength\": 20}}}, \"patternProperties\": {\"^I_\": {\"schema\": {\"type\": 16}}, \"^S_\": {\"schema\": {\"type\": 4}}, \"^[SI]*\": {\"schema\": {\"maxLength\": 20}}}}",
  "{}",
};
TEST_F(TestJsonSchema, test_parse_pattern_and_pro)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"patternProperties\": {"
    "\"^S_\": { \"type\": \"string\" },"
    "\"^I_\": { \"type\": \"integer\" },"
    "\"^[SI]*\": { \"maxLength\": 20 }},"
    "\"properties\": {"
    "\"S_25\": { \"type\": \"number\" },"
    "\"I_0\": { \"type\": \"string\" }}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(PATTERN_AND_PRO, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), pattern_and_pro_str[i]);
  }
}

# define ADD_PRO_COUNT 2
ObString add_pro_str[ADD_PRO_COUNT] = {
  "{\"schema\": {\"type\": 32, \"additionalProperties\": [[], [\"^I_\", \"^S_\"]]}, \"patternProperties\": {\"^I_\": {\"schema\": {\"type\": 16}}, \"^S_\": {\"schema\": {\"type\": 4}}}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_add_pro)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"patternProperties\": {"
    "\"^S_\": { \"type\": \"string\" },"
    "\"^I_\": { \"type\": \"integer\" },"
    "\"^*\": { \"type\": \"boolean\" }},"
    "\"additionalProperties\": false }");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(ADD_PRO_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), add_pro_str[i]);
  }
}

# define PATTERN_PRO_ADD 2
ObString pattern_pro_add_str[PATTERN_PRO_ADD] = {
  "{\"schema\": {\"type\": 32, \"required\": [\"name\", \"email\"]}, \"properties\": {\"I_0\": {\"schema\": {\"type\": 128, \"maxLength\": 20}}, \"S_25\": {\"schema\": {\"type\": 128, \"maxLength\": 20}}}, \"patternProperties\": {\"^I_\": {\"schema\": {\"type\": 16}}, \"^S_\": {\"schema\": {\"type\": 4}}, \"^[SI]*\": {\"schema\": {\"maxLength\": 20}}}, "
  "\"additionalProperties\": [[[\"I_0\", \"S_25\", \"name\", \"email\"], [\"^I_\", \"^S_\", \"^[SI]*\"]], {\"schema\": {\"type\": 4}}]}",
  "{}",
};
TEST_F(TestJsonSchema, test_parse_pattern_pro_additional)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"patternProperties\": {"
    "\"^S_\": { \"type\": \"string\" },"
    "\"^I_\": { \"type\": \"integer\" },"
    "\"^[SI]*\": { \"maxLength\": 20 }},"
    "\"properties\": {"
    "\"S_25\": { \"type\": \"number\" },"
    "\"I_0\": { \"type\": \"string\" }},"
    "\"additionalProperties\": { \"type\": \"string\" },"
    "\"required\": [\"name\", \"email\"]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(PATTERN_PRO_ADD, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), pattern_pro_add_str[i]);
  }
}

# define ITEMS_COUNT 2
ObString itmes_str[ITEMS_COUNT] = {
  "{\"items\": {\"schema\": {\"enum\": [1, 2, 3, 4], \"type\": 8}}, \"schema\": {\"type\": 64}}",
  "{}",
};
TEST_F(TestJsonSchema, test_parse_items)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": { \"type\": \"number\","
              "\"enum\": [1, 2, 3, 4] }}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(ITEMS_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), itmes_str[i]);
  }
}

# define TUPLE_ITEMS_COUNT 2
ObString tuple_itmes_str[TUPLE_ITEMS_COUNT] = {
  "{\"schema\": {\"type\": 64, \"additionalItems\": 4}, \"tupleItems\": {\"0\": {\"schema\": {\"type\": 8}}, \"1\": {\"schema\": {\"type\": 4}}, \"2\": {\"schema\": {\"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}, \"3\": {\"schema\": {\"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]}}}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_tuple_items)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(TUPLE_ITEMS_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), tuple_itmes_str[i]);
  }
}

# define ADD_ITEMS_COUNT 2
ObString add_itmes_str[ADD_ITEMS_COUNT] = {
  "{\"schema\": {\"type\": 64}, \"tupleItems\": {\"0\": {\"schema\": {\"type\": 8}}, \"1\": {\"schema\": {\"type\": 4}}, \"2\": {\"schema\": {\"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}, \"3\": {\"schema\": {\"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]}}}, \"additionalItems\": {\"4\": {\"schema\": {\"type\": 4}}}}",
  "{}"
};
TEST_F(TestJsonSchema, test_parse_add_items)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": { \"type\": \"string\" }}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(ADD_ITEMS_COUNT, schema_count);
  for (int i = 0; i < schema_count; ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), add_itmes_str[i]);
  }
}

# define ALLOF_UNNESTED_COUNT 4
ObString allof_unnested_str[ALLOF_UNNESTED_COUNT] = {
  "{\"allOf\": [[2], [3]], \"schema\": {\"type\": 64, \"additionalItems\": 4}, \"tupleItems\": {\"0\": {\"schema\": {\"type\": 8}}, \"1\": {\"schema\": {\"type\": 4}}, \"2\": {\"schema\": {\"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}, \"3\": {\"schema\": {\"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]}}}, \"composition\": [2, 3]}",
  "{}",
  "{\"type\": 4}",
  "{\"maxLength\": 5}"
};
TEST_F(TestJsonSchema, test_all_of_unnested)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false,"
  "\"allOf\": ["
    "{ \"type\": \"string\"},"
    "{ \"maxLength\": 5 }]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(ALLOF_UNNESTED_COUNT, schema_count);
  for (int i = 0; i < schema_count; i++) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), allof_unnested_str[i]);
  }
}
# define ALLOF_NESTED_COUNT 6
ObString allof_nested_str[ALLOF_NESTED_COUNT] = {
  "{\"allOf\": [[2], [3], [{\"allOf\": [[4, {\"allOf\": [[5]]}]]}]], \"schema\": {\"type\": 64, \"additionalItems\": 4}, \"tupleItems\": {\"0\": {\"schema\": {\"type\": 8}}, \"1\": {\"schema\": {\"type\": 4}}, \"2\": {\"schema\": {\"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}, \"3\": {\"schema\": {\"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]}}}, \"composition\": [2, 3, 4, 5]}",
  "{}",
  "{\"type\": 4}",
  "{\"maxLength\": 5}",
  "{\"type\": 8}",
  "{\"required\": [\"city\"]}"
};
TEST_F(TestJsonSchema, test_all_of_nested)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false,"
  "\"allOf\": ["
    "{ \"type\": \"string\"},"
    "{ \"maxLength\": 5 },"
    "{ \"allOf\": [{\"type\": \"number\", \"allOf\":[{\"required\":[\"city\"]} ]} ]} ]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(ALLOF_NESTED_COUNT, schema_count);
  for (int i = 0; i < schema_count; i++) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), allof_nested_str[i]);
  }
}

# define NOT_NESTED_COUNT 3
ObString not_nested_str[NOT_NESTED_COUNT] = {
  "{\"not\": [{\"not\": [2]}], \"schema\": {\"type\": 64, \"additionalItems\": 4}, \"tupleItems\": {\"0\": {\"schema\": {\"type\": 8}}, \"1\": {\"schema\": {\"type\": 4}}, \"2\": {\"schema\": {\"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}}, \"3\": {\"schema\": {\"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]}}}, \"composition\": [2]}",
  "{}",
  "{\"required\": [\"city\"]}"
};
TEST_F(TestJsonSchema, test_not_nested)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false,"
  "\"not\": "
    "{ \"not\": {\"required\":[\"city\"]}}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(NOT_NESTED_COUNT, schema_count);
  for (int i = 0; i < schema_count; i++) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), not_nested_str[i]);
  }
}

# define COUNT_COMPLEX_SCHEMA 17
ObString complex_str[COUNT_COMPLEX_SCHEMA] = {
  "{\"allOf\": [[2], [3, 4], [{\"oneOf\": [[5, {\"allOf\": [[6]]}]]}]], \"anyOf\": [[7], [8, 9, 10, 11, 12, 13], [{\"oneOf\": [[14], [], [15, 16]]}]], "
  "\"schema\": {\"type\": 32, \"required\": [\"street_address\"]}, \"properties\": {\"city\": {\"composition\": [9, 12]}, \"state\": {\"composition\": [3, 10, 13]}, \"street_address\": {\"schema\": {\"type\": 32}, "
  "\"properties\": {\"k2\": {\"schema\": {\"type\": 4}}}, \"composition\": [16]}}, \"composition\": [2, 5, 6, 7, 8, 14, 15], \"patternProperties\": {\"^[cs]*\": {\"composition\": [11]}, \"^city*\": {\"schema\": {\"type\": 4}}}, "
  "\"additionalProperties\": [[[\"street_address\"], [\"^city*\"]], {\"schema\": {\"type\": 4}}, [[\"state\"], []], {\"composition\": [4]}]}",
  "{}",
  "{\"type\": 32}",
  "{\"type\": 4}",
  "{\"maxProperties\": 6}",
  "{\"type\": 8}",
  "{\"required\": [\"city\"]}",
  "{\"type\": 32}",
  "{\"type\": 32}",
  "{\"type\": 4}",
  "{\"type\": 4}",
  "{\"type\": 4}",
  "-1",
  "-1",
  "{\"type\": 8}",
  "{\"type\": 32}",
  "{\"type\": 4}",
};
TEST_F(TestJsonSchema, test_complex_schema)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {\"street_address\": { \"type\": \"object\", \"properties\": {\"k2\":{\"type\": \"string\"}}}},"
  "\"patternProperties\": {\"^city*\": { \"type\": \"string\" }},"
  "\"additionalProperties\": { \"type\": \"string\" },"
  "\"required\": [\"street_address\"],"

  "\"allOf\": [{\"type\":\"object\",\"require\":[\"state\"]},"
  "{\"properties\": {\"state\": { \"type\": \"string\"} },"
  "\"additionalProperties\": { \"maxProperties\": 6}},"
  "{ \"oneOf\": [{\"type\": \"number\",\"allOf\":[{\"required\":[\"city\"]}]}]}],"
  "\"anyOf\": [{\"type\":\"object\"},{\"type\": \"object\","
  "\"properties\": {\"city\": { \"type\": \"string\" },\"state\": { \"type\": \"string\" }},"
    "\"patternProperties\": {\"^[cs]*\": { \"type\": \"string\" }}},"
    "{ \"oneOf\": [{\"type\": \"number\"},{\"maxmum\":10},"
    "{\"type\": \"object\",\"properties\": {\"street_address\":{\"type\": \"string\"}}}]}]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  int schema_count = json_schema.schema_map_->element_count();
  ASSERT_EQ(COUNT_COMPLEX_SCHEMA, schema_count);
  for (int i = 0; i < schema_count; i++) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
    ASSERT_EQ(ObString(j_buf.ptr()), complex_str[i]);
  }
}

# define STRING_VALIDATOR_COUNT 8
ObString string_validator_text = {
  "[\"S_1\",\"S_\", \"S\", \"S_12\", [\"S_1\"] , 12 , true , null ]"
};
bool string_validator_ans[STRING_VALIDATOR_COUNT] = {true, true, false, false, false, false, false, false};
TEST_F(TestJsonSchema, test_validate_string_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"string\",\"pattern\":\"^S_\",\"minLength\": 2,\"maxLength\": 3}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, string_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < STRING_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS,j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, string_validator_ans[i]);
  }
}

# define NULL_VALIDATOR_COUNT 5
ObString null_validator_text = {
  "[null,\"S_\", 12 , true , [12] ]"
};
bool null_validator_ans[NULL_VALIDATOR_COUNT] = {true, false, false, false, false};
TEST_F(TestJsonSchema, test_validate_null_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"null\",\"pattern\":\"^S_\",\"minLength\": 2,\"maxLength\": 3}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, null_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < NULL_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS,j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, null_validator_ans[i]);
  }
}

ObString bool_validator_text = {
  "[null,\"S_\", 12 , true , [12] ]"
};
bool bool_validator_ans[NULL_VALIDATOR_COUNT] = {false, false, false, true, false};
TEST_F(TestJsonSchema, test_validate_bool_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"boolean\",\"pattern\":\"^S_\",\"minLength\": 2,\"maxLength\": 3}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, null_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < NULL_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS,j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, bool_validator_ans[i]);
  }
}

# define NUMBER_VALIDATOR_COUNT 10
ObString number_validator_text = {
  "[0, 100, 50, 50.5, 3.141592653, true, false, \"test\", {\"a\":1}, [2]]"
};
bool number_validator_ans[NUMBER_VALIDATOR_COUNT] = {true, false, true, true, true, false, false, false, false, false};
TEST_F(TestJsonSchema, test_validate_number_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"number\", \"minimum\": 0, \"maximum\": 100, \"exclusiveMinimum\": false, \"exclusiveMaximum\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, number_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < NUMBER_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS,j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, number_validator_ans[i]);
  }
}

# define INTEGER_VALIDATOR_COUNT 10
ObString integer_validator_text = {
  "[0, 100, 50, \"50.5\", 3.141592653, true, false, \"test\", {\"a\":1}, [2]]"
};
bool integer_validator_ans[INTEGER_VALIDATOR_COUNT] = {true, true, true, false, false, false, false, false, false, false};
TEST_F(TestJsonSchema, test_validate_integer_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"integer\", \"minimum\": 0, \"maximum\": 100.5, \"exclusiveMinimum\": false, \"exclusiveMaximum\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, integer_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < INTEGER_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS,j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, integer_validator_ans[i]);
  }
}

# define OBJECT_SCALAR_VALIDATOR_COUNT 10
ObString object_scalar_validator_text = {
  "[{\"name\": 0, \"test\": 1, \"credit_card\": 2, \"billing_address\": 3},"
  "{\"name\": 0, \"test\": 1},"
  "{\"name\": 0, \"billing_address\": 3},"
  "{\"name\": 0, \"credit_card\": 2, \"billing_address\": 3},"
  "{\"name\": 0},"
  "{\"credit_card\": 2, \"billing_address\": 3},"
  "{\"name\": 0, \"credit_card\": 2},"
  "{\"name\": 0, \"credit_card\": 2, \"billing_address1\": 3},"
  "{\"name\": 0, \"credit_card\": 2, \"billing_addresss\": 3},"
  "[{\"name\": 0, \"test\": 1, \"credit_card\": 2, \"billing_address\": 3}]"
  "]"
};
bool object_scalar_validator_ans[OBJECT_SCALAR_VALIDATOR_COUNT] = {true, true, true, true, false, false, false, false, false, false};
TEST_F(TestJsonSchema, test_object_scalar_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\",\"minProperties\": 2,\"maxProperties\": 3.5,"
                          "\"dependencies\": {\"credit_card\": [\"billing_address\"]},"
                          "\"required\": [\"name\"]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, object_scalar_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < OBJECT_SCALAR_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, object_scalar_validator_ans[i]);
  }
}

# define ARRAY_SCALAR_VALIDATOR_COUNT 5
ObString array_scalar_validator_text = {
  "[[1],"
  "[1,2,3,4,5],"
  "[1,2,3,4,5,6],"
  "[1,2,3,1],"
  "{\"name\": 0, \"test\": 1, \"credit_card\": 2, \"billing_address\": 3}"
  "]"
};
bool array_scalar_validator_ans[ARRAY_SCALAR_VALIDATOR_COUNT] = {true, true, false, false, false};
TEST_F(TestJsonSchema, test_array_scalar_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\", \"minItems\": 2.5, \"maxItems\": 5, \"uniqueItems\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, array_scalar_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < ARRAY_SCALAR_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, array_scalar_validator_ans[i]);
  }
}

# define ENUM_VALIDATOR_COUNT 10
ObString enum_validator_text = {
  "[\"test\", 1, [2], false, null, {\"test\":3}, 2, true, \"test1\", {\"test\":4}]"
};
bool enum_validator_ans[ENUM_VALIDATOR_COUNT] = {true, true, true, true, true, true, false, false, false, false};
TEST_F(TestJsonSchema, test_enum_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"enum\": [\"test\", 1, false, null, [2], {\"test\":3}]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator,enum_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < ENUM_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, enum_validator_ans[i]);
  }
}

# define PRO_VALIDATOR_COUNT 6
ObString pro_validator_text = {
  "[{\"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\" },"
  "{ },"
  "{\"number\": 1600, \"street_name\": \"Pennsylvania\" },"
  "{\"number\": \"1600\", \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\"},"
  "{\"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\", \"direction\": \"NW\"},"
  "{\"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"test\" }]"
};
bool pro_validator_ans[PRO_VALIDATOR_COUNT] = {true, true, true, false, false, false};
TEST_F(TestJsonSchema, test_pro_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {"
    "\"number\": { \"type\": \"number\" },"
    "\"street_name\": { \"type\": \"string\" },"
    "\"street_type\": { \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]}},"
    "\"additionalProperties\": false}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, pro_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < PRO_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, pro_validator_ans[i]);
  }
}

# define PATTERN_PRO_VALIDATOR_COUNT 8
ObString pattern_pro_validator_text = {
  "[{ \"S_25\": \"This is a string\" },"
  "{ },"
  "{ \"I_0\": 42 },"
  "{ \"I_0\": 42, \"S_25\": \"This is a string\", \"S_test\": \"string\" },"
  "{ \"keyword\": \"value\" },"
  "{ \"S_25\": \"test\" },"
  "{ \"S_25\": \"This is a string\" , \"I_0\": 42, \"I_1\": \"test\"},"
  "{ \"S_25\": \"This is a string\" , \"I_0\": \"test\", \"I_0\": 42}]"
};
bool pattern_pro_validator_ans[PATTERN_PRO_VALIDATOR_COUNT] = {true, true, true, true, false, false, false, false};
TEST_F(TestJsonSchema, test_pattern_pro_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {"
    "\"S_25\": { \"minLength\": 5 }},"
  "\"patternProperties\": {"
    "\"^S_\": { \"type\": \"string\" },"
    "\"^I_\": { \"type\": \"integer\"}},"
    "\"additionalProperties\": false}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, pattern_pro_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, ObJsonParser::JSN_PRESERVE_DUP_FLAG));
  for (int i = 0; i < PATTERN_PRO_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    j_buf.reset();
    j_value->print(j_buf, false);
    // cout<<i<<":"<<is_valid<<": "<<j_buf.ptr()<<endl;
    ASSERT_EQ(is_valid, pattern_pro_validator_ans[i]);
  }
}

# define ADD_PRO_VALIDATOR_COUNT 10
ObString add_pro_validator_text = {
  "[{ \"S_25\": \"This is a string\" },"
  "{ },"
  "{ \"I_0\": 42 },"
  "{ \"I_0\": 42, \"S_25\": \"This is a string\", \"S_test\": \"string\" },"
  "{ \"builtin\": 42 },"
  "{ \"keyword\": \"value\" },"
  "{ \"keyword\": 16 },"
  "{ \"I_0\": 2.5 },"
  "{ \"test1\": true},"
  "{ \"test2\": null}]"
};
bool add_pro_validator_ans[ADD_PRO_VALIDATOR_COUNT] = {true, true, true, true, true, true, false, false, false, false};
TEST_F(TestJsonSchema, test_add_pro_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {\"builtin\": { \"type\": \"number\" }},"
  "\"patternProperties\": {\"^S_\": { \"type\": \"string\" },\"^I_\": { \"type\": \"integer\" }},"
  "\"additionalProperties\": { \"type\": \"string\" }}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, add_pro_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < json_schema.schema_map_->element_count(); ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
  }
  for (int i = 0; i < ADD_PRO_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, add_pro_validator_ans[i]);
  }
}

# define ITEMS_VALIDATOR_COUNT 4
ObString items_validator_text = {
  "[[\"NW\", \"NE\", \"SW\", \"SE\"],"
  "[],"
  "[\"NW\"],"
  "[\"NW\", \"NE\", \"SW\", \"SE\", \"test\"]]"
};
bool items_validator_ans[ITEMS_VALIDATOR_COUNT] = {true, true, true, false};
TEST_F(TestJsonSchema, test_items_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": { \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]},"
  "\"additionalItems\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, items_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < ITEMS_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS,schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, items_validator_ans[i]);
  }
}

# define TUPLE_ITEMS_VALIDATOR_COUNT 4
ObString tuple_items_validator_text = {
  "[[1600, \"Pennsylvania\", \"Avenue\", \"NW\"],"
  "[],"
  "[1600, \"Pennsylvania\", \"Avenue\"],"
  "[1600, \"Pennsylvania\", \"Avenue\", \"NW\", \"test\"]]"
};
bool tuple_items_validator_ans[TUPLE_ITEMS_VALIDATOR_COUNT] = {true, true, true, false};
TEST_F(TestJsonSchema, test_tuple_items_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": ["
    "{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, tuple_items_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < TUPLE_ITEMS_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, tuple_items_validator_ans[i]);
  }
}

# define ADD_ITEMS_VALIDATOR_COUNT 4
ObString add_items_validator_text = {
  "[[1600, \"Pennsylvania\", \"Avenue\", \"NW\"],"
  "[],"
  "[1600, \"Pennsylvania\", \"Avenue\", \"NW\", 89757],"
  "[1600, \"Pennsylvania\", \"Avenue\", \"NW\", \"test\"]]"
};
bool add_items_validator_ans[ADD_ITEMS_VALIDATOR_COUNT] = {true, true, true, false};
TEST_F(TestJsonSchema, test_add_items_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": ["
    "{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": { \"type\": \"number\" }}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, add_items_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < ADD_ITEMS_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, add_items_validator_ans[i]);
  }
}

# define UNNESTED_ALLOF_VALIDATOR_COUNT 6
ObString unnested_allof_validator_text = {
  "[[1600, \"test\", \"Avenue\", \"NW\"],"
  "[],"
  "[1600, \"test\", \"Avenue\"],"
  "[1600, \"Pennsylvania\", \"Avenue\", \"NW\"],"
  "[1600, \"test\", \"Boulevard\", \"NW\"],"
  "[1600]]"
};
bool unnested_allof_validator_ans[UNNESTED_ALLOF_VALIDATOR_COUNT] = {true, true, true, false, false, true};
TEST_F(TestJsonSchema, test_unnested_allof_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false,"
  "\"allOf\": [{\"items\":{ \"maxLength\": 6 }}]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, unnested_allof_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < UNNESTED_ALLOF_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, unnested_allof_validator_ans[i]);
  }
}

# define UNNESTED_ANYOF_VALIDATOR_COUNT 6
ObString unnested_anyof_validator_text = {
  "[[1600, \"test\", \"Avenue\", \"NW\"],"
  "[],"
  "[1600, \"test\", \"Avenue\"],"
  "[1600, \"Pennsylvania\", \"Avenue\", \"NW\"],"
  "[1600, \"test\", \"Boulevard\", \"NW\"],"
  "[1600]]"
};
bool unnested_anyof_validator_ans[UNNESTED_ANYOF_VALIDATOR_COUNT] = {true, true, true, false, true, true};
TEST_F(TestJsonSchema, test_unnested_anyof_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false,"
  "\"anyOf\": [{\"items\":{ \"maxLength\": 6 }}, {\"items\":{ \"maxLength\": 9 }}]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, unnested_anyof_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < UNNESTED_ANYOF_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, unnested_anyof_validator_ans[i]);
  }
}

# define UNNESTED_ONEOF_VALIDATOR_COUNT 6
ObString unnested_oneof_validator_text = {
  "[[1600, \"test\", \"Avenue\", \"NW\"],"
  "[],"
  "[1600, \"test\", \"Boulevard\"],"
  "[1600, \"Pennsylvania\", \"Boulevard\"],"
  "[1600, \"test\", \"Boulevard\",\"SW\"],"
  "[1600]]"
};
bool unnested_oneof_validator_ans[UNNESTED_ONEOF_VALIDATOR_COUNT] = {false, false, true, true, true, false};
TEST_F(TestJsonSchema, test_unnested_oneof_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false,"
  "\"oneOf\": [{\"items\":{ \"maxLength\": 6 }}, {\"items\":{ \"maxLength\": 12 }}]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, unnested_oneof_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < UNNESTED_ONEOF_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, unnested_oneof_validator_ans[i]);
  }
}

# define UNNESTED_NOT_VALIDATOR_COUNT 6
ObString unnested_not_validator_text = {
  "[[1600, \"test\", \"Avenue\", \"NW\"],"
  "[],"
  "[1600, \"test\", \"Boulevard\"],"
  "[1600, \"Pennsylvania\"],"
  "[1600, \"test\", \"Boulevard\",\"SW\"],"
  "[1600]]"
};
bool unnested_not_validator_ans[UNNESTED_NOT_VALIDATOR_COUNT] = {true, false, true, false, true, false};
TEST_F(TestJsonSchema, test_unnested_not_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"array\","
  "\"items\": [{ \"type\": \"number\" },"
    "{ \"type\": \"string\" },"
    "{ \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"] },"
    "{ \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"] }],"
  "\"additionalItems\": false,"
  "\"not\": {\"items\":{ \"minLength\": 9 }}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, unnested_not_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < UNNESTED_NOT_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, unnested_not_validator_ans[i]);
  }
}

# define UNNESTED_DEP_SCHEMA_VALIDATOR_COUNT 6
ObString unnested_dep_schema_validator_text = {
  "[{\"name\":\"test_name\", \"last_name\":\"test_last_name\", \"credit_card\": 123, \"billing_address\": \"addstr\"},"
  "[],"
  "{\"name\":\"test_name\", \"last_name\":\"test_last_name\", \"billing_address\": \"add_str\"},"
  "{\"name\":\"test_name\", \"billing_address\": \"add_str\"},"
  "{\"name\":\"test_name\", \"last_name\":\"test_last_name\", \"credit_card\": 123},"
  "{\"name\":\"test_name\", \"last_name\":\"test_last_name\", \"credit_card\": 123, \"billing_address\": 123}]"
};
bool unnested_dep_schema_validator_ans[UNNESTED_DEP_SCHEMA_VALIDATOR_COUNT] = {true, false, true, false, true, false};
TEST_F(TestJsonSchema, test_unnested_dep_schema_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {"
    "\"name\": { \"type\": \"string\" },"
    "\"credit_card\": { \"type\": \"number\" },"
    "\"billing_address\": { \"type\": \"string\" }},"
  "\"required\": [\"name\"],"
  "\"dependencies\": {"
    "\"credit_card\": {"
      "\"properties\": {"
        "\"billing_address\": {  \"maxLength\": 6  }}},"
    "\"name\": [\"last_name\"]}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, unnested_dep_schema_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < UNNESTED_DEP_SCHEMA_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, unnested_dep_schema_validator_ans[i]);
  }
}

# define COMPLEX_VALIDATOR_COUNT 3
ObString complex_validator_text = {
  "[{\"street_address\": {\"k2\": \"Pennsylvania Avenue NW\"}, \"city\": \"Washington\",\"state\": \"DC\",\"type\": \"business\"},"
  "{\"street_address\": {\"k2\": \"Pennsylvania Avenue NW\"}, \"city\": \"Washington\",\"state\": \"DC\",\"type\": \"business\", \"test1\": \"test\", \"test2\": \"test\", \"test3\": \"test\"},"
  "{\"street_address\": {\"k2\": \"Pennsylvania Avenue NW\"}, \"city\": \"Washington\",\"state\": \"DC\",\"type\": \"business\", \"test_pro\": \"test\"}"
  "]"
};
bool complex_validator_ans[COMPLEX_VALIDATOR_COUNT] = {true, true, false};
TEST_F(TestJsonSchema, test_complex_schema_validator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"type\": \"object\","
  "\"properties\": {\"street_address\": "
  "{ \"type\": \"object\", "
  "\"properties\": {\"k2\":{\"type\": \"string\"}}}},"
  "\"patternProperties\": {\"^city*\": { \"type\": \"string\" }},"
  "\"additionalProperties\": { \"type\": \"string\" },"
  "\"required\": [\"street_address\"],"
  "\"allOf\": ["
  "{\"type\":\"object\",\"require\":[\"state\"]},"
  "{\"properties\": {\"state\": { \"type\": \"string\"} }, \"additionalProperties\": { \"maxProperties\": 6}},"
  "{ \"oneOf\": ["
    "{\"required\":[\"test_pro\"]},"
    "{\"allOf\":[{\"required\":[\"state\"]}]}"
  "]}],"
  "\"anyOf\": ["
  "{\"type\":\"object\"},"
  "{\"type\": \"object\","
  "\"properties\": {\"city\": { \"type\": \"string\" },\"state\": { \"type\": \"string\" }},"
    "\"patternProperties\": {\"^[cs]*\": { \"type\": \"string\" }}},"
    "{ \"oneOf\": [{\"type\": \"number\"},{\"maximum\":10},"
    "{\"type\": \"object\",\"properties\": {\"street_address\":{\"type\": \"string\"}}}]}]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, complex_validator_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < json_schema.schema_map_->element_count(); ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    //cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
  }
  for (int i = 0; i < COMPLEX_VALIDATOR_COUNT; ++i) {
    ObIJsonBase *j_value = NULL;
    bool is_valid = false;
    ASSERT_EQ(OB_SUCCESS, j_doc->get_array_element(i, j_value));
    ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_value, is_valid));
    //cout<<i<<":"<<is_valid<<endl;
    ASSERT_EQ(is_valid, complex_validator_ans[i]);
  }
}

TEST_F(TestJsonSchema, test_complex_schema2)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonSeekResult hit;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("{\"definitions\": {\"item\": {\"type\": \"array\",\"additionalItems\": false,"
                "\"items\": [{ \"$ref\": \"#/definitions/sub-item\" },{ \"$ref\": \"#/definitions/sub-item\" }]},"
                "\"sub-item\": {\"type\": \"object\",\"required\": [\"foo\"]}},"
            "\"type\": \"array\",\"additionalItems\": false,"
            "\"items\": [{ \"$ref\": \"#/\" },{ \"$ref\": \"#/definitions/item\" },"
           " { \"$ref\": \"#/definitions/item\" }]}");
  ObString j_doc_txt("[[ {\"foo\": null}, {\"foo\": null} ]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree, ObJsonParser::JSN_SCHEMA_FLAG));
  ObJsonSchemaTree json_schema(&allocator);
  ASSERT_EQ(OB_SUCCESS, json_schema.build_schema_tree(j_tree));
  ObJsonSchemaValidator schema_validator(&allocator, json_schema.schema_map_);
  ObIJsonBase *j_doc = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_doc_txt,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_doc, 0));
  for (int i = 0; i < json_schema.schema_map_->element_count(); ++i) {
    ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, json_schema.schema_map_->get_array_element(i, value));
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, value->print(j_buf, false));
    //cout<<"json_schema "<<i<<" : "<<j_buf.ptr()<<endl;
  }
  bool is_valid = false;
  ASSERT_EQ(OB_SUCCESS, schema_validator.schema_validator(j_doc, is_valid));
  ASSERT_EQ(true, is_valid);
}

# define REGEX_COUNT 6
ObString regex_pattern_str[REGEX_COUNT] = {
  "^S_",
  "^I_",
  "^[SI]*",
  "^[+-]?\\d{1,3}\\.?\\d?$",
  "f.o",
  /*"^*"*/"("
};
ObString regex_valid_str[REGEX_COUNT] = {
  "S_25",
  "I_0",
  "I_0",
  "+0.1",
  "foo",
  "S_25"
};
TEST_F(TestJsonSchema, test_regex_match)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer j_buf(&allocator);
  for(int i = 0; i < REGEX_COUNT; ++i) {
    bool is_valid = false;
    ObJsonSchemaUtils::is_valid_pattern(regex_pattern_str[i], j_buf, is_valid);
    if (is_valid) {
      ObJsonSchemaUtils::if_regex_match(regex_valid_str[i], regex_pattern_str[i], j_buf, is_valid);
      //cout<<"pattern "<<i<<" : "<<is_valid<<endl;
      ASSERT_EQ(is_valid, true);
    } else {
      ASSERT_EQ(i, REGEX_COUNT-1);
    }
  }
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_json_schema.log");
  OB_LOGGER.set_file_name("test_json_schema.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}
