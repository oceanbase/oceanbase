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

#define USING_LOG_PREFIX SHARE

#define private public
#include "plugin/share/ob_properties.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::plugin;
using namespace oceanbase::json;

TEST(TestPropertyValue, test_property_value_json)
{
  ObProperties::PropertyValue property_value;
  property_value.sensitive = false;
  property_value.decrypted_value = "abcd";

  const int buf_len = 1024;
  char buf[buf_len];
  int64_t json_len = property_value.to_json_string(buf, buf_len);
  ASSERT_TRUE(json_len > 0);
  LOG_INFO("property value json string", K(buf));

  ObMalloc allocator;
  json::Value *json_root = nullptr;
  json::Parser json_parser;
  ObArenaAllocator arena_allocator;
  ASSERT_EQ(OB_SUCCESS, json_parser.init(&arena_allocator));
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(buf, json_len, json_root));
  ASSERT_NE(json_root, nullptr);

  ObProperties::PropertyValue property_value2;
  ASSERT_EQ(OB_SUCCESS, property_value2.from_json(allocator, json_root));
  ASSERT_EQ(property_value.sensitive, property_value2.sensitive);
  ASSERT_EQ(property_value.encrypted_value, property_value2.encrypted_value);
  ASSERT_EQ(property_value.decrypted_value, property_value2.decrypted_value);

  snprintf(buf, buf_len, "{\"value\":\"oceanbase\"}");
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(buf, strlen(buf), json_root));
  ASSERT_NE(json_root, nullptr);
  ObProperties::PropertyValue property_value3;
  ASSERT_EQ(OB_SUCCESS, property_value3.from_json(allocator, json_root));
  ASSERT_EQ(property_value3.sensitive, false);
  ASSERT_EQ(property_value3.encrypted_value, "");
  ASSERT_EQ(property_value3.decrypted_value, "oceanbase");

  snprintf(buf, buf_len, "{\"value\":\"oceanbase\", \"sensitive\":true}");
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(buf, strlen(buf), json_root));
  ASSERT_NE(json_root, nullptr);
  ObProperties::PropertyValue property_value4;
  ASSERT_EQ(OB_SUCCESS, property_value4.from_json(allocator, json_root));
  ASSERT_EQ(property_value4.sensitive, true);
  ASSERT_EQ(property_value4.encrypted_value, "oceanbase");
  ASSERT_EQ(property_value4.decrypted_value, "");

  snprintf(buf, buf_len, "{\"sensitive\":true, \"value\":\"oceanbase\"}");
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(buf, strlen(buf), json_root));
  ASSERT_NE(json_root, nullptr);
  ASSERT_EQ(OB_SUCCESS, property_value4.from_json(allocator, json_root));
  ASSERT_EQ(property_value4.sensitive, true);
  ASSERT_EQ(property_value4.encrypted_value, "oceanbase");
  ASSERT_EQ(property_value4.decrypted_value, "");
}

TEST(TestObProperties, test_properties_json)
{
  ObProperties properties;
  ObMalloc allocator;
  std::pair<const char *, const char *> kvs[] = {
    {"host", "127.0.0.1"},
    {"port", "3306"},
    {"database", "test"},
    {"table", "test_table"},
    {"user", "root"},
    {"password", "123456"}
  };
  ASSERT_EQ(OB_SUCCESS, properties.init(allocator, false/*encrypt_mode*/));
  for (size_t i = 0; i < sizeof(kvs)/sizeof(kvs[0]); i++) {
    const auto &kv = kvs[i];
    ASSERT_EQ(OB_SUCCESS, properties.set_property(kv.first, kv.second));
  }
  ASSERT_EQ(OB_SUCCESS, properties.mark_sensitive("password"));

  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t json_len = properties.to_json_string(buf, buf_len);
  ASSERT_TRUE(json_len > 0);
  LOG_INFO("properties after json", KCSTRING(buf));

  // test from json
  json::Value *json_root = nullptr;
  json::Parser json_parser;
  ObArenaAllocator arena_allocator;
  ASSERT_EQ(OB_SUCCESS, json_parser.init(&arena_allocator));
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(buf, json_len, json_root));
  ASSERT_NE(json_root, nullptr);
  ASSERT_EQ(json_root->get_type(), JT_OBJECT);

  ObProperties properties2;
  ASSERT_EQ(OB_SUCCESS, properties2.init(allocator, false/*encrypt_mode*/));
  ASSERT_EQ(OB_SUCCESS, properties2.load_from_json_node(json_root));
  bool compare_result = false;
  ASSERT_EQ(OB_SUCCESS, properties2.equal_to(properties, compare_result));
  ASSERT_EQ(true, compare_result);
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
