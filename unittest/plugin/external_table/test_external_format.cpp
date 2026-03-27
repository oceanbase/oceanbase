/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX SHARE

#define private public
#include "plugin/external_table/ob_external_format.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::plugin;
using namespace oceanbase::json;

TEST(TestExternalFormat, test_json)
{
  ObMalloc allocator(ObMemAttr(OB_SERVER_TENANT_ID, "unittest"));
  ObPluginFormat format;
  ASSERT_EQ(OB_SUCCESS, format.init(allocator, false /*encrypt mode */));
  ASSERT_EQ(OB_SUCCESS, format.set_type_name("memory"));

  const char *parameters_str = R"(
     {
       "table":"t_external",
       "user":"root",
       "jdbc_url":"jdbc:mysql://100.88.121.134:3306/test?useSSL=false&connectionTimeZone=UTC",
       "object": {
         "key1":"value1",
         "key2":"value2"
       },
       "array": ["object1", "object2"]
     }
)";

  ASSERT_EQ(OB_SUCCESS, format.set_parameters(parameters_str));

  const int buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  int64_t json_len = format.to_json_string(buf + pos, buf_len - pos);
  ASSERT_TRUE(json_len > 0);
  pos += json_len;
  databuff_printf(buf, buf_len, pos, "}");
  LOG_INFO("property value json string", K(buf));

  json::Value *json_root = nullptr;
  json::Parser json_parser;
  ObArenaAllocator arena_allocator;
  ASSERT_EQ(OB_SUCCESS, json_parser.init(&arena_allocator));
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(buf, pos, json_root));
  ASSERT_NE(json_root, nullptr);

  ObPluginFormat format2;
  ASSERT_EQ(OB_SUCCESS, format2.init(allocator, false /*encrypt*/));
  ASSERT_EQ(json_root->get_type(), JT_OBJECT);
  ASSERT_EQ(OB_SUCCESS, format2.load_from_json_node(json_root->get_object().get_first()));
  ASSERT_TRUE(0 == format.type_name().compare(format2.type_name()));
  ASSERT_EQ(0, format2.parameters().compare(format.parameters()));
}

TEST(TestExternalFormat, test_empty_properties)
{
  ObMalloc allocator(ObMemAttr(OB_SERVER_TENANT_ID, "unittest"));

  ObPluginFormat format;
  ASSERT_EQ(OB_SUCCESS, format.init(allocator, false/*encrypt*/));
  ASSERT_EQ(OB_SUCCESS, format.set_type_name("memory"));

  const int buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  int64_t json_len = format.to_json_string(buf + pos, buf_len - pos);
  ASSERT_TRUE(json_len > 0);
  pos += json_len;
  databuff_printf(buf, buf_len, pos, "}");
  LOG_INFO("property value json string", K(buf));

  json::Value *json_root = nullptr;
  json::Parser json_parser;
  ObArenaAllocator arena_allocator;
  ASSERT_EQ(OB_SUCCESS, json_parser.init(&arena_allocator));
  ASSERT_EQ(OB_SUCCESS, json_parser.parse(buf, pos, json_root));
  ASSERT_NE(json_root, nullptr);

  ObPluginFormat format2;
  ASSERT_EQ(OB_SUCCESS, format2.init(allocator, false/*encrypt*/));
  ASSERT_EQ(json_root->get_type(), JT_OBJECT);
  ASSERT_EQ(OB_SUCCESS, format2.load_from_json_node(json_root->get_object().get_first()));
  ASSERT_TRUE(0 == format.type_name().compare(format2.type_name()));
  ASSERT_EQ(0, format2.parameters().compare(format.parameters()));
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
