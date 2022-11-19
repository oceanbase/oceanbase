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
#include "lib/json/ob_json.h"
#include "lib/allocator/ob_mod_define.h"

using namespace oceanbase::common;
using namespace oceanbase::json;
//using namespace oceanbase::sql;

class TestJsonFormat : public ::testing::Test
{
public:
  TestJsonFormat() {};
  virtual ~TestJsonFormat() {};
  virtual void SetUp() {};
  virtual void TearDown() {};
private:
  // disallow copy and assign
  TestJsonFormat(const TestJsonFormat &other);
  TestJsonFormat& operator=(const TestJsonFormat &ohter);
};

TEST_F(TestJsonFormat, basic)
{
  char json[] = "{\
  \"main_query\":  [{\
    \"PHY_PROJECT\":  { },\
    \"PHY_INDEX_SELECTOR\":  {\
      \"USE_INDEX\": \"test\" },\
            \"PHY_TABLE_RPC_SCAN_IMPL\":  {\
              \"PHY_RPC_SCAN\":  {\
                \"scan_plan\":  {\
                  \"plan\":  {\
                    \"plan\":  {\
                      \"main_query\":  {\
                        \"PHY_PROJECT\":  { },\
                        \"PHY_TABLET_SCAN_V2\":  { } } } } } } } }] }";
  int32_t length = static_cast<int32_t>(strlen(json));

  ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
  Parser parser;
  parser.init(&allocator, NULL);
  Value *root = NULL;
  parser.parse(json, length, root);
  Tidy tidy(root);
  char output_buf[OB_MAX_LOG_BUFFER_SIZE];
  output_buf[0] = '\n';
  int64_t pos = tidy.to_string(output_buf + 1, sizeof(output_buf));
  output_buf[pos + 1] = '\n';
  _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos + 2), output_buf);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
