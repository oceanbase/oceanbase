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

#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
class TestAvroLib : public ::testing::Test
{
public:
  TestAvroLib() = default;
  ~TestAvroLib() = default;
};

TEST_F(TestAvroLib, demo)
{
  const char PERSON_SCHEMA[]
      = R"({"type":"record", "name":"Person", "fields":[{"name": "ID", "type": "long"}, {"name": "First", "type": "string"}]})";
  ::avro::ValidSchema schema = ::avro::compileJsonSchemaFromString(PERSON_SCHEMA);
  ASSERT_EQ(avro::Type::AVRO_RECORD, schema.root().get()->type());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}