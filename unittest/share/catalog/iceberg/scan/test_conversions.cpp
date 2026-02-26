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
#include "sql/table_format/iceberg/scan/conversions.h"
#include "sql/table_format/iceberg/spec/schema.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace sql::iceberg;
class TestIcebergConversions : public ::testing::Test
{
public:
  TestIcebergConversions() = default;
  ~TestIcebergConversions() = default;
  ObArenaAllocator allocator;
};

TEST_F(TestIcebergConversions, test_bool_true)
{
  std::vector<uint8_t> data{0x01};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_BOOLEAN, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_bool(true);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_bool_false)
{
  std::vector<uint8_t> data{0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_BOOLEAN, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_bool(false);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_int)
{
  std::vector<uint8_t> data{0x0A, 0x00, 0x00, 0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_INT, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_int32(10);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_int_2_long)
{
  std::vector<uint8_t> data{0xF4, 0x01, 0x00, 0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_LONG, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_int(500);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_long)
{
  std::vector<uint8_t> data{0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_LONG, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_int(500);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_float)
{
  std::vector<uint8_t> data{0xA4, 0x70, 0x9D, 0x3F};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_FLOAT, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_float(1.23);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_float_2_double)
{
  std::vector<uint8_t> data{0xA4, 0x70, 0x9D, 0x3F};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_DOUBLE, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_double(1.23f);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_double)
{
  std::vector<uint8_t> data{0xB2, 0x9D, 0xEF, 0xA7, 0xC6, 0x4B, 0xF7, 0x3F};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_DOUBLE, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_double(1.456);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_date)
{
  std::vector<uint8_t> data{0xBD, 0x4B, 0x00, 0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_DATE, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  // 2023-02-01
  result_obj.set_date(19389);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_time)
{
  std::vector<uint8_t> data{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_TIME, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  // 00:00.000001
  result_obj.set_time(1);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_timestamp)
{
  std::vector<uint8_t> data{0x40, 0x52, 0x0A, 0x80, 0x6F, 0xB5, 0x04, 0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_TIMESTAMP, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  // 2012-01-01 12:00:01
  result_obj.set_datetime(1325390401000000);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_timestamp_tz)
{
  std::vector<uint8_t> data{0x40, 0x52, 0x0A, 0x80, 0x6F, 0xB5, 0x04, 0x00};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_TIMESTAMP_TZ, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  // 2012-01-02 12:00:01
  result_obj.set_timestamp(1325390401000000);
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_string)
{
  std::vector<uint8_t> data{0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_STRING, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  result_obj.set_varchar("hello world");
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_binary)
{
  std::vector<uint8_t> data{0x01, 0x23, 0x45, 0x6F};
  ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
  schema::ObColumnSchemaV2 column_schema;
  Schema::set_column_schema_type_by_type_str(Schema::TYPE_BINARY, column_schema);
  ObObj obj;
  ASSERT_EQ(
      OB_SUCCESS,
      Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
  ObObj result_obj;
  result_obj.set_collation_type(ObCollationType::CS_TYPE_BINARY);
  result_obj.set_binary(ObString(data.size(), reinterpret_cast<const char *>(data.data())));
  ASSERT_EQ(result_obj, obj);
}

TEST_F(TestIcebergConversions, test_decimal)
{
  {
    // 123.12
    std::vector<uint8_t> data{0x30, 0x18};
    ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
    schema::ObColumnSchemaV2 column_schema;
    Schema::set_column_schema_type_by_type_str(ObString("decimal(9,2)"), column_schema);
    ObObj obj;
    ASSERT_EQ(
        OB_SUCCESS,
        Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
    ObObj result_obj;
    int32_t buffer_size = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(9);
    char *buf = static_cast<char *>(allocator.alloc(buffer_size));
    memset(buf, 0, buffer_size);
    buf[0] = 0x18;
    buf[1] = 0x30;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    result_obj.set_decimal_int(4, 2, decint);
    ASSERT_EQ(result_obj, obj);
  }

  {
    // -14.20
    std::vector<uint8_t> data{0xFA, 0x74};
    ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
    schema::ObColumnSchemaV2 column_schema;
    Schema::set_column_schema_type_by_type_str(ObString("decimal(9,2)"), column_schema);
    ObObj obj;
    ASSERT_EQ(
        OB_SUCCESS,
        Conversions::convert_statistics_binary_to_ob_obj(allocator, binary, column_schema, obj));
    ObObj result_obj;
    int32_t buffer_size = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(9);
    char *buf = static_cast<char *>(allocator.alloc(buffer_size));
    memset(buf, 0, buffer_size);
    buf[0] = 0x74;
    buf[1] = 0xFA;
    buf[2] = 0xFF;
    buf[3] = 0xFF;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    result_obj.set_decimal_int(4, 2, decint);
    ASSERT_EQ(result_obj, obj);
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}