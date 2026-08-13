/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "share/vector_index/ob_vector_index_util.h"
#undef private
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
using namespace common;
namespace share
{

static int build_varchar_pk_schema(const int64_t declared_chars,
                                   schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  schema::ObColumnSchemaV2 column;
  const uint64_t table_id = 50000;
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_table_id(table_id);
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  column.set_tenant_id(OB_SYS_TENANT_ID);
  column.set_table_id(table_id);
  column.set_column_id(OB_APP_MIN_COLUMN_ID);
  column.set_column_name(ObString::make_string("pk"));
  column.set_data_type(ObVarcharType);
  column.set_charset_type(CHARSET_UTF8MB4);
  column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  column.set_data_length(declared_chars);
  column.set_rowkey_position(1);
  column.set_order_in_rowkey(ASC);
  column.set_nullable(false);
  if (OB_FAIL(table_schema.add_column(column))) {
    LOG_WARN("failed to add varchar primary key", K(ret));
  }
  return ret;
}

TEST(TestVectorExtraInfo, utf8mb4_schema_uses_max_byte_length)
{
  schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_varchar_pk_schema(4, table_schema));
  int64_t actual_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObVectorIndexUtil::check_extra_info_size(
                table_schema, nullptr, true, 1, actual_size));
  ASSERT_EQ(21, actual_size);
}

TEST(TestVectorExtraInfo, utf8mb4_schema_without_table_id_uses_tenant_mode)
{
  schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_varchar_pk_schema(4, table_schema));
  table_schema.set_table_id(OB_INVALID_ID);
  int64_t actual_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObVectorIndexUtil::check_extra_info_size(
                table_schema, nullptr, true, 1, actual_size));
  ASSERT_EQ(21, actual_size);
}

static ObVecExtraInfoObj make_string_extra_info(const char *ptr, const int32_t len)
{
  ObVecExtraInfoObj obj;
  obj.ptr_ = ptr;
  obj.len_ = len;
  obj.obj_map_type_ = ObObjDatumMapType::OBJ_DATUM_STRING;
  return obj;
}

TEST(TestVectorExtraInfo, exact_slot_succeeds)
{
  const char emoji[] = "\xF0\x9F\x98\x80";
  ObVecExtraInfoObj obj = make_string_extra_info(emoji, 4);
  char buf[9] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObVecExtraInfo::extra_info_to_buf(&obj, 1, buf, sizeof(buf), pos));
  ASSERT_EQ(sizeof(buf), pos);
}

TEST(TestVectorExtraInfo, short_slot_returns_overflow_without_oob_write)
{
  const char emoji[] = "\xF0\x9F\x98\x80";
  ObVecExtraInfoObj obj = make_string_extra_info(emoji, 4);
  const int64_t slot_size = 8;
  unsigned char guarded[slot_size + 8];
  MEMSET(guarded, 0, sizeof(guarded));
  MEMSET(guarded + slot_size, 0xA5, 8);
  int64_t pos = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObVecExtraInfo::extra_info_to_buf(
                &obj, 1, reinterpret_cast<char *>(guarded), slot_size, pos));
  for (int64_t i = slot_size; i < static_cast<int64_t>(sizeof(guarded)); ++i) {
    ASSERT_EQ(0xA5, guarded[i]);
  }
}

TEST(TestVectorExtraInfo, negative_length_returns_overflow)
{
  const char value[] = "x";
  ObVecExtraInfoObj obj = make_string_extra_info(value, -1);
  char buf[32] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObVecExtraInfo::extra_info_to_buf(&obj, 1, buf, sizeof(buf), pos));
}

TEST(TestVectorExtraInfo, composite_string_fields_fit_exact_slot)
{
  const char chinese[] = "\xE4\xB8\xAD\xE6\x96\x87";
  const char emoji[] = "\xF0\x9F\x98\x80";
  ObVecExtraInfoObj objs[2] = {
      make_string_extra_info(chinese, 6),
      make_string_extra_info(emoji, 4)};
  char buf[19] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObVecExtraInfo::extra_info_to_buf(objs, 2, buf, sizeof(buf), pos));
  ASSERT_EQ(sizeof(buf), pos);
}

TEST(TestVectorExtraInfo, batch_rejects_row_larger_than_slot)
{
  ObArenaAllocator allocator("VecExtraTest");
  const char emoji[] = "\xF0\x9F\x98\x80";
  ObVecExtraInfoObj obj = make_string_extra_info(emoji, 4);
  char *buf = reinterpret_cast<char *>(0x1);
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObVecExtraInfo::extra_infos_to_buf(
                allocator, &obj, 1, 8, 1, buf));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
