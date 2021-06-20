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
#include "storage/blocksstable/ob_column_map.h"
#include "lib/random/ob_random.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/config/ob_server_config.h"
namespace oceanbase {
using namespace blocksstable;
using namespace common;
namespace unittest {
class TestColumnMap : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
  static void SetUpTestCase()
  {}
  static void TearDownTestCase()
  {}

  void test_init(const int result, const int64_t schema_version, const int64_t schema_rowkey_count,
      const int64_t store_count, const ObIArray<share::schema::ObColDesc>& out_cols,
      const share::schema::ColumnMap* cols_id_map = nullptr, const ObIArray<int32_t>* projector = nullptr,
      const bool is_multi_version = false);

public:
  ObArenaAllocator allocator_;
};

void TestColumnMap::test_init(const int result, const int64_t schema_version, const int64_t schema_rowkey_count,
    const int64_t store_count, const ObIArray<share::schema::ObColDesc>& out_cols,
    const share::schema::ColumnMap* cols_id_map, const ObIArray<int32_t>* projector, const bool is_multi_version)
{
  int ret = OB_SUCCESS;
  ObColumnMap map;
  ret = map.init(
      allocator_, schema_version, schema_rowkey_count, store_count, out_cols, cols_id_map, projector, is_multi_version);
  ASSERT_EQ(result, ret);
  if (OB_SUCC(ret)) {
    ASSERT_EQ(out_cols.count(), map.get_request_count());
    ASSERT_EQ(store_count, map.get_store_count());
    ASSERT_EQ(schema_rowkey_count, map.get_rowkey_store_count());
    ASSERT_TRUE(map.is_valid());
  }
}

TEST_F(TestColumnMap, init_invalid)
{
  ObArray<share::schema::ObColDesc> out_cols;
  test_init(OB_INVALID_ARGUMENT, -1, 2, 10, out_cols);
  test_init(OB_INVALID_ARGUMENT, 1, 0, 10, out_cols);
  test_init(OB_INVALID_ARGUMENT, 1, 2, -1, out_cols);
  test_init(OB_INVALID_ARGUMENT, 1, 2, 10, out_cols);
  share::schema::ObColDesc col_desc;
  col_desc.col_id_ = 1 + OB_APP_MIN_COLUMN_ID;
  col_desc.col_type_.set_type(static_cast<ObObjType>(ObInt32Type));
  ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  test_init(OB_SUCCESS, 1, 2, 10, out_cols);
  col_desc.col_id_ = 2 + OB_APP_MIN_COLUMN_ID;
  ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  test_init(OB_SUCCESS, 1, 2, 10, out_cols);
  test_init(OB_SUCCESS, 1, 2, 10, out_cols, nullptr, nullptr, true);
  test_init(OB_SUCCESS, 1, 2, 10, out_cols, nullptr, nullptr, false);
  test_init(OB_INVALID_ARGUMENT, 1, 2, 1, out_cols);
  test_init(OB_INVALID_ARGUMENT, 1, 2, OB_ROW_MAX_COLUMNS_COUNT + 1, out_cols);
  out_cols.reuse();
  for (int64_t i = 0; i < OB_ROW_MAX_COLUMNS_COUNT + 1; ++i) {
    col_desc.col_id_ = i + OB_APP_MIN_COLUMN_ID;
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  }
  test_init(OB_INVALID_ARGUMENT, 1, 2, 10, out_cols);

  out_cols.reuse();
  col_desc.col_id_ = 7;
  ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  col_desc.col_id_ = 8;
  ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  ObArray<int32_t> projector;
  ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(1));
  test_init(OB_SUCCESS, 1, 2, 10, out_cols, nullptr, &projector);
  projector.reuse();
  ASSERT_EQ(OB_SUCCESS, projector.push_back(-1));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(-2));
  test_init(OB_SUCCESS, 1, 2, 10, out_cols, nullptr, &projector);
  projector.reuse();
  ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(10));
  test_init(OB_INVALID_ARGUMENT, 1, 2, 10, out_cols, nullptr, &projector);
  test_init(OB_SUCCESS, 1, 2, 0, out_cols, nullptr, &projector);
  projector.reuse();
  ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(OB_ROW_MAX_COLUMNS_COUNT));
  test_init(OB_INVALID_ARGUMENT, 1, 2, 0, out_cols, nullptr, &projector);
}

TEST_F(TestColumnMap, init_cols)
{
  ObColumnMap map;
  ObArray<share::schema::ObColDesc> out_cols;
  const int64_t schema_version = 1;
  const int64_t rowkey_count = 2;
  const int64_t column_count = ObHexStringType;
  for (int64_t i = 0; i < column_count; ++i) {
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(static_cast<ObObjType>(i));
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col));
  }
  ASSERT_EQ(OB_SUCCESS, map.init(allocator_, schema_version, rowkey_count, column_count, out_cols));
  ASSERT_EQ(out_cols.count(), map.get_request_count());
  ASSERT_EQ(rowkey_count, map.get_rowkey_store_count());
  ASSERT_EQ(out_cols.count(), map.get_seq_read_column_count());
  const ObColumnIndexItem* column_indexs = map.get_column_indexs();
  for (int64_t i = 0; i < map.get_request_count(); ++i) {
    ASSERT_EQ(column_indexs[i].column_id_, out_cols[i].col_id_);
    ASSERT_EQ(column_indexs[i].store_index_, i);
    ASSERT_EQ(column_indexs[i].request_column_type_, out_cols[i].col_type_);
    ASSERT_EQ(column_indexs[i].is_column_type_matched_, true);
  }
}

TEST_F(TestColumnMap, init_projector)
{
  ObColumnMap map;
  ObArray<share::schema::ObColDesc> out_cols;
  const int64_t schema_version = 1;
  const int64_t rowkey_count = 2;
  const int64_t column_count = ObHexStringType;
  for (int64_t i = 0; i < column_count; ++i) {
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(static_cast<ObObjType>(i));
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col));
  }

  ObArray<int32_t> projector;
  for (int64_t i = 0; i < column_count; ++i) {
    const int64_t store_index = ObRandom::rand(0, column_count - 1);
    ASSERT_EQ(OB_SUCCESS, projector.push_back(static_cast<int32_t>(store_index)));
  }
  ASSERT_EQ(
      OB_SUCCESS, map.init(allocator_, schema_version, rowkey_count, column_count, out_cols, nullptr, &projector));
  ASSERT_EQ(out_cols.count(), map.get_request_count());
  ASSERT_EQ(rowkey_count, map.get_rowkey_store_count());
  const ObColumnIndexItem* column_indexs = map.get_column_indexs();
  for (int64_t i = 0; i < map.get_request_count(); ++i) {
    ASSERT_EQ(column_indexs[i].column_id_, out_cols[i].col_id_);
    ASSERT_EQ(column_indexs[i].store_index_, projector[i]);
    ASSERT_EQ(column_indexs[i].request_column_type_, out_cols[i].col_type_);
    ASSERT_EQ(column_indexs[i].is_column_type_matched_, true);
  }
}

TEST_F(TestColumnMap, init_multi_version)
{
  ObColumnMap map;

  // test sequence columns with multi version
  ObArray<share::schema::ObColDesc> out_cols;
  const int64_t schema_version = 1;
  const int64_t rowkey_count = 2;
  const int64_t column_count = ObHexStringType;
  for (int64_t i = 0; i < column_count; ++i) {
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(static_cast<ObObjType>(i));
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col));
  }
  int extra_col_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const ObColumnIndexItem* column_indexs = nullptr;

  // test projector with multi version
  map.reset();
  ObArray<int32_t> projector;
  for (int64_t i = 0; i < column_count; ++i) {
    const int64_t store_index = ObRandom::rand(0, column_count - 1);
    ASSERT_EQ(OB_SUCCESS, projector.push_back(static_cast<int32_t>(store_index)));
  }
  ASSERT_EQ(OB_SUCCESS,
      map.init(allocator_, schema_version, rowkey_count, column_count, out_cols, nullptr, &projector, extra_col_cnt));
  ASSERT_EQ(out_cols.count(), map.get_request_count());
  column_indexs = map.get_column_indexs();
  for (int64_t i = 0; i < map.get_request_count(); ++i) {
    ASSERT_EQ(column_indexs[i].column_id_, out_cols[i].col_id_);
    if (projector[i] < rowkey_count) {
      ASSERT_EQ(column_indexs[i].store_index_, projector[i]);
    } else {
      ASSERT_EQ(column_indexs[i].store_index_, projector[i] + extra_col_cnt);
    }
    ASSERT_EQ(column_indexs[i].request_column_type_, out_cols[i].col_type_);
    ASSERT_EQ(column_indexs[i].is_column_type_matched_, true);
  }
}

TEST_F(TestColumnMap, test_column_hash)
{
  int64_t column_cnt = 5;
  uint16_t column_ids[5] = {16, 17, 18, 19, 30};
  ObColumnHashSet column_hash;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, column_hash.init(column_cnt, column_ids, allocator));
  int64_t idx = -1;
  for (int64_t i = 0; i < column_cnt; ++i) {
    column_hash.get_index(column_ids[i], idx);
    ASSERT_EQ(i, idx);
  }
  column_hash.get_index(20, idx);
  ASSERT_EQ(-1, idx);

  column_cnt = 4;
  column_ids[0] = 16;
  column_ids[1] = 32;
  column_ids[2] = 48;
  column_ids[3] = 24;
  ASSERT_EQ(OB_SUCCESS, column_hash.init(column_cnt, column_ids, allocator));
  idx = -1;
  for (int64_t i = 0; i < column_cnt; ++i) {
    column_hash.get_index(column_ids[i], idx);
    ASSERT_EQ(i, idx);
  }
  column_hash.get_index(20, idx);
  ASSERT_EQ(-1, idx);
  ASSERT_EQ(13, column_hash.shift_);
  for (int64_t i = 0; i <= 8; ++i) {
    STORAGE_LOG(INFO, "column hash bucket", K(i), K(column_hash.bucket_[i]));
  }
  for (int64_t i = 0; i < column_cnt; ++i) {
    STORAGE_LOG(INFO, "column hash chain", K(i), K(column_hash.chain_[i]));
  }
}

TEST_F(TestColumnMap, rebuild_perf)
{
  ObColumnMap map;
  ObArray<share::schema::ObColDesc> out_cols;
  const int64_t schema_version = 1;
  const int64_t rowkey_count = 1;
  const int64_t column_count = ObHexStringType;
  for (int64_t i = 0; i < column_count; i += 2) {
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(static_cast<ObObjType>(i));
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col));
  }

  ObArray<int32_t> projector;
  for (int64_t i = 0; i < column_count; i += 2) {
    ASSERT_EQ(OB_SUCCESS, projector.push_back(i));
  }
  ASSERT_EQ(
      OB_SUCCESS, map.init(allocator_, schema_version, rowkey_count, column_count, out_cols, nullptr, &projector));
  ObMacroBlockMetaV2 macro_meta;
  ObMacroBlockSchemaInfo macro_schema;
  ObFullMacroBlockMeta full_meta;
  full_meta.meta_ = &macro_meta;
  full_meta.schema_ = &macro_schema;
  macro_meta.schema_version_ = 1;
  macro_meta.column_number_ = column_count;
  macro_schema.schema_version_ = 1;
  macro_schema.column_number_ = column_count;
  uint16_t column_ids[column_count];
  ObObjMeta column_types[column_count];
  for (int64_t i = 0; i < column_count; ++i) {
    column_ids[i] = static_cast<uint16_t>(i + OB_APP_MIN_COLUMN_ID);
    column_types[i].set_type(static_cast<ObObjType>(i));
  }
  macro_schema.column_id_array_ = column_ids;
  macro_schema.column_type_array_ = column_types;

  const int64_t start_time = ObTimeUtility::current_time();
  for (int64_t i = 0; i < 10000000; ++i) {
    map.rebuild(full_meta, true);
  }
  const int64_t elapsed_time = ObTimeUtility::current_time() - start_time;
  STORAGE_LOG(INFO, "rebuild perf cost", K(elapsed_time));
  ASSERT_EQ(out_cols.count(), map.get_request_count());
  ASSERT_EQ(rowkey_count, map.get_rowkey_store_count());
  const ObColumnIndexItem* column_indexs = map.get_column_indexs();
  for (int64_t i = 0; i < map.get_request_count(); ++i) {
    ASSERT_EQ(column_indexs[i].column_id_, out_cols[i].col_id_);
    ASSERT_EQ(column_indexs[i].store_index_, projector[i]);
    ASSERT_EQ(column_indexs[i].request_column_type_, out_cols[i].col_type_);
    ASSERT_EQ(column_indexs[i].is_column_type_matched_, true);
  }
  int filled_cnt = 0;
  int64_t cnt = 1 << (16 - map.column_hash_.shift_);
  for (int64_t i = 0; i < cnt; ++i) {
    if (map.column_hash_.bucket_[i] != -1) {
      filled_cnt++;
    }
    STORAGE_LOG(INFO, "bucket content", K(i), K(map.column_hash_.bucket_[i]));
  }
  STORAGE_LOG(INFO, "bucket stat", K(filled_cnt), K(column_count), K(cnt));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = false;
  system("rm -rf test_column_map.log");
  OB_LOGGER.set_file_name("test_column_map.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
