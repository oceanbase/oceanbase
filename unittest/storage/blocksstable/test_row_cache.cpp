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
#include "storage/blocksstable/ob_row_cache.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/ob_i_store.h"
namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
namespace unittest {
class TestRowCache : public ::testing::Test {
public:
  TestRowCache();
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
  static void SetUpTestCase()
  {}
  static void TearDownTestCase()
  {}
  void alloc(char*& ptr, const int64_t size);
  void init(ObStoreRow& row, const int64_t value);
  void init(ObStoreRowkey& rowkey, const int64_t value);
  void init(ObRowCacheValue& cache, const int64_t value);
  ModuleArena& get_arena()
  {
    return arena_;
  }

private:
  ModulePageAllocator alloc_;
  ModuleArena arena_;
};
TestRowCache::TestRowCache() : alloc_(ObModIds::TEST), arena_(ModuleArena::DEFAULT_BIG_PAGE_SIZE, alloc_)
{}
void TestRowCache::alloc(char*& ptr, const int64_t size)
{
  ptr = reinterpret_cast<char*>(arena_.alloc(size));
  ASSERT_TRUE(NULL != ptr);
}
void TestRowCache::init(ObStoreRow& row, const int64_t value)
{
  int64_t size = sizeof(ObObj) * 5;
  row.row_val_.cells_ = reinterpret_cast<ObObj*>(arena_.alloc(size));
  ASSERT_TRUE(NULL != row.row_val_.cells_);
  row.row_val_.count_ = 5;
  ObObj obj;
  row.flag_ = ObActionFlag::OP_ROW_EXIST;
  for (int64_t i = 0; i < 5; ++i) {
    obj.set_int(value + i);
    row.row_val_.cells_[i] = obj;
  }
}
void TestRowCache::init(ObStoreRowkey& rowkey, const int64_t value)
{
  int64_t size = sizeof(ObObj) * 2;
  ObObj* obj_ptr = reinterpret_cast<ObObj*>(arena_.alloc(size));
  ASSERT_TRUE(NULL != obj_ptr);
  for (int64_t i = 0; i < 2; ++i) {
    obj_ptr[i].set_int(value + i);
  }
  rowkey.assign(obj_ptr, 2);
}
void TestRowCache::init(ObRowCacheValue& cache, const int64_t value)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  if (-1 == value) {
    cache.init(full_meta, NULL, MacroBlockId(0));
  } else {
    char* buf = reinterpret_cast<char*>(arena_.alloc(1024));
    ObRowWriter writer;
    ObStoreRow row;
    init(row, value);
    int64_t pos = 0;
    int64_t rowkey_start_pos = 0;
    int64_t rowkey_end_pos = 0;
    ret = writer.write(2, row, buf, 1024, pos, rowkey_start_pos, rowkey_end_pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObMacroBlockMetaV2 macro_meta;
    ObMacroBlockSchemaInfo macro_schema;
    full_meta.meta_ = &macro_meta;
    full_meta.schema_ = &macro_schema;
    macro_meta.column_number_ = 5;
    macro_meta.schema_version_ = 0;
    row.flag_ = 0;
    row.set_dml(storage::T_DML_UNKNOWN);
    ret = cache.init(full_meta, &row, MacroBlockId(0, 1, 0, 2));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}
TEST_F(TestRowCache, normal)
{
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
  int ret = OB_SUCCESS;
  ObRowCache cache;
  const char* name = "row_cache";
  ret = cache.init(name, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  MacroBlockId block_id(1, 0, 10, 2);
  int64_t file_id = 100;
  ObStoreRowkey rowkey;
  ObRowCacheValue cachevalue;
  init(rowkey, 5);
  init(cachevalue, 5);
  ObRowCacheKey key(combine_id(1, 3001), file_id, rowkey, 0, ObITable::MAJOR_SSTABLE);

  // not exist
  ObRowValueHandle handle;
  ret = cache.get_row(key, handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // put row
  ret = cache.put_row(key, cachevalue);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, cache.count(1));

  // get now
  ret = cache.get_row(key, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj* obj_ptr = handle.row_value_->get_obj_array();
  ObObj* obj_ptr1 = cachevalue.get_obj_array();
  for (int i = 0; i < 5; i++) {
    ASSERT_TRUE(obj_ptr[i] == obj_ptr1[i]);
  }

  ObRowCacheKey key1(combine_id(1, 3001), file_id, rowkey, 2, ObITable::MINOR_SSTABLE);
  ObRowValueHandle handle1;
  ObRowCacheValue value;
  init(value, 6);
  ret = cache.get_row(key1, handle1);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  ret = cache.put_row(key1, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, cache.count(1));

  ret = cache.get_row(key1, handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj_ptr = handle1.row_value_->get_obj_array();
  obj_ptr1 = value.get_obj_array();
  for (int i = 0; i < 5; i++) {
    ASSERT_TRUE(obj_ptr[i] == obj_ptr1[i]);
  }

  ObRowCacheKey key2(combine_id(1, 3001), file_id, rowkey, 3, ObITable::MINOR_SSTABLE);
  ObRowValueHandle handle2;
  ret = cache.get_row(key2, handle2);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  ObRowCacheKey key3(combine_id(1, 3002), file_id, rowkey, 0, ObITable::MAJOR_SSTABLE);
  ObRowValueHandle handle3;
  ret = cache.get_row(key3, handle3);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  // ASSERT_EQ(0, memcmp(value.buf_, cachevalue.buf_, cachevalue.size_));
  ObKVGlobalCache::get_instance().destroy();
}
}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
