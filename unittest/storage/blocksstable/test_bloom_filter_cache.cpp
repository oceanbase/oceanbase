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
#include <storage/ob_i_table.h>
#define private public
#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {

TEST(ObBloomFilterCache, test_invalid)
{
  int ret = OB_SUCCESS;
  ObBloomFilterCache bf_cache;
  ObStoreRowkey rowkey;
  bool may_contain = false;
  ObBloomFilterCacheValue bf_value;
  uint64_t table_id = combine_id(1, 3001);
  MacroBlockId block_id(3);
  int64_t file_id = 100;

  ret = bf_cache.may_contain(table_id, block_id, file_id, rowkey, may_contain);
  EXPECT_NE(OB_SUCCESS, ret);
  EXPECT_TRUE(may_contain);

  ret = bf_cache.put_bloom_filter(table_id, block_id, file_id, bf_value, true);
  EXPECT_NE(OB_SUCCESS, ret);

  MacroBlockId macro_block_id;
  ObMacroBlockMetaV2 macro_meta;
  storage::ObITable::TableKey table_key;
  ret = bf_cache.inc_empty_read(table_id, macro_block_id, file_id, macro_meta, rowkey.get_obj_cnt(), table_key);
  EXPECT_NE(OB_SUCCESS, ret);

  bf_cache.destroy();
}

TEST(ObBloomFilterCache, test_normal)
{
  int ret = OB_SUCCESS;
  ObBloomFilterCache bf_cache;
  ObStoreRowkey rowkey;
  bool may_contain = false;
  ObBloomFilterCacheValue bf_value;
  uint64_t table_id = combine_id(1, 3001);
  MacroBlockId block_id(3);
  int64_t file_id = 100;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
  ObMacroBlockMetaMgr::get_instance().init(10000);
  ObMacroBlockMetaV2 macro_meta;

  // test ObMacroBlockMeta bf_flag_
  macro_meta.bf_flag_ = 0;

  // test ObBloomFilterCache may_contain()
  ret = bf_cache.init("bf_cache", 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = bf_value.init(2, 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObObj obj[2];
  obj[0].set_int32(1);
  obj[1].set_int32(2);
  rowkey.assign(obj, 2);
  ret = bf_value.insert(rowkey);
  EXPECT_EQ(OB_SUCCESS, ret);

  macro_meta.rowkey_column_number_ = 2;

  ret = bf_cache.put_bloom_filter(table_id, block_id, file_id, bf_value, true);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = bf_cache.may_contain(table_id, block_id, file_id, rowkey, may_contain);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(may_contain);

  ObStoreRowkey rowkey2;
  ObObj obj2[2];
  obj2[0].set_int32(1);
  obj2[1].set_int32(3);
  rowkey2.assign(obj2, 2);
  ret = bf_cache.may_contain(table_id, block_id, file_id, rowkey2, may_contain);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(may_contain);

  ret = bf_cache.put_bloom_filter(table_id, block_id, file_id, bf_value);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObEmptyReadCell* cell;
  ObBloomFilterCacheKey bf_key(table_id, block_id, file_id, rowkey.get_obj_cnt());
  storage::ObITable::TableKey table_key;
  ret = bf_cache.inc_empty_read(table_id, block_id, file_id, macro_meta, rowkey.get_obj_cnt(), table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  bf_cache.get_cell(bf_key.hash(), cell);
  ASSERT_TRUE(NULL != cell);
  EXPECT_EQ(1, cell->count_);

  ret = bf_cache.inc_empty_read(table_id, block_id, file_id, macro_meta, rowkey.get_obj_cnt(), table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  bf_cache.get_cell(bf_key.hash(), cell);
  ASSERT_TRUE(NULL != cell);
  EXPECT_EQ(2, cell->count_);

  bf_cache.destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObMacroBlockMetaMgr::get_instance().destroy();
}

TEST(ObEmptyReadCell, test_invalid)
{
  int ret = OB_SUCCESS;
  ObBloomFilterCache bf_cache;
  uint64_t table_id = combine_id(1, 3001);
  MacroBlockId block_id(3);
  int64_t file_id = 100;
  int8_t empty_read_prefix = 3;
  ObEmptyReadCell* cell;

  ret = bf_cache.init("bf_cache", 1, 7);
  EXPECT_NE(OB_SUCCESS, ret);

  ObBloomFilterCacheKey bf_key(table_id, block_id, file_id, empty_read_prefix);
  ret = bf_cache.get_cell(bf_key.hash(), cell);
  EXPECT_NE(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL == cell);

  bf_cache.destroy();
}

TEST(ObEmptyReadCell, test_normal)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
  ObBloomFilterCache bf_cache;
  ret = bf_cache.init("bf_cache", 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObStoreRowkey rowkey;
  ObObj obj[2];
  obj[0].set_int32(1);
  obj[1].set_int32(2);
  rowkey.assign(obj, 2);
  uint64_t cur_cnt = 0;
  uint64_t table_id = combine_id(1, 3001);
  MacroBlockId block_id(3);
  int64_t file_id = 100;
  ObBloomFilterCacheKey bf_key(table_id, block_id, file_id, rowkey.get_obj_cnt());
  ObEmptyReadCell* cell;

  bf_cache.get_cell(bf_key.hash(), cell);
  ASSERT_TRUE(NULL != cell);

  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  EXPECT_EQ(3, cur_cnt);

  bf_cache.get_cell(bf_key.hash() + bf_cache.get_bucket_size(), cell);
  ASSERT_TRUE(NULL != cell);
  cell->inc_and_fetch(bf_key.hash() + bf_cache.get_bucket_size(), cur_cnt);
  EXPECT_EQ(1, cur_cnt);
  cell->inc_and_fetch(bf_key.hash() + bf_cache.get_bucket_size(), cur_cnt);
  EXPECT_EQ(1, cur_cnt);

  cell->set(bf_key.hash());
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  EXPECT_EQ(2, cur_cnt);

  cell->reset();
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  EXPECT_EQ(1, cur_cnt);

  bf_cache.destroy();
  ObKVGlobalCache::get_instance().destroy();
}

}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
