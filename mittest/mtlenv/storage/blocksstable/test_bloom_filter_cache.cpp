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
#define protected public
#define USING_LOG_PREFIX STORAGE

#include "mtlenv/mock_tenant_module_env.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/ob_i_table.h"
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "storage/blocksstable/ob_sstable.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/access/ob_empty_read_bucket.h"

namespace oceanbase
{
using namespace common;
static ObSimpleMemLimitGetter getter;
namespace blocksstable
{

class TestBloomFilterCache : public ::testing::Test
{
public:
  TestBloomFilterCache() {}
  virtual ~TestBloomFilterCache() {}

  virtual void SetUp();
  virtual void TearDown() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_valid_read_handle(storage::ObSSTableReadHandle &read_handle, const int64_t rowkey_datum_cnt);
  void prepare_valid_sstable_key(storage::ObITable::TableKey &sstable_key);
public:
  ObArenaAllocator allocator_;
  ObStorageDatumUtils datum_utils_;
};

void TestBloomFilterCache::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  ObColDesc col_desc;
  ObSEArray<ObColDesc, 2> col_descs;
  col_desc.col_type_.set_int32();
  for (int64_t i = 0; i < 2; i++) {
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + i;
    ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_desc));
  }
  ASSERT_EQ(OB_SUCCESS, datum_utils_.init(col_descs, 2, lib::is_oracle_mode(), allocator_));
}

void TestBloomFilterCache::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestBloomFilterCache::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestBloomFilterCache::prepare_valid_read_handle(storage::ObSSTableReadHandle &read_handle,
                                                     const int64_t rowkey_datum_cnt)
{
  int ret = OB_SUCCESS;
  // prepare row header
  ObIndexBlockRowHeader * tmp_row_header = OB_NEWx(ObIndexBlockRowHeader, &allocator_);
  tmp_row_header->is_macro_node_ = 1;
  tmp_row_header->has_macro_block_bloom_filter_ = 0;
  read_handle.index_block_info_.row_header_ = tmp_row_header;
  // prepare rowkey
  ObDatumRowkey * tmp_rowkey = OB_NEWx(ObDatumRowkey, &allocator_);
  tmp_rowkey->datum_cnt_ = rowkey_datum_cnt;
  read_handle.rowkey_ = tmp_rowkey;
  // prepare index block info
  read_handle.index_block_info_.endkey_.type_ = ObCommonDatumRowkey::RowkeyType::COMPACT;
  read_handle.index_block_info_.endkey_.key_ptr_ = static_cast<void *>(tmp_rowkey);
  // double check
  ASSERT_EQ(true, read_handle.index_block_info_.row_header_->is_valid());
  ASSERT_EQ(true, read_handle.index_block_info_.is_valid());
  ASSERT_EQ(true, read_handle.is_valid());
}

void TestBloomFilterCache::prepare_valid_sstable_key(storage::ObITable::TableKey &sstable_key)
{
  sstable_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  sstable_key.tablet_id_ = 1;
  sstable_key.version_range_.snapshot_version_ = 1;
  ASSERT_EQ(true, sstable_key.is_valid());
}

TEST_F(TestBloomFilterCache, test_invalid)
{
  int ret = OB_SUCCESS;
  ObBloomFilterCache bf_cache;
  bool may_contain = false;
  ObBloomFilterCacheValue bf_value;
  const uint64_t tenant_id = 1;
  uint32_t key_hash = 0;
  MacroBlockId block_id(0, 3, 0);
  ObDatumRowkey rowkey;
  rowkey.set_max_rowkey();

  ret = bf_cache.may_contain(tenant_id, block_id, rowkey, datum_utils_, may_contain);
  EXPECT_NE(OB_SUCCESS, ret);
  EXPECT_TRUE(may_contain);

  ret = bf_cache.put_bloom_filter(tenant_id, block_id, bf_value, true);
  EXPECT_NE(OB_SUCCESS, ret);

  MacroBlockId macro_block_id;
  /* Mock Variable */
  share::ObLSID ls_id(1);
  storage::ObSSTableReadHandle read_handle;
  prepare_valid_read_handle(read_handle, 2);
  storage::ObITable::TableKey sstable_key;
  prepare_valid_sstable_key(sstable_key);
  ret = bf_cache.inc_empty_read(tenant_id, 1099511627877, ls_id, sstable_key, macro_block_id, 2, &read_handle);
  EXPECT_NE(OB_SUCCESS, ret);

  bf_cache.destroy();
  MTL(storage::ObEmptyReadBucket *)->reset();
}


TEST_F(TestBloomFilterCache, test_normal)
{
  int ret = OB_SUCCESS;
  ObBloomFilterCache bf_cache;
  ObDatumRowkey rowkey;
  bool may_contain = false;
  ObBloomFilterCacheValue bf_value;
  const uint64_t tenant_id = 1;
  MacroBlockId block_id(0, 3, 0);
  uint64_t key_hash;

  // test ObBloomFilterCache may_contain()
  ret = bf_cache.init("test_normal_bf_cache", 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = bf_value.init(2, 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObStorageDatum obj[2];
  obj[0].set_int32(1);
  obj[1].set_int32(2);
  rowkey.assign(obj, 2);
  ASSERT_EQ(OB_SUCCESS, rowkey.murmurhash(0, datum_utils_, key_hash));
  ret = bf_value.insert(key_hash);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = bf_cache.put_bloom_filter(tenant_id, block_id, bf_value, true);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = bf_cache.may_contain(tenant_id, block_id, rowkey, datum_utils_, may_contain);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(may_contain);

  ObDatumRowkey rowkey2;
  ObStorageDatum obj2[2];
  obj2[0].set_int32(1);
  obj2[1].set_int32(3);
  rowkey2.assign(obj2, 2);
  ret = bf_cache.may_contain(tenant_id, block_id, rowkey2, datum_utils_, may_contain);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(may_contain);

  ret = bf_cache.put_bloom_filter(tenant_id, block_id, bf_value);
  EXPECT_EQ(OB_SUCCESS, ret);

  /* Mock Variable */
  share::ObLSID ls_id(1);
  storage::ObSSTableReadHandle read_handle;
  prepare_valid_read_handle(read_handle, rowkey.get_datum_cnt());
  storage::ObITable::TableKey sstable_key;
  prepare_valid_sstable_key(sstable_key);

  storage::ObEmptyReadCell *cell;
  ObBloomFilterCacheKey bf_key(tenant_id, block_id, rowkey.get_datum_cnt());
  ret = bf_cache.inc_empty_read(tenant_id, 1099511627877, ls_id, sstable_key, block_id, rowkey.get_datum_cnt(), &read_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  MTL(storage::ObEmptyReadBucket *)->get_cell(bf_key.hash(),cell);
  ASSERT_TRUE(NULL != cell);
  EXPECT_EQ(1, cell->count_);

  prepare_valid_read_handle(read_handle, rowkey.get_datum_cnt());
  ret = bf_cache.inc_empty_read(tenant_id, 1099511627877, ls_id, sstable_key, block_id, rowkey.get_datum_cnt(), &read_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  MTL(storage::ObEmptyReadBucket *)->get_cell(bf_key.hash(),cell);
  ASSERT_TRUE(NULL != cell);
  EXPECT_EQ(2, cell->count_);

  bf_cache.destroy();
  ObKVGlobalCache::get_instance().destroy();
  MTL(storage::ObEmptyReadBucket *)->reset();
}


TEST_F(TestBloomFilterCache, test_empty_read_cell_normal)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  ObBloomFilterCache bf_cache;
  ret = bf_cache.init("test_bf_cache1", 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObStoreRowkey rowkey;
  ObObj obj[2];
  obj[0].set_int32(1);
  obj[1].set_int32(2);
  rowkey.assign(obj, 2);
  uint64_t cur_cnt=0;
  const uint64_t tenant_id = 1;
  MacroBlockId block_id(0, 3, 0);
  ObBloomFilterCacheKey bf_key(tenant_id, block_id, rowkey.get_obj_cnt());
  storage::ObEmptyReadCell *cell;

  MTL(storage::ObEmptyReadBucket *)->get_cell(bf_key.hash(),cell);
  ASSERT_TRUE(NULL != cell);

  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  EXPECT_EQ(3, cur_cnt);

  MTL(storage::ObEmptyReadBucket *)->get_cell(bf_key.hash() + MTL(ObEmptyReadBucket *)->get_bucket_size(),cell);
  ASSERT_TRUE(NULL != cell);
  cell->inc_and_fetch(bf_key.hash() + MTL(storage::ObEmptyReadBucket *)->get_bucket_size(), cur_cnt);
  EXPECT_EQ(1, cur_cnt);
  cell->inc_and_fetch(bf_key.hash() + MTL(storage::ObEmptyReadBucket *)->get_bucket_size(), cur_cnt);
  EXPECT_EQ(1, cur_cnt);

  cell->set(bf_key.hash(), 1);
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  EXPECT_EQ(2, cur_cnt);

  cell->reset();
  cell->inc_and_fetch(bf_key.hash(), cur_cnt);
  EXPECT_EQ(1, cur_cnt);

  bf_cache.destroy();
  ObKVGlobalCache::get_instance().destroy();
  MTL(storage::ObEmptyReadBucket *)->reset();
}

}
}

int main(int argc, char** argv)
{
  system("rm -f test_bloom_fitler_cache.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_bloom_filter_cache.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
