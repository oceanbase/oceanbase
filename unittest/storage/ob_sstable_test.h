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
#include "lib/container/ob_array.h"
#include "lib/stat/ob_diagnose_info.h"
#define private public
#define protected public
#include "share/ob_tenant_mgr.h"
#include "storage/ob_sstable.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "blocksstable/ob_row_generate.h"
#include "./blocksstable/ob_data_file_prepare.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {

struct SkipInfo {
  SkipInfo() : start_key_(0), gap_key_(0)
  {}
  TO_STRING_KV(K_(start_key), K_(gap_key));
  int64_t start_key_;
  int64_t gap_key_;
};

class ObSSTableTest : public TestDataFilePrepare {
public:
  ObSSTableTest(const char* data_dir = "sstable", const int64_t macro_block_size = 64 * 1024,
      const int64_t macro_block_cnt = 100);
  virtual ~ObSSTableTest();
  virtual void SetUp();
  virtual void TearDown();
  static void convert_rowkey(const ObStoreRowkey& rowkey, ObExtStoreRowkey& ext_rowkey, ObIAllocator& allocator);
  static void convert_rowkey(
      const ObIArray<ObStoreRowkey>& rowkeys, ObArray<ObExtStoreRowkey>& ext_rowkeys, ObIAllocator& allocator);
  bool compare(const bool is_reverse_scan, const int64_t curr, const int64_t end);

protected:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 8;
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t TEST_MULTI_GET_CNT = 2000;
  static const int64_t TEST_CACHE_SCENES_NUM = 4;
  void prepare_schema();
  void prepare_data(const int64_t row_cnt, ObSSTable& sstable);
  void convert_range(const ObStoreRange& range, ObExtStoreRange& ext_range, ObIAllocator& allocator);
  void convert_range(
      const ObIArray<ObStoreRange>& ranges, ObIArray<ObExtStoreRange>& ext_ranges, ObIAllocator& allocator);
  void destroy_row_cache();
  void destroy_block_index_cache();
  void destroy_block_cache();
  void destroy_cache();
  void destroy_all_cache();
  int prepare_query_param(const bool is_reverse_scan, const int64_t limit);
  void destroy_query_param();
  int64_t row_cnt_;
  ObSSTable sstable_;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObArenaAllocator allocator_;
  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObLimitParam limit_param_;
  ObStoreCtx store_ctx_;
  ObBlockCacheWorkingSet block_cache_ws_;
  ObITable::TableKey table_key_;

protected:
  enum CacheHitMode {
    HIT_ALL = 0,
    HIT_NONE,
    HIT_PART,
    HIT_MAX,
  };
};

ObSSTableTest::ObSSTableTest(const char* data_dir, const int64_t macro_block_size, const int64_t macro_block_cnt)
    : TestDataFilePrepare(data_dir, macro_block_size, macro_block_cnt),
      row_cnt_(0),
      sstable_(),
      table_schema_(),
      row_generate_(),
      allocator_(ObModIds::TEST),
      columns_(),
      param_(),
      context_(),
      store_ctx_()
{}

ObSSTableTest::~ObSSTableTest()
{}

void ObSSTableTest::SetUp()
{
  TestDataFilePrepare::SetUp();
  prepare_schema();
  ASSERT_EQ(OB_SUCCESS, sstable_.init(table_key_));
  ASSERT_EQ(OB_SUCCESS, sstable_.set_storage_file_handle(get_storage_file_handle()));
  prepare_data(3000, sstable_);
  STORAGE_LOG(INFO, "sstable info", K(sstable_));
  srand(static_cast<uint32_t>(time(NULL)));
}

void ObSSTableTest::TearDown()
{
  table_schema_.reset();
  sstable_.destroy();
  row_generate_.reset();
  allocator_.reuse();
  block_cache_ws_.reset();
  TestDataFilePrepare::TearDown();
}

void ObSSTableTest::destroy_row_cache()
{
  const uint64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().erase_cache(tenant_id, "user_row_cache"));
}

void ObSSTableTest::destroy_block_index_cache()
{
  const uint64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().erase_cache(tenant_id, "block_index_cache"));
}

void ObSSTableTest::destroy_block_cache()
{
  const uint64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().erase_cache(tenant_id, "user_block_cache"));
}

void ObSSTableTest::destroy_cache()
{
  const int64_t cache_index = rand() % (TEST_CACHE_SCENES_NUM + 1);
  switch (cache_index % 4) {
    case 0:
      // do nothing
      break;
    case 1:
      destroy_row_cache();
      STORAGE_LOG(DEBUG, "destory row cache");
      break;
    case 2:
      destroy_block_index_cache();
      STORAGE_LOG(DEBUG, "destory block index cache");
      break;
    case 3:
      destroy_block_cache();
      STORAGE_LOG(DEBUG, "destory block cache");
      break;
    default:
      break;
  }
}

void ObSSTableTest::destroy_all_cache()
{
  destroy_row_cache();
  destroy_block_index_cache();
  destroy_block_cache();
}

bool ObSSTableTest::compare(const bool is_reverse_scan, const int64_t curr, const int64_t end)
{
  return is_reverse_scan ? curr >= end : curr <= end;
}

void ObSSTableTest::prepare_schema()
{
  int64_t table_id = combine_id(TENANT_ID, TABLE_ID);
  table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key_.pkey_ = ObPartitionKey(table_id, 0, 0);
  table_key_.table_id_ = table_id;
  table_key_.version_ = ObVersion(1, 0);
  table_key_.trans_version_range_.multi_version_start_ = 0;
  table_key_.trans_version_range_.base_version_ = 0;
  table_key_.trans_version_range_.snapshot_version_ = 20;

  ObColumnSchemaV2 column;
  // init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_sstable"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(4 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObObjMeta meta_type;
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_length(1);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    meta_type.set_type(obj_type);
    column.set_meta_type(meta_type);
    if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
      meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
      meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_meta_type(meta_type);
    }
    if (obj_type == common::ObVarcharType) {
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObCharType) {
      column.set_rowkey_position(2);
    } else if (obj_type == common::ObDoubleType) {
      column.set_rowkey_position(3);
    } else if (obj_type == common::ObNumberType) {
      column.set_rowkey_position(4);
    } else if (obj_type == common::ObUNumberType) {
      column.set_rowkey_position(5);
    } else if (obj_type == common::ObIntType) {
      column.set_rowkey_position(6);
    } else if (obj_type == common::ObHexStringType) {
      column.set_rowkey_position(7);
    } else if (obj_type == common::ObUInt64Type) {
      column.set_rowkey_position(8);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void ObSSTableTest::prepare_data(const int64_t row_cnt, ObSSTable& sstable)
{
  int ret = OB_SUCCESS;
  int64_t data_version = 1;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObCreateSSTableParamWithTable param;

  param.table_key_ = table_key_;
  param.logical_data_version_ = table_key_.version_;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;

  ObPGKey pg_key(combine_id(1, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = data_desc.init(table_schema_,
      data_version,
      NULL,
      1,
      MAJOR_MERGE,
      true,
      true,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);
  param.pg_key_ = data_desc.pg_key_;
  ret = sstable.open(param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.open(data_desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  row_cnt_ = 0;
  while (row_cnt_ < row_cnt) {
    ret = row_generate_.get_next_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_cnt_;
  }

  // close sstable
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sstable.append_macro_blocks(writer.get_macro_block_write_ctx()));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void ObSSTableTest::convert_rowkey(const ObStoreRowkey& rowkey, ObExtStoreRowkey& ext_rowkey, ObIAllocator& allocator)
{
  ext_rowkey.get_store_rowkey() = rowkey;
  ASSERT_EQ(OB_SUCCESS, ext_rowkey.to_collation_free_on_demand_and_cutoff_range(allocator));
}

void ObSSTableTest::convert_rowkey(
    const ObIArray<ObStoreRowkey>& rowkeys, ObArray<ObExtStoreRowkey>& ext_rowkeys, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ext_rowkeys.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
    ObExtStoreRowkey ext_rowkey;
    convert_rowkey(rowkeys.at(i), ext_rowkey, allocator);
    ASSERT_EQ(OB_SUCCESS, ext_rowkeys.push_back(ext_rowkey));
  }
}

void ObSSTableTest::convert_range(
    const ObIArray<ObStoreRange>& ranges, ObIArray<ObExtStoreRange>& ext_ranges, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ext_ranges.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    ObExtStoreRange ext_range;
    convert_range(ranges.at(i), ext_range, allocator);
    ASSERT_EQ(OB_SUCCESS, ext_ranges.push_back(ext_range));
  }
}

void ObSSTableTest::convert_range(const ObStoreRange& range, ObExtStoreRange& ext_range, ObIAllocator& allocator)
{
  ext_range.reset();
  ext_range.get_range() = range;
  ASSERT_EQ(OB_SUCCESS, ext_range.to_collation_free_range_on_demand_and_cutoff_range(allocator));
}

int ObSSTableTest::prepare_query_param(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  columns_.reset();
  ObQueryFlag query_flag;
  block_cache_ws_.reset();
  const uint64_t tenant_id = extract_tenant_id(table_schema_.get_table_id());
  if (OB_FAIL(table_schema_.get_column_ids(columns_))) {
    STORAGE_LOG(WARN, "fail to get column ids", K(ret));
  } else if (OB_FAIL(block_cache_ws_.init(tenant_id))) {
    STORAGE_LOG(WARN, "block_cache_ws_ init failed", K(ret));
  } else {
    if (is_reverse_scan) {
      query_flag.scan_order_ = ObQueryFlag::Reverse;
    }
    param_.table_id_ = table_schema_.get_table_id();
    param_.rowkey_cnt_ = table_schema_.get_rowkey_column_num();
    param_.out_cols_ = &columns_;
    limit_param_.offset_ = 0;
    limit_param_.limit_ = limit;
    context_.query_flag_ = query_flag;
    context_.store_ctx_ = &store_ctx_;
    context_.allocator_ = &allocator_;
    context_.stmt_allocator_ = &allocator_;
    context_.limit_param_ = &limit_param_;
    context_.block_cache_ws_ = &block_cache_ws_;
    context_.is_inited_ = true;
  }
  return ret;
}

void ObSSTableTest::destroy_query_param()
{
  columns_.reset();
  param_.reset();
  context_.reset();
  block_cache_ws_.reset();
}

}  // end namespace unittest
}  // end namespace oceanbase
