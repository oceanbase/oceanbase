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
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_micro_block_index_cache.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "share/schema/ob_table_schema.h"
#include "ob_row_generate.h"
#include "ob_data_file_prepare.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace memtable;

namespace unittest {
class TestMicroBlockIndexCache : public TestDataFilePrepare {
public:
  TestMicroBlockIndexCache();
  virtual ~TestMicroBlockIndexCache();
  virtual void SetUp();
  virtual void TearDown();

protected:
  static const int64_t TEST_TABLE_ID = 3001;
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t MAX_TEST_COLUMN_CNT = TEST_COLUMN_CNT + 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 8;
  static const int64_t MAX_TEST_ROWKEY_COLUMN_CNT = TEST_ROWKEY_COLUMN_CNT + 1;
  void prepare_schema();
  void prepare_micro_index(const int64_t micro_block_row_count = 10, bool is_multi_version = false);
  void prepare_micro_index_mark_deletion();
  int find_border_key(int64_t& seed, ObMacroBlockCtx& block_ctx);
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObMicroBlockIndexCache block_index_cache_;
  MacroBlockId block_id_;
  int64_t row_cnt_;
  ObSSTable inc_store_;
  ObSSTable sstable_;
  ObITable::TableKey table_key_;
  int64_t border_seed_;
};

TestMicroBlockIndexCache::TestMicroBlockIndexCache()
    : TestDataFilePrepare("TestMicroBlockIndexCache"), row_cnt_(0), border_seed_(0)
{
  ObAddr self;
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  obrpc::ObCommonRpcProxy common_rpc;
  share::ObRsMgr rs_mgr;
  int64_t tenant_id = 1;
  self.set_ip_addr("127.0.0.1", 8086);
  ObTenantManager::get_instance().init(
      self, rpc_proxy, common_rpc, rs_mgr, &req_transport, &ObServerConfig::get_instance());
  ObTenantManager::get_instance().add_tenant(tenant_id);
  ObTenantManager::get_instance().set_tenant_mem_limit(
      tenant_id, 2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
}

TestMicroBlockIndexCache::~TestMicroBlockIndexCache()
{}

void TestMicroBlockIndexCache::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();
  ret = block_index_cache_.init("test_block_index_cache", 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  prepare_schema();
  prepare_micro_index();
}

void TestMicroBlockIndexCache::TearDown()
{
  block_index_cache_.destroy();
  sstable_.destroy();
  inc_store_.destroy();
  TestDataFilePrepare::TearDown();
}

void TestMicroBlockIndexCache::prepare_schema()
{
  ObColumnSchemaV2 column;
  // init table schema
  uint64_t table_id = combine_id(1, TEST_TABLE_ID);
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_row_writer"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(8 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(TEST_TABLE_ID);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
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
      column.set_collation_type(CS_TYPE_BINARY);
    } else if (obj_type == common::ObUInt64Type) {
      column.set_rowkey_position(8);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void TestMicroBlockIndexCache::prepare_micro_index(const int64_t micro_block_row_count, bool is_multi_version)
{
  int ret = OB_SUCCESS;
  int64_t data_version = 1;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObStoreRow row;
  ObObj cells[MAX_TEST_COLUMN_CNT];
  row.row_val_.assign(cells, is_multi_version ? MAX_TEST_COLUMN_CNT : TEST_COLUMN_CNT);
  row_generate_.reset();

  ObDataStoreDesc desc;
  const ObMultiVersionRowInfo* multi_version_row_info = NULL;
  if (is_multi_version) {
    ObMultiVersionColDescGenerate gen;
    ASSERT_EQ(OB_SUCCESS, gen.init(&table_schema_));
    ASSERT_EQ(OB_SUCCESS, gen.generate_multi_version_row_info(multi_version_row_info));
    ASSERT_TRUE(NULL != multi_version_row_info);
  }

  ObPGKey pg_key(combine_id(1, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = desc.init(table_schema_,
      data_version,
      multi_version_row_info,
      1,
      is_multi_version ? MINOR_MERGE : MAJOR_MERGE /*is_major*/,
      true,
      true,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.open(desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = row_generate_.init(table_schema_, is_multi_version);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCreateSSTableParamWithTable param;
  ObITable::TableKey table_key;
  int64_t table_id = combine_id(1, 3001);
  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key.pkey_ = ObPartitionKey(table_id, 0, 0);
  table_key.table_id_ = table_id;
  table_key.version_ = ObVersion(1, 0);
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.snapshot_version_ = 20;
  param.table_key_ = table_key;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
  param.pg_key_ = desc.pg_key_;
  param.logical_data_version_ = table_key.version_;
  sstable_.destroy();
  ASSERT_EQ(OB_SUCCESS, sstable_.init(table_key));
  ASSERT_EQ(OB_SUCCESS, sstable_.set_storage_file_handle(get_storage_file_handle()));
  ret = sstable_.open(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "create sstable param", K(param.pg_key_));

  row_cnt_ = 0;
  int64_t seed = 0;
  while (true) {
    if (is_multi_version) {
      ObRowDml dml = row_cnt_ < 2 ? T_DML_DELETE : (row_cnt_ < 4 ? T_DML_UPDATE : T_DML_INSERT);
      ret = row_generate_.get_next_row(seed, seed, dml, dml, row);
      row.row_type_flag_.set_first_multi_version_row(true);
      row.row_type_flag_.set_last_multi_version_row(true);
      row.row_type_flag_.set_compacted_multi_version_row(true);
    } else {
      ret = row_generate_.get_next_row(seed, row);
    }
    ++seed;
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_cnt_;
    if (micro_block_row_count > 0) {
      if (row_cnt_ % micro_block_row_count == 0) {
        ret = writer.build_micro_block(false);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    if (0 == border_seed_ && writer.macro_blocks_[writer.current_index_].header_->micro_block_count_ == 1) {
      border_seed_ = row_cnt_ - 1;
      STORAGE_LOG(INFO, "border row", K(row), K_(border_seed));
    }
    if (writer.get_macro_block_write_ctx().get_macro_block_count() > 0) {
      break;
    }
  }

  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sstable_.append_macro_blocks(writer.get_macro_block_write_ctx()));
  ret = sstable_.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  block_id_ = writer.get_macro_block_write_ctx().macro_block_list_[0];

  int64_t lsn = 0;
  ret = SLOGGER.commit(lsn);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMicroBlockIndexCache::prepare_micro_index_mark_deletion()
{
  int ret = OB_SUCCESS;
  table_key_.pkey_.table_id_ = table_schema_.get_table_id();
  table_key_.pkey_.part_id_ = 1;
  table_key_.pkey_.part_cnt_ = 1;
  table_key_.pkey_.assit_id_ = 0;
  table_key_.table_id_ = table_schema_.get_table_id();
  table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
  table_key_.version_ = 1;
  table_key_.trans_version_range_.base_version_ = 1;
  table_key_.trans_version_range_.multi_version_start_ = 1;
  table_key_.trans_version_range_.snapshot_version_ = INT64_MAX - 2;
  ASSERT_TRUE(table_key_.is_valid());

  ObCreateSSTableParamWithTable param;
  param.table_key_ = table_key_;
  param.create_snapshot_version_ = INT64_MAX - 2;
  param.progressive_merge_end_version_ = 0;
  param.progressive_merge_start_version_ = 0;
  param.schema_ = &table_schema_;
  param.schema_version_ = INT64_MAX - 2;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
  param.logical_data_version_ = table_key_.version_;

  ret = inc_store_.init(table_key_);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t data_version = 1;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObDataStoreDesc desc;

  ObPGKey pg_key(combine_id(1, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = desc.init(table_schema_,
      data_version,
      NULL,
      1,
      MINOR_MERGE,
      true,
      true,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);

  param.pg_key_ = desc.pg_key_;
  ret = inc_store_.open(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.open(desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);

  row_cnt_ = 0;
  int64_t seed = 0;
  while (true) {
    ret = row_generate_.get_next_row(seed, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    row.dml_ = T_DML_INSERT;
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_cnt_;
    if (writer.get_macro_block_write_ctx().get_macro_block_count() > 0) {
      break;
    }
    ++seed;
  }

  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = inc_store_.append_macro_blocks(writer.get_macro_block_write_ctx());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = inc_store_.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  // init mark deletion
  ObStoreRow multi_version_row;
  ObObj cells_multi_version[TEST_COLUMN_CNT + 1];
  multi_version_row.row_val_.assign(cells_multi_version, TEST_COLUMN_CNT + 1);
  writer.reset();
  desc.reset();
  ObMultiVersionColDescGenerate multi_version_col_desc_gen;
  ret = multi_version_col_desc_gen.init(&table_schema_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObMultiVersionRowInfo* multi_version_row_info = NULL;
  ret = multi_version_col_desc_gen.generate_multi_version_row_info(multi_version_row_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(multi_version_row_info != NULL);
  ret = desc.init(table_schema_,
      data_version,
      multi_version_row_info,
      1,
      MINOR_MERGE,
      true,
      true,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);
  // init mark deletion
  ObMemtableCtxFactory ctx_factory;
  ObBlockMarkDeletionMaker mark_deletion_maker;
  ObTablesHandle tables_handle;
  ret = tables_handle.add_table(&inc_store_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mark_deletion_maker.init(table_schema_, table_key_.pkey_, 0, INT64_MAX - 2, INT64_MAX);
  ASSERT_EQ(OB_SUCCESS, ret);
  inc_store_.status_ = SSTABLE_READY_FOR_READ;
  desc.mark_deletion_maker_ = &mark_deletion_maker;
  ret = writer.open(desc, 0, NULL);
  ASSERT_EQ(OB_SUCCESS, ret);
  seed = 0;
  row_cnt_ = 0;

  while (true) {
    ret = row_generate_.get_next_row(seed, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < table_schema_.get_rowkey_column_num(); ++i) {
      multi_version_row.row_val_.cells_[i] = row.row_val_.cells_[i];
    }
    multi_version_row.row_val_.cells_[table_schema_.get_rowkey_column_num()].set_int(-9999);
    for (int64_t i = table_schema_.get_rowkey_column_num(); i < table_schema_.get_column_count(); ++i) {
      multi_version_row.row_val_.cells_[i + 1] = row.row_val_.cells_[i];
    }
    multi_version_row.row_type_flag_.flag_ = 3;
    multi_version_row.dml_ = T_DML_DELETE;
    multi_version_row.flag_ = 16;
    ret = writer.append_row(multi_version_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_cnt_;
    if (writer.get_lob_macro_block_write_ctx().get_macro_block_count() > 0) {
      break;
    }
    ++seed;
  }

  block_id_ = writer.get_macro_block_write_ctx().macro_block_list_[0];

  int64_t lsn = 0;
  ret = SLOGGER.commit(lsn);
  ASSERT_EQ(OB_SUCCESS, ret);
}

int TestMicroBlockIndexCache::find_border_key(int64_t& seed, ObMacroBlockCtx& block_ctx)
{
  int ret = OB_SUCCESS;
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;
  seed = border_seed_;
  return ret;
}

TEST_F(TestMicroBlockIndexCache, range)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  ObMacroBlockCtx block_ctx;
  ObArray<ObMicroBlockInfo> micro_infos;

  // invalid use
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid block id
  block_ctx.sstable_block_id_.macro_block_id_.block_index_ = 0;
  block_ctx.sstable_ = &sstable_;
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_NE(OB_SUCCESS, ret);

  // whole range
  micro_infos.reset();
  range.reset();
  range.start_key_.set_min();
  range.end_key_.set_max();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;

  ObFullMacroBlockMeta meta;

  ret = sstable_.get_meta(block_ctx.get_macro_block_id(), meta);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(meta.is_valid());

  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta.meta_->micro_block_count_, micro_infos.count());

  // border range
  int64_t seed = 0;
  ret = find_border_key(seed, block_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  range.reset();
  ObStoreRow row;
  ObObj end_cells[TEST_COLUMN_CNT];
  row.row_val_.assign(end_cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(seed, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  range.start_key_.set_min();
  range.end_key_.assign(end_cells, TEST_ROWKEY_COLUMN_CNT);
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, micro_infos.count());
}

TEST_F(TestMicroBlockIndexCache, rowkey)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObStoreRowkey rowkey;
  rowkey.assign(cells, TEST_ROWKEY_COLUMN_CNT);
  ObMicroBlockInfo micro_info;
  ObMacroBlockCtx block_ctx;

  // invalid block id
  block_ctx.sstable_block_id_.macro_block_id_.block_index_ = 0;
  block_ctx.sstable_ = &sstable_;
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, rowkey, micro_info);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal key
  micro_info.reset();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;
  ret = row_generate_.get_next_row(0, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, rowkey, micro_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(micro_info.is_valid());

  // border key
  int64_t seed = 0;
  micro_info.reset();
  ret = find_border_key(seed, block_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate_.get_next_row(seed, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, rowkey, micro_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(micro_info.is_valid());

  // not exist key
  micro_info.reset();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;
  ret = row_generate_.get_next_row(row_cnt_, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, rowkey, micro_info);
  ASSERT_EQ(OB_BEYOND_THE_RANGE, ret);
}

TEST_F(TestMicroBlockIndexCache, DISABLED_perf)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObStoreRowkey rowkey;
  rowkey.assign(cells, TEST_ROWKEY_COLUMN_CNT);
  ObMicroBlockInfo micro_info;
  ObMacroBlockCtx block_ctx;

  // normal key
  micro_info.reset();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;
  block_ctx.sstable_ = &sstable_;
  int64_t begin_time = ObTimeUtility::current_time();
  const int64_t perf_round = 1000;
  for (int64_t j = 0; j < perf_round; ++j) {
    for (int64_t i = 0; i < 100; ++i) {
      ret = row_generate_.get_next_row(i, row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, rowkey, micro_info);
      ASSERT_EQ(OB_SUCCESS, ret) << "i: " << i << ", j: " << j << ", row_cnt: " << row_cnt_;
      ASSERT_TRUE(micro_info.is_valid());
    }
  }

  int64_t end_time = ObTimeUtility::current_time();
  STORAGE_LOG(INFO, "perf find rowkey", "times", perf_round, "cost_time_us", end_time - begin_time, K(row_cnt_));
}

TEST_F(TestMicroBlockIndexCache, DISABLED_test_mark_deletion)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  MacroBlockId block_id;
  ObMacroBlockCtx block_ctx;
  ObArray<ObMicroBlockInfo> micro_infos;

  // invalid use
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid block id
  block_ctx.sstable_block_id_.macro_block_id_.block_index_ = 0;
  block_ctx.sstable_ = &sstable_;
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_NE(OB_SUCCESS, ret);

  // whole range
  micro_infos.reset();
  range.reset();
  range.start_key_.set_min();
  range.end_key_.set_max();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;

  ObFullMacroBlockMeta meta;

  ret = sstable_.get_meta(block_ctx.get_macro_block_id(), meta);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(meta.is_valid());

  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta.meta_->micro_block_count_, micro_infos.count());

  for (int64_t i = 0; i < micro_infos.count(); ++i) {
    ASSERT_TRUE(micro_infos.at(i).mark_deletion_ == false);
  }

  // border range
  int64_t seed = 0;
  ret = find_border_key(seed, block_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  micro_infos.reset();
  range.reset();
  ObStoreRow row;
  ObObj end_cells[TEST_COLUMN_CNT];
  row.row_val_.assign(end_cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(seed, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  range.start_key_.set_min();
  range.end_key_.assign(end_cells, TEST_ROWKEY_COLUMN_CNT);
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, micro_infos.count());
  for (int64_t i = 0; i < micro_infos.count(); ++i) {
    ASSERT_TRUE(micro_infos.at(i).mark_deletion_ == false);
  }

  prepare_micro_index_mark_deletion();
  micro_infos.reset();
  range.reset();
  range.start_key_.set_min();
  range.end_key_.set_max();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;
  block_ctx.sstable_ = &inc_store_;
  ret = inc_store_.get_meta(block_ctx.get_macro_block_id(), meta);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(meta.is_valid());

  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta.meta_->micro_block_count_, micro_infos.count());
  for (int64_t i = 0; i < micro_infos.count(); ++i) {
    ASSERT_TRUE(micro_infos.at(i).mark_deletion_ == false);
  }
}

TEST_F(TestMicroBlockIndexCache, test_get_endkey)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  ObMacroBlockCtx block_ctx;
  ObArray<ObMicroBlockInfo> micro_infos;

  // whole range
  micro_infos.reset();
  range.reset();
  range.start_key_.set_min();
  range.end_key_.set_max();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;
  block_ctx.sstable_ = &sstable_;

  ObFullMacroBlockMeta meta;

  ret = sstable_.get_meta(block_ctx.get_macro_block_id(), meta);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(meta.is_valid());

  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta.meta_->micro_block_count_, micro_infos.count());

  for (int64_t i = 0; i < micro_infos.count(); ++i) {
    ASSERT_TRUE(micro_infos.at(i).mark_deletion_ == false);
  }

  ObStoreRowkey endkey;
  ObArenaAllocator allocator;
  int32_t micro_block_index = 0;
  ret = block_index_cache_.get_micro_endkey(
      table_schema_.get_table_id(), block_ctx, micro_block_index, allocator, endkey);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(WARN, "endkey is", K(endkey.get_obj_cnt()), K(endkey));
}

TEST_F(TestMicroBlockIndexCache, test_get_endkeys)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  ObMacroBlockCtx block_ctx;
  ObArray<ObMicroBlockInfo> micro_infos;

  // whole range
  micro_infos.reset();
  range.reset();
  range.start_key_.set_min();
  range.end_key_.set_max();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  block_ctx.sstable_block_id_.macro_block_id_ = block_id_;
  block_ctx.sstable_ = &sstable_;

  ObFullMacroBlockMeta meta;

  ret = sstable_.get_meta(block_ctx.get_macro_block_id(), meta);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(meta.is_valid());

  ret = block_index_cache_.get_micro_infos(table_schema_.get_table_id(), block_ctx, range, true, true, micro_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta.meta_->micro_block_count_, micro_infos.count());

  for (int64_t i = 0; i < micro_infos.count(); ++i) {
    ASSERT_TRUE(micro_infos.at(i).mark_deletion_ == false);
  }

  ObStoreRowkey endkey;
  ObArenaAllocator allocator;
  ObArray<ObStoreRowkey> endkeys;
  ret = block_index_cache_.get_micro_endkeys(table_schema_.get_table_id(), block_ctx, allocator, endkeys);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < endkeys.count(); ++i) {
    STORAGE_LOG(WARN, "endkey is", K(endkeys.at(i)));
  }
  ASSERT_TRUE(meta.meta_->micro_block_count_ == endkeys.count());
}

}  // end namespace unittest
}  // end namespace oceanbase
int main(int argc, char** argv)
{
  system("rm -f test_micro_block_index_cache.log*");
  OB_LOGGER.set_file_name("test_micro_block_index_cache.log", true, true);
  // OB_LOGGER.set_log_level("WARN");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
