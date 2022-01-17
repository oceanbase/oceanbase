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
#include "lib/container/ob_iarray.h"
#define private public
#define protected public
#include "storage/ob_partition_group.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/ob_partition_merge_task.h"
#include "../blocksstable/ob_data_file_prepare.h"
#include "../blocksstable/ob_row_generate.h"
#include "../mock_ob_partition_report.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace storage;
using namespace memtable;
namespace compaction {

static const int64_t BASE_ROW_COUNT = 2;
class TestPartitionMergeUtil : public TestDataFilePrepare {
public:
  TestPartitionMergeUtil();
  virtual ~TestPartitionMergeUtil();
  virtual void SetUp();
  virtual void TearDown();

protected:
  void prepare_schema();
  void create_base_sstable();
  void create_inc_sstable1();
  void create_inc_sstable2();
  void prepare_scan_param(const bool is_minor);
  void complete_sstable(const ObITable::TableKey& table_key, ObMacroBlocksWriteCtx& write_ctx, ObTableHandle& handle);
  int assign_column_id(ObStoreRow& row);
  void complete_sstable(const ObITable::TableKey& table_key, ObMacroBlocksWriteCtx& write_ctx, ObTableHandle& handle);
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  static const int64_t TEST_COLUMN_CNT = 3;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObTableHandle base_handle_;  // 0,1
  ObRowGenerate inc_row_generate_;
  ObTableHandle inc_handle1_;  // 2,3
  ObTableHandle inc_handle2_;  // del 1,2,5
  ObPartitionComponentFactory cp_fty_;
  ObMultiVersionColDescGenerate multi_version_col_desc_gen_;
  const ObMultiVersionRowInfo* multi_version_row_info_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObBlockCacheWorkingSet block_cache_ws_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
  ObColDescArray columns_;
  ObColDescArray minor_columns_;
  transaction::ObTransService trans_service_;
  transaction::ObPartitionTransCtxMgr ctx_mgr_;
};

TestPartitionMergeUtil::TestPartitionMergeUtil()
    : TestDataFilePrepare("TestPartitionMergeUtil"), multi_version_row_info_(NULL)
{}

TestPartitionMergeUtil::~TestPartitionMergeUtil()
{}

void TestPartitionMergeUtil::SetUp()
{
  GCONF._enable_sparse_row = true;
  int ret = OB_SUCCESS;
  ObTableMgr::get_instance().init();
  TestDataFilePrepare::SetUp();
  prepare_schema();
  ObPartitionKey pkey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
  ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().init());
  ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().add_partition(pkey, pkey));
  ObPartitionKey& part = const_cast<ObPartitionKey&>(ctx_mgr_.get_partition());
  part = pkey;
  trans_service_.part_trans_ctx_mgr_.mgr_cache_.set(pkey.hash(), &ctx_mgr_);
  ObPartitionService::get_instance().txs_ = &trans_service_;
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));
  ASSERT_EQ(OB_SUCCESS, inc_row_generate_.init(table_schema_, &allocator_, true /*multi-version row*/));
  create_base_sstable();
  create_inc_sstable1();
  create_inc_sstable2();
}

void TestPartitionMergeUtil::TearDown()
{
  table_schema_.reset();
  base_handle_.reset();
  inc_handle1_.reset();
  inc_handle2_.reset();
  TestDataFilePrepare::TearDown();
  ObTableMgr::get_instance().destroy();
  ObPartitionService::get_instance().get_pg_index().destroy();
}

void TestPartitionMergeUtil::prepare_scan_param(const bool is_minor)
{
  param_.reset();
  context_.reset();
  block_cache_ws_.reset();
  projector_.reset();

  ObQueryFlag query_flag;
  query_flag.whole_macro_scan_ = true;
  query_flag.multi_version_minor_merge_ = is_minor;

  param_.table_id_ = table_schema_.get_table_id();
  param_.schema_version_ = table_schema_.get_schema_version();
  param_.rowkey_cnt_ = table_schema_.get_rowkey_column_num();
  if (is_minor) {
    ++param_.rowkey_cnt_;
    param_.out_cols_ = &minor_columns_;
  } else {
    param_.out_cols_ = &columns_;
  }
  ASSERT_EQ(OB_SUCCESS, block_cache_ws_.init(table_schema_.get_tenant_id()));
  context_.query_flag_ = query_flag;
  context_.store_ctx_ = &store_ctx_;
  context_.allocator_ = &allocator_;
  context_.stmt_allocator_ = &allocator_;
  context_.block_cache_ws_ = &block_cache_ws_;
  context_.trans_version_range_.base_version_ = 0;
  context_.trans_version_range_.multi_version_start_ = 0;
  context_.trans_version_range_.snapshot_version_ = INT64_MAX - 1;
  context_.is_inited_ = true;
}

void TestPartitionMergeUtil::prepare_schema()
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 16 * 1024;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = combine_id(tenant_id, TABLE_ID);
  ObColumnSchemaV2 column;

  // generate data table schema
  table_schema_.reset();
  ret = table_schema_.set_table_name("test_ssstore");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema_.set_tenant_id(tenant_id);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObColDesc col_desc;
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = ObIntType;
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    col_desc.col_id_ = column.get_column_id();
    col_desc.col_type_ = column.get_meta_type();
    ASSERT_EQ(OB_SUCCESS, columns_.push_back(col_desc));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema_));
  ASSERT_EQ(OB_SUCCESS, multi_version_col_desc_gen_.init(&table_schema_));
  ASSERT_EQ(OB_SUCCESS, multi_version_col_desc_gen_.generate_multi_version_row_info(multi_version_row_info_));
  ASSERT_EQ(OB_SUCCESS, multi_version_col_desc_gen_.generate_column_ids(minor_columns_));
}

void TestPartitionMergeUtil::complete_sstable(
    const ObITable::TableKey& table_key, ObMacroBlocksWriteCtx& write_ctx, ObTableHandle& handle)
{
  ObCreateSSTableParamWithTable param;
  ObSSTable* sstable = NULL;

  param.table_key_ = table_key;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
  param.logical_data_version_ = table_key.version_;
  ASSERT_EQ(OB_SUCCESS, ObTableMgr::get_instance().create_sstable(param, handle));
  ASSERT_EQ(OB_SUCCESS, handle.get_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS, sstable->append_macro_blocks(write_ctx));
  ASSERT_EQ(OB_SUCCESS, sstable->close());
  ASSERT_EQ(OB_SUCCESS, ObTableMgr::get_instance().complete_sstable(handle));
}

void TestPartitionMergeUtil::complete_sstable(
    const ObITable::TableKey& table_key, ObMacroBlocksWriteCtx& write_ctx, ObTableHandle& handle)
{
  ObCreateSSTableParam param;
  ObSSTable* sstable = NULL;

  param.table_key_ = table_key;
  param.logical_data_version_ = table_key.version_;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
  ASSERT_EQ(OB_SUCCESS, ObTableMgr::get_instance().create_sstable(param, handle));
  ASSERT_EQ(OB_SUCCESS, handle.get_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS, sstable->append_macro_blocks(write_ctx));
  ASSERT_EQ(OB_SUCCESS, sstable->close());
  ASSERT_EQ(OB_SUCCESS, ObTableMgr::get_instance().complete_sstable(handle));
}

void TestPartitionMergeUtil::create_base_sstable()
{
  int64_t data_version = 1;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObITable::TableKey table_key;

  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, table_key.pkey_.init(table_schema_.get_table_id(), 1, 0));
  table_key.table_id_ = table_schema_.get_table_id();
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 1;
  table_key.trans_version_range_.snapshot_version_ = 1;

  ASSERT_EQ(OB_SUCCESS,
      data_desc.init(table_schema_,
          data_version,
          NULL,
          1 /*partition id*/,
          MAJOR_MERGE /*is major*/,
          true /*column_checksum*/,
          true /*micro_checksum*/,
          ObPGKey(table_schema_.get_tablegroup_id(), 1, table_schema_.get_partition_cnt())));
  ASSERT_EQ(OB_SUCCESS, writer.open(data_desc, start_seq));

  int64_t count = BASE_ROW_COUNT;
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
    COMMON_LOG(INFO, "add base row", K(row));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }

  // close sstable
  ASSERT_EQ(OB_SUCCESS, writer.close());
}

void TestPartitionMergeUtil::create_inc_sstable1()
{
  int64_t data_version = 0;
  ObStoreRow row;
  ObObj cells[multi_version_row_info_->column_cnt_];
  uint16_t col_ids[multi_version_row_info_->column_cnt_];
  row.row_val_.assign(cells, multi_version_row_info_->column_cnt_);
  row.column_ids_ = col_ids;
  ObDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObITable::TableKey table_key;
  const bool is_compacted_row = true;
  const bool is_last_row = true;
  const bool is_first_row = true;

  table_key.table_type_ = ObITable::MULTI_VERSION_MINOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, table_key.pkey_.init(table_schema_.get_table_id(), 1, 0));
  table_key.table_id_ = table_schema_.get_table_id();
  table_key.trans_version_range_.base_version_ = 1;
  table_key.trans_version_range_.multi_version_start_ = 1;
  table_key.trans_version_range_.snapshot_version_ = 100;
  ASSERT_EQ(true, table_key.is_valid());

  ASSERT_EQ(OB_SUCCESS,
      data_desc.init(table_schema_,
          data_version,
          multi_version_row_info_,
          1 /*partition id*/,
          MINOR_MERGE /*is major*/,
          false /*column_checksum*/,
          false /*micro_checksum*/,
          ObPGKey(table_schema_.get_data_table_id(), 1, table_schema_.get_partition_cnt())));
  ASSERT_EQ(OB_SUCCESS, writer.open(data_desc, start_seq));

  int64_t count = BASE_ROW_COUNT;
  int64_t start_id = BASE_ROW_COUNT;
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        inc_row_generate_.get_next_row(start_id + i,
            90 /*trans_version*/,
            T_DML_INSERT,
            T_DML_INSERT,
            is_compacted_row,
            is_last_row,
            is_first_row,
            row));
    if (OB_FAIL(assign_column_id(row))) {
      STORAGE_LOG(WARN, "add inc row 1 failed", K(row));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    COMMON_LOG(INFO, "add inc row 1", K(row));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }

  // close sstable
  ASSERT_EQ(OB_SUCCESS, writer.close());
}

int TestPartitionMergeUtil::assign_column_id(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row.column_ids_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    row.is_sparse_row_ = true;
    int index = 0;
    for (int i = 0; i < TEST_ROWKEY_COLUMN_CNT; ++i) {
      row.column_ids_[i] = columns_[index++].col_id_;
    }
    row.column_ids_[TEST_ROWKEY_COLUMN_CNT] = OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
    row.column_ids_[TEST_ROWKEY_COLUMN_CNT + 1] = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
    for (int i = TEST_ROWKEY_COLUMN_CNT + 2; i < row.row_val_.count_; ++i) {
      row.column_ids_[i] = columns_[index++].col_id_;
    }
  }
  return ret;
}

void TestPartitionMergeUtil::create_inc_sstable2()
{
  int64_t data_version = 0;
  ObStoreRow row;
  ObObj cells[multi_version_row_info_->column_cnt_];
  uint16_t col_ids[multi_version_row_info_->column_cnt_];
  row.row_val_.assign(cells, multi_version_row_info_->column_cnt_);
  row.column_ids_ = col_ids;
  ObDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObITable::TableKey table_key;
  const bool is_compacted_row = true;
  const bool is_last_row = true;
  const bool is_first_row = true;

  table_key.table_type_ = ObITable::MULTI_VERSION_MINOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, table_key.pkey_.init(table_schema_.get_table_id(), 1, 0));
  table_key.table_id_ = table_schema_.get_table_id();
  table_key.trans_version_range_.base_version_ = 100;
  table_key.trans_version_range_.multi_version_start_ = 100;
  table_key.trans_version_range_.snapshot_version_ = 200;
  table_schema_.set_row_store_type(SPARSE_ROW_STORE);

  ASSERT_EQ(OB_SUCCESS,
      data_desc.init(table_schema_,
          data_version,
          multi_version_row_info_,
          1 /*partition id*/,
          MINOR_MERGE /*is major*/,
          false /*column_checksum*/,
          false /*micro_checksum*/,
          ObPGKey(table_schema_.get_data_table_id(), 1, table_schema_.get_partition_cnt())));
  ASSERT_EQ(OB_SUCCESS, writer.open(data_desc, start_seq));

  int64_t start_id = BASE_ROW_COUNT / 2;
  int64_t count = BASE_ROW_COUNT;
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        inc_row_generate_.get_next_row(start_id + i,
            150 /*trans_version*/,
            T_DML_DELETE,
            T_DML_DELETE,
            is_compacted_row,
            is_last_row,
            is_first_row,
            row));
    if (OB_FAIL(assign_column_id(row))) {
      STORAGE_LOG(WARN, "add inc row 2 failed", K(row));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "add inc row 2", K(row));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }

  ASSERT_EQ(OB_SUCCESS,
      inc_row_generate_.get_next_row(BASE_ROW_COUNT * 2,
          200 /*trans_version*/,
          T_DML_DELETE,
          T_DML_DELETE,
          is_compacted_row,
          is_last_row,
          is_first_row,
          row));
  if (OB_FAIL(assign_column_id(row))) {
    STORAGE_LOG(WARN, "add inc row 2 failed", K(row));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "add inc row 2", K(row));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));

  // close sstable
  ASSERT_EQ(OB_SUCCESS, writer.close());
}
// Base
// 0
// 1
//
//
// Inc1 (1,1,100]
// 2 version 90
// 3 version 90
//
// Inc2 (100,100,200]
// Del 1 version 150
// Del 2 version150
// Del 4 version200
//
//
// New Major (1,200, 200]
// 0
// 3

TEST_F(TestPartitionMergeUtil, test_major_merge)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtableCtxFactory memctx_factory;
  ObArenaAllocator allocator;
  ObTenantSchema tenant_schema;
  MockObIPartitionReport report;

  storage::ObSSTableMergeCtx ctx;

  ctx.data_table_schema_ = &table_schema_;
  ctx.is_full_merge_ = false;
  ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  ctx.report_ = &report;
  ctx.param_.merge_type_ = MAJOR_MERGE;
  ctx.param_.schedule_merge_type_ = MAJOR_MERGE;
  ctx.param_.merge_version_ = 2;
  ctx.logical_data_version_ = 2;
  ctx.param_.index_id_ = table_schema_.get_table_id();
  ctx.param_.pkey_.init(table_schema_.get_table_id(), 0, 0);
  ctx.param_.pg_key_ = ctx.param_.pkey_;
  ctx.report_ = &report;

  ASSERT_EQ(OB_SUCCESS,
      ctx.merge_context_.init(1 /*merge count*/, false /*has lob*/, &ctx.column_stats_, false /*merge memtable*/));
  ASSERT_EQ(OB_SUCCESS, ctx.tables_handle_.add_table(base_handle_));
  ASSERT_EQ(OB_SUCCESS, ctx.tables_handle_.add_table(inc_handle1_));
  ASSERT_EQ(OB_SUCCESS, ctx.tables_handle_.add_table(inc_handle2_));
  ASSERT_EQ(OB_SUCCESS, ctx.base_table_handle_.assign(base_handle_));

  ctx.sstable_version_range_.base_version_ = 0;
  ctx.sstable_version_range_.multi_version_start_ = 200;
  ctx.sstable_version_range_.snapshot_version_ = 200;
  ctx.create_snapshot_version_ = 1;
  ctx.schema_version_ = table_schema_.get_schema_version();
  ctx.base_schema_version_ = table_schema_.get_schema_version();
  ctx.table_schema_ = &table_schema_;
  ctx.data_table_schema_ = &table_schema_;
  ctx.stat_sampling_ratio_ = 0;
  ctx.progressive_merge_num_ = 0;
  ctx.progressive_merge_start_version_ = 0;
  ctx.mv_dep_table_schema_ = NULL;
  ASSERT_EQ(OB_SUCCESS, ctx.init_parallel_merge());
  ASSERT_TRUE(ctx.is_valid());

  table_schema_.row_store_type_ = FLAT_ROW_STORE;
  ObMacroBlockBuilder builder;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&memctx_factory, ctx, builder, 0 /*idx*/));
}

TEST_F(TestPartitionMergeUtil, test_build_bloomfilter)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtableCtxFactory memctx_factory;
  ObArenaAllocator allocator;
  ObTenantSchema tenant_schema;
  MockObIPartitionReport report;

  storage::ObSSTableMergeCtx ctx;

  table_schema_.set_is_use_bloomfilter(true);
  ctx.data_table_schema_ = &table_schema_;
  ctx.is_full_merge_ = false;
  ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  ctx.report_ = &report;
  ctx.param_.merge_type_ = MAJOR_MERGE;
  ctx.param_.schedule_merge_type_ = MAJOR_MERGE;
  ctx.param_.merge_version_ = 2;
  ctx.logical_data_version_ = 2;
  ctx.param_.index_id_ = table_schema_.get_table_id();
  ctx.param_.pkey_.init(table_schema_.get_table_id(), 0, 0);
  ctx.param_.pg_key_ = ctx.param_.pkey_;
  ctx.report_ = &report;

  ASSERT_EQ(OB_SUCCESS,
      ctx.merge_context_.init(1 /*merge count*/, false /*has lob*/, &ctx.column_stats_, false /*merge memtable*/));
  ASSERT_EQ(OB_SUCCESS, ctx.tables_handle_.add_table(base_handle_));
  ASSERT_EQ(OB_SUCCESS, ctx.tables_handle_.add_table(inc_handle1_));
  ASSERT_EQ(OB_SUCCESS, ctx.base_table_handle_.assign(base_handle_));

  ctx.sstable_version_range_.base_version_ = 0;
  ctx.sstable_version_range_.multi_version_start_ = 200;
  ctx.sstable_version_range_.snapshot_version_ = 200;
  ctx.create_snapshot_version_ = 1;
  ctx.schema_version_ = table_schema_.get_schema_version();
  ctx.base_schema_version_ = table_schema_.get_schema_version();
  ctx.table_schema_ = &table_schema_;
  ctx.data_table_schema_ = &table_schema_;
  ctx.stat_sampling_ratio_ = 0;
  ctx.progressive_merge_num_ = 0;
  ctx.progressive_merge_start_version_ = 0;
  ctx.mv_dep_table_schema_ = NULL;
  ASSERT_EQ(OB_SUCCESS, ctx.init_parallel_merge());
  ctx.bf_rowkey_prefix_ = 2;

  ObPartitionGroup pg;
  ObPGMgr pgmg;
  ASSERT_EQ(OB_SUCCESS, pg.try_switch_partition_state(ObPartitionState::F_WORKING));
  ASSERT_EQ(OB_SUCCESS, pg.try_switch_partition_state(ObPartitionState::L_TAKEOVER));
  ASSERT_FALSE(is_follower_state(pg.get_partition_state()));
  ctx.pg_guard_.set_partition_group(pgmg, pg);

  ASSERT_TRUE(ctx.is_valid());

  ObMacroBlockBuilder builder;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&memctx_factory, ctx, builder, 0 /*idx*/));
  ASSERT_EQ(1, builder.get_data_store_desc()->merge_info_->macro_bloomfilter_count_);
  bool is_contain = false;
  ObStoreRowkey rowkey;
  ObObj obj[TEST_ROWKEY_COLUMN_CNT];
  for (int i = 0; i < TEST_ROWKEY_COLUMN_CNT; ++i) {
    obj[i].set_int(0);
  }
  rowkey.assign(obj, TEST_ROWKEY_COLUMN_CNT);

  ObStoreRowkey rowkey2;
  ObObj obj2[TEST_ROWKEY_COLUMN_CNT];
  for (int i = 0; i < TEST_ROWKEY_COLUMN_CNT; ++i) {
    obj2[i].set_int(10);
  }
  rowkey2.assign(obj2, TEST_ROWKEY_COLUMN_CNT);
}

TEST_F(TestPartitionMergeUtil, test_rowkey_prefix_bloomfilter)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtableCtxFactory memctx_factory;
  ObArenaAllocator allocator;
  ObTenantSchema tenant_schema;
  MockObIPartitionReport report;

  storage::ObSSTableMergeCtx ctx;

  table_schema_.set_is_use_bloomfilter(true);
  ctx.data_table_schema_ = &table_schema_;
  ctx.is_full_merge_ = false;
  ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  ctx.report_ = &report;
  ctx.param_.merge_type_ = MAJOR_MERGE;
  ctx.param_.schedule_merge_type_ = MAJOR_MERGE;
  ctx.param_.merge_version_ = 2;
  ctx.logical_data_version_ = 2;
  ctx.param_.index_id_ = table_schema_.get_table_id();
  ctx.param_.pkey_.init(table_schema_.get_table_id(), 0, 0);
  ctx.param_.pg_key_ = ctx.param_.pkey_;
  ctx.report_ = &report;

  ASSERT_EQ(OB_SUCCESS,
      ctx.merge_context_.init(1 /*merge count*/, false /*has lob*/, &ctx.column_stats_, false /*merge memtable*/));
  ASSERT_EQ(OB_SUCCESS, ctx.tables_handle_.add_table(base_handle_));
  ASSERT_EQ(OB_SUCCESS, ctx.tables_handle_.add_table(inc_handle1_));
  ASSERT_EQ(OB_SUCCESS, ctx.base_table_handle_.assign(base_handle_));

  ctx.sstable_version_range_.base_version_ = 0;
  ctx.sstable_version_range_.multi_version_start_ = 200;
  ctx.sstable_version_range_.snapshot_version_ = 200;
  ctx.create_snapshot_version_ = 1;
  ctx.schema_version_ = table_schema_.get_schema_version();
  ctx.base_schema_version_ = table_schema_.get_schema_version();
  ctx.table_schema_ = &table_schema_;
  ctx.data_table_schema_ = &table_schema_;
  ctx.stat_sampling_ratio_ = 0;
  ctx.progressive_merge_num_ = 0;
  ctx.progressive_merge_start_version_ = 0;
  ctx.mv_dep_table_schema_ = NULL;
  ASSERT_EQ(OB_SUCCESS, ctx.init_parallel_merge());
  ctx.bf_rowkey_prefix_ = 1;

  ObPartitionGroup pg;
  ObPGMgr pgmg;
  ASSERT_EQ(OB_SUCCESS, pg.try_switch_partition_state(ObPartitionState::F_WORKING));
  ASSERT_EQ(OB_SUCCESS, pg.try_switch_partition_state(ObPartitionState::L_TAKEOVER));
  ASSERT_FALSE(is_follower_state(pg.get_partition_state()));
  ctx.pg_guard_.set_partition_group(pgmg, pg);
  ASSERT_TRUE(ctx.is_valid());

  ObMacroBlockBuilder builder;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&memctx_factory, ctx, builder, 0 /*idx*/));
  ASSERT_EQ(1, builder.get_data_store_desc()->merge_info_->macro_bloomfilter_count_);
  bool is_contain = false;
  ObStoreRowkey rowkey;
  ObObj obj[2];
  obj[0].set_int(0);
  obj[1].set_int(0);
  rowkey.assign(obj, 2);

  ObStoreRowkey rowkey1;
  ObObj obj1[1];
  obj1[0].set_int(0);
  rowkey1.assign(obj1, 1);

  ObStoreRowkey rowkey2;
  ObObj obj2[2];
  obj2[0].set_int(10);
  obj2[1].set_int(10);
  rowkey2.assign(obj2, 2);

  ObStoreRowkey rowkey3;
  ObObj obj3[1];
  obj3[0].set_int(10);
  rowkey3.assign(obj3, 1);
}

}  // namespace compaction
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_partition_merge_util.log*");
  OB_LOGGER.set_file_name("test_partition_merge_util.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
