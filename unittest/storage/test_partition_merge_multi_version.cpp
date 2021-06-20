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
#include "lib/container/ob_iarray.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_partition_component_factory.h"
#include "blocksstable/ob_data_file_prepare.h"
#include "blocksstable/ob_row_generate.h"
#include "storage/ob_ms_row_iterator.h"
#include "observer/ob_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"

#include "common/cell/ob_cell_reader.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_i_store.h"
#include "share/ob_srv_rpc_proxy.h"

#include "storage/ob_i_table.h"
#include "storage/compaction/ob_partition_merge_builder.h"
#include "ob_multi_version_sstable_test.h"
#include "mock_ob_partition_report.h"
#include "storage/ob_sstable_merge_info_mgr.h"

#include "memtable/utils_rowkey_builder.h"
#include "memtable/utils_mock_row.h"

#include "storage/compaction/ob_partition_merge.h"
#include "ob_uncommitted_trans_test.h"

#include "mockcontainer/mock_ob_trans_service.h"
namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace rpc::frame;
using namespace memtable;
namespace storage {

typedef compaction::ObIPartitionMergeFuser::MERGE_ITER_ARRAY MERGE_ITER_ARRAY;
class TestMultiVersionMerge : public ObMultiVersionSSTableTest {
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestMultiVersionMerge() : ObMultiVersionSSTableTest("test_multi_version_merge")
  {}
  virtual ~TestMultiVersionMerge()
  {}

  virtual void SetUp()
  {
    init_tenant_mgr();
    ObTableMgr::get_instance().init();
    ObMultiVersionSSTableTest::SetUp();
    ObPartitionKey pkey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().init());
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().add_partition(pkey, pkey));
    ObPartitionKey& part = const_cast<ObPartitionKey&>(test_trans_part_ctx_.get_partition());
    part = pkey;
    // trans_service_.part_trans_ctx_mgr_.mgr_cache_.set(pkey.hash(), &test_trans_part_ctx_);
    trans_service_.part_trans_ctx_mgr_.partition_ctx_map_.insert_and_get(pkey, &test_trans_part_ctx_);
    store_ctx_.trans_table_guard_ = new transaction::ObTransStateTableGuard();
    ObPartitionService::get_instance().txs_ = &trans_service_;
  }
  virtual void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
    ObTableMgr::get_instance().destroy();
    ObPartitionService::get_instance().get_pg_index().destroy();
  }

  void prepare_merge_context(const storage::ObTablesHandle& tables_handle, const ObMergeType& merge_type,
      const bool is_full_merge, const ObVersionRange& trans_version_range, ObSSTableMergeCtx& merge_context,
      const ObObjType* type_list = NULL);

  void build_sstable(ObSSTableMergeCtx& ctx, ObSSTable*& merged_sstable);
  int create_sstable(const storage::ObCreateSSTableParamWithTable& param, ObTableHandle& handle);
  void build_two_sstable(ObSSTableMergeCtx& ctx, ObSSTable*& mini_sstable, ObSSTable*& complement_minor_sstable);
  void prepare_query_param(const ObVersionRange& trans_version_range);
  void prepare_schema(const ObObjType* type_list);
  void prepare_schema();
  void build_parallel_merge(ObSSTableMergeCtx& ctx, const char** rowkey_data);
  int init_tenant_mgr();
  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
  MockObIPartitionReport report_;
  share::schema::ObTenantSchema tenant_schema_;
  ObMockIterator range_iter_;
  transaction::MockObTransService trans_service_;
  TestUncommittedMinorMergeScan test_trans_part_ctx_;
  TestUncommittedMinorMergeScan scan_trans_part_ctx_;
};

void TestMultiVersionMerge::prepare_query_param(const ObVersionRange& trans_version_range)
{
  columns_.reset();
  param_.reset();
  context_.reset();
  block_cache_ws_.reset();
  projector_.reset();

  ObQueryFlag query_flag;
  query_flag.whole_macro_scan_ = true;
  query_flag.multi_version_minor_merge_ = true;

  ObColDesc col_desc;
  int multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int schema_rowkey_cnt = rowkey_cnt_ - multi_version_col_cnt;  // schema rowkey count
  int trans_version_col =
      ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(schema_rowkey_cnt, multi_version_col_cnt);
  int sql_no_col =
      ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(schema_rowkey_cnt, multi_version_col_cnt);
  for (int i = 0; i < column_cnt_; i++) {
    if (trans_version_col != i && sql_no_col != i) {
      col_desc.col_id_ = i + OB_APP_MIN_COLUMN_ID;
      col_desc.col_type_ = data_iter_[0].get_column_type()[i];
      OK(columns_.push_back(col_desc));
    }
  }
  for (int i = 0; i < column_cnt_ - multi_version_col_cnt; i++) {
    OK(projector_.push_back(i));
  }

  param_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  param_.schema_version_ = SCHEMA_VERSION;
  param_.rowkey_cnt_ = schema_rowkey_cnt;  // schema rowkey count
  param_.out_cols_ = &columns_;

  OK(block_cache_ws_.init(TENANT_ID));
  context_.query_flag_ = query_flag;
  context_.store_ctx_ = &store_ctx_;
  context_.allocator_ = &allocator_;
  context_.stmt_allocator_ = &allocator_;
  context_.block_cache_ws_ = &block_cache_ws_;
  context_.trans_version_range_ = trans_version_range;
  context_.read_out_type_ = SPARSE_ROW_STORE;
  scan_trans_part_ctx_.clear_all();
  scan_trans_part_ctx_.set_all_transaction_status(transaction::ObTransTableStatusType::RUNNING);
  store_ctx_.trans_table_guard_->set_trans_state_table(&scan_trans_part_ctx_);
  context_.is_inited_ = true;
}

void TestMultiVersionMerge::prepare_schema()
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 16 * 1024;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = combine_id(tenant_id, TABLE_ID);
  ObColumnSchemaV2 column;

  // generate data table schema
  table_schema_.reset();
  ret = table_schema_.set_table_name("test_merge_multi_version");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema_.set_tenant_id(tenant_id);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(SPARSE_ROW_STORE);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObColDesc col_desc;
  const int64_t column_ids[] = {16, 17, 20, 21};
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = ObIntType;
    const int64_t column_id = column_ids[i];

    if (i == 1) {
      obj_type = ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
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
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    col_desc.col_id_ = column.get_column_id();
    col_desc.col_type_ = column.get_meta_type();
    ASSERT_EQ(OB_SUCCESS, columns_.push_back(col_desc));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema_));
}

void TestMultiVersionMerge::prepare_schema(const ObObjType* type_list)
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 16 * 1024;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = combine_id(tenant_id, TABLE_ID);
  ObColumnSchemaV2 column;

  // generate data table schema
  table_schema_.reset();
  ret = table_schema_.set_table_name("test_merge_multi_version");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema_.set_tenant_id(tenant_id);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(SPARSE_ROW_STORE);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObColDesc col_desc;
  const int64_t column_ids[] = {16, 17, 20, 21};
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = type_list[i];
    const int64_t column_id = column_ids[i];
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
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
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    col_desc.col_id_ = column.get_column_id();
    col_desc.col_type_ = column.get_meta_type();
    ASSERT_EQ(OB_SUCCESS, columns_.push_back(col_desc));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema_));
}

void TestMultiVersionMerge::prepare_merge_context(const storage::ObTablesHandle& tables_handle,
    const ObMergeType& merge_type, const bool is_full_merge, const ObVersionRange& trans_version_range,
    ObSSTableMergeCtx& merge_context, const ObObjType* type_list /* = NULL*/)
{
  ObMultiVersionColDescGenerate multi_version_col_desc_gen;
  const ObMultiVersionRowInfo* multi_version_row_info = NULL;
  bool has_lob = false;
  bool has_memtable = false;
  if (OB_NOT_NULL(type_list)) {
    prepare_schema(type_list);
  } else {
    prepare_schema();
  }

  merge_context.base_schema_version_ = table_schema_.get_schema_version();
  merge_context.schema_version_ = table_schema_.get_schema_version();
  ASSERT_EQ(OB_SUCCESS, merge_context.tables_handle_.assign(tables_handle));
  merge_context.data_table_schema_ = &table_schema_;
  merge_context.is_full_merge_ = is_full_merge;
  merge_context.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  merge_context.report_ = &report_;
  merge_context.param_.merge_type_ = merge_type;
  merge_context.param_.schedule_merge_type_ = merge_type;
  merge_context.param_.merge_version_ = 0;
  merge_context.param_.index_id_ = table_schema_.get_table_id();
  merge_context.param_.pkey_.init(table_schema_.get_table_id(), 0 /*partition id*/, 0 /*partition count*/);
  merge_context.param_.pg_key_ = merge_context.param_.pkey_;
  merge_context.sstable_version_range_ = trans_version_range;
  merge_context.mv_dep_table_schema_ = NULL;
  merge_context.table_schema_ = &table_schema_;
  merge_context.logical_data_version_ = trans_version_range.snapshot_version_;

  ObStorageFile* file = NULL;
  ASSERT_EQ(OB_SUCCESS,
      ObFileSystemUtil::get_pg_file_with_guard(merge_context.param_.pg_key_, merge_context.pg_guard_, file));

  ASSERT_EQ(OB_SUCCESS, multi_version_col_desc_gen.init(&table_schema_));

  ASSERT_EQ(OB_SUCCESS, multi_version_col_desc_gen.generate_multi_version_row_info(multi_version_row_info));
  COMMON_LOG(INFO,
      "dump multi_version_row_info",
      "rowley_cnt",
      multi_version_row_info->rowkey_column_cnt_,
      "multi_version_rowkey_cnt",
      multi_version_row_info->multi_version_rowkey_column_cnt_);
  merge_context.multi_version_row_info_ = *multi_version_row_info;
  merge_context.multi_version_row_info_.trans_version_index_ = 2;
  merge_context.progressive_merge_start_version_ = 0;
  merge_context.progressive_merge_num_ = 0;

  for (int64_t i = 0; i < tables_handle.get_count(); ++i) {
    if (tables_handle.get_table(i)->is_memtable()) {
      has_memtable = true;
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_context.init_parallel_merge());
  ASSERT_EQ(OB_SUCCESS,
      merge_context.merge_context_.init(1 /*merge count*/, has_lob, &merge_context.column_stats_, has_memtable));
  ASSERT_EQ(OB_SUCCESS,
      merge_context.merge_context_for_complement_minor_sstable_.init(
          1 /*merge count*/, has_lob, &merge_context.column_stats_, has_memtable));
}

int TestMultiVersionMerge::create_sstable(const storage::ObCreateSSTableParamWithTable& param, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObSSTable* table = NULL;
  void* buf = nullptr;

  if (!param.is_valid() || (!ObITable::is_sstable(param.table_key_.table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(param));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for sstable", K(ret));
  } else if (FALSE_IT(table = new (buf) ObSSTable())) {
  } else if (OB_FAIL(table->init(param.table_key_))) {
    STORAGE_LOG(WARN, "failed to init sstable", K(ret), K(param.table_key_));
  } else if (OB_FAIL(table->set_storage_file_handle(get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "failed to set storage file handle", K(ret));
  } else if (OB_FAIL(table->open(param))) {
    STORAGE_LOG(WARN, "Failed to open sstable", K(ret));
  } else if (OB_FAIL(handle.set_table(table))) {
    STORAGE_LOG(WARN, "failed to set table to handle", K(ret));
  }
  return ret;
}

int TestMultiVersionMerge::init_tenant_mgr()
{
  ObTenantManager& tm = ObTenantManager::get_instance();
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  obrpc::ObCommonRpcProxy common_rpc_proxy;
  share::ObRsMgr rs_mgr;

  int ret = tm.init(self, rpc_proxy, common_rpc_proxy, rs_mgr, &req_transport, &ObServerConfig::get_instance());
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 16LL << 30;
  const int64_t llmt = 8LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  return OB_SUCCESS;
}

void TestMultiVersionMerge::build_sstable(ObSSTableMergeCtx& ctx, ObSSTable*& merged_sstable)
{
  ObCreateSSTableParamWithTable param;

  param.table_key_.table_type_ = ctx.get_merged_table_type();
  param.table_key_.pkey_ = ctx.param_.pkey_;
  param.table_key_.table_id_ = ctx.param_.index_id_;
  param.table_key_.version_ = ctx.param_.merge_version_;
  param.table_key_.trans_version_range_ = ctx.sstable_version_range_;
  param.table_key_.log_ts_range_.start_log_ts_ = 0;
  param.table_key_.log_ts_range_.end_log_ts_ = 10;
  param.table_key_.log_ts_range_.max_log_ts_ = 10;
  param.logical_data_version_ = param.table_key_.trans_version_range_.snapshot_version_;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;

  int ret = OB_SUCCESS;
  void* buf = nullptr;

  storage::ObTableHandle table_handle;
  OB_FILE_SYSTEM.get_partition_service().get_pg_key(ctx.param_.pkey_, param.pg_key_);
  ASSERT_EQ(OB_SUCCESS, create_sstable(param, table_handle));

  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(merged_sstable));
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.merge_context_.block_ctxs_.count(); ++i) {
    if (OB_ISNULL(ctx.merge_context_.block_ctxs_.at(i))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "data block ctx must not be null", K(ret));
    } else {
      ASSERT_EQ(OB_SUCCESS, merged_sstable->append_macro_blocks(*ctx.merge_context_.block_ctxs_.at(i)));
    }
  }
  ASSERT_EQ(OB_SUCCESS, merged_sstable->close());
  /*
  if (OB_FAIL(ctx.merge_context_.create_sstable(param, ctx.pg_guard_, table_handle))) {
    STORAGE_LOG(WARN, "fail to create sstable", K(ret), K(ctx.param_), K(table_handle));
  } else if (OB_FAIL(table_handle.get_sstable(merged_sstable))) {
    STORAGE_LOG(WARN, "fail to get sstable", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, create_sstable(param, ctx.merged_table_handle_));
  ASSERT_EQ(OB_SUCCESS, ctx.merged_table_handle_.get_sstable(merged_sstable));
  ASSERT_EQ(OB_SUCCESS, merged_sstable->close());
  */
  // ASSERT_EQ(OB_SUCCESS, ObTableMgr::get_instance().complete_sstable(ctx.merged_table_handle_));
}

void TestMultiVersionMerge::build_two_sstable(
    ObSSTableMergeCtx& ctx, ObSSTable*& mini_sstable, ObSSTable*& complement_minor_sstable)
{
  ObCreateSSTableParamWithTable param;

  param.table_key_.table_type_ = ctx.get_merged_table_type();
  param.table_key_.pkey_ = ctx.param_.pkey_;
  param.table_key_.table_id_ = ctx.param_.index_id_;
  param.table_key_.version_ = ctx.param_.merge_version_;
  param.table_key_.trans_version_range_ = ctx.sstable_version_range_;
  param.table_key_.log_ts_range_.start_log_ts_ = 0;
  param.table_key_.log_ts_range_.end_log_ts_ = 10;
  param.table_key_.log_ts_range_.max_log_ts_ = 10;

  param.logical_data_version_ = param.table_key_.trans_version_range_.snapshot_version_;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;

  ASSERT_EQ(OB_SUCCESS, create_sstable(param, ctx.merged_table_handle_));
  ASSERT_EQ(OB_SUCCESS, ctx.merged_table_handle_.get_sstable(mini_sstable));
  ASSERT_EQ(OB_SUCCESS, mini_sstable->close());
  // ASSERT_EQ(OB_SUCCESS, ObTableMgr::get_instance().complete_sstable(ctx.merged_table_handle_));
  param.table_key_.table_type_ = ObITable::COMPLEMENT_MINOR_SSTABLE;

  ASSERT_EQ(OB_SUCCESS, create_sstable(param, ctx.merged_complement_minor_table_handle_));
  ASSERT_EQ(OB_SUCCESS, ctx.merged_complement_minor_table_handle_.get_sstable(complement_minor_sstable));
  ASSERT_EQ(OB_SUCCESS, complement_minor_sstable->close());
  // ASSERT_EQ(OB_SUCCESS, ObTableMgr::get_instance().complete_sstable(ctx.merged_complement_minor_table_handle_));
}

void TestMultiVersionMerge::build_parallel_merge(ObSSTableMergeCtx& ctx, const char** rowkey_data)
{
  ObExtStoreRange merge_range;
  const ObStoreRow* row = nullptr;
  const int64_t rowkey_col_cnt = TEST_ROWKEY_COLUMN_CNT;
  const int64_t mv_rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_TRUE(nullptr != rowkey_data);
  range_iter_.reset();
  ctx.parallel_merge_ctx_.range_array_.reset();
  OK(range_iter_.from(rowkey_data[0]));
  ASSERT_TRUE(range_iter_.count() > 0);
  for (int64_t i = 0; i <= range_iter_.count(); i++) {
    if (0 == i) {
      merge_range.get_range().get_start_key().set_min();
      merge_range.get_range().set_left_open();
    }
    if (i == range_iter_.count()) {
      merge_range.get_range().get_end_key().set_max();
    } else {
      OK(range_iter_.get_row(i, row));
      ASSERT_TRUE(nullptr != row);
      merge_range.get_range().get_end_key().assign(row->row_val_.cells_, mv_rowkey_cnt);
      for (int64_t i = rowkey_col_cnt; i < mv_rowkey_cnt; i++) {
        merge_range.get_range().get_end_key().get_obj_ptr()[i].set_max_value();
      }
    }
    merge_range.get_range().set_right_closed();
    ASSERT_EQ(OB_SUCCESS, ctx.parallel_merge_ctx_.range_array_.push_back(merge_range));
    merge_range.reset();
    if (i < range_iter_.count()) {
      merge_range.get_range().get_start_key().assign(row->row_val_.cells_, mv_rowkey_cnt);
      for (int64_t i = rowkey_col_cnt; i < mv_rowkey_cnt; i++) {
        merge_range.get_range().get_start_key().get_obj_ptr()[i].set_max_value();
      }
      merge_range.get_range().set_left_open();
    }
  }
  ASSERT_TRUE(ctx.parallel_merge_ctx_.range_array_.count() > 0);
  for (int64_t i = 0; i < ctx.parallel_merge_ctx_.range_array_.count(); i++) {
    ASSERT_EQ(OB_SUCCESS,
        ctx.parallel_merge_ctx_.range_array_.at(i).to_collation_free_range_on_demand_and_cutoff_range(
            ctx.parallel_merge_ctx_.allocator_));
  }
  ctx.parallel_merge_ctx_.concurrent_cnt_ = ctx.parallel_merge_ctx_.range_array_.count();
  ctx.parallel_merge_ctx_.parallel_type_ = ObParallelMergeCtx::PARALLEL_MINI_MINOR;
  STORAGE_LOG(INFO, "parallel merge context prepared", K(ctx.parallel_merge_ctx_));
  ctx.merge_context_.destroy();
  ASSERT_EQ(OB_SUCCESS,
      ctx.merge_context_.init(
          ctx.parallel_merge_ctx_.concurrent_cnt_ /*merge count*/, false, &ctx.column_stats_, false));
}
/*  test use Trans_version as
TEST_F(TestMultiVersionMerge, test_mini_merge)
{
  prepare_schema();
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  storage::ObTablesHandle tables_handle;
  ObSSTableMergeInfoMgr::get_instance().init(1024 * 10);
  ObSSTable sstable1;
  int64_t trans_version = 0;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;
  table_key.log_ts_range_.start_log_ts_ = 0;
  table_key.log_ts_range_.end_log_ts_ = 10;
  table_key.log_ts_range_.max_log_ts_ = 10;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable *> array;
  wctx.tables_ = &array;

  CD cd(
    16, ObIntType, CS_TYPE_UTF8MB4_BIN,
    17, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    20, ObIntType, CS_TYPE_UTF8MB4_BIN,
    21, ObIntType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(
    I(998),
    V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI),
    I(1024),
    I(987)
    );
  for (int i = 0; i < columns.count(); ++i) {

    STORAGE_LOG(INFO, "columns", K(i), K(columns.at(i)));
  }
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj *>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  STORAGE_LOG(INFO, "row", K(row));
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = mem_ctx.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  ++trans_version;
  wctx.mem_ctx_->trans_end(true, trans_version);
  mem_ctx.free(wctx.mem_ctx_);

  for (int i = 0; i < 10; ++i) {
    row.set_dml(T_DML_UPDATE);
    mri.reset();

    row.row_val_.cells_[2].set_int(i * 11);
    row.row_val_.cells_[3].set_int(i * 111);
    mri.add_row(row);

    wctx.mem_ctx_ = mem_ctx.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
    ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
    EXPECT_EQ(OB_SUCCESS, ret);
    ++trans_version;
    wctx.mem_ctx_->trans_end(true, trans_version);
    mem_ctx.free(wctx.mem_ctx_);
  }
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&mt));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator *max_scanner = NULL;
  ObStoreRowIterator *mini_scanner = NULL;
  ObExtStoreRange range;

  ObMinorMergeMacroBlockBuilder builder;
  ObSSTable *complement_minor_sstable = nullptr;
  ObSSTable *mini_sstable = nullptr;
  mt.set_freeze_log_ts(5);
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder,  0));
  build_two_sstable(merge_context, mini_sstable, complement_minor_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  // set because not use prepare_* func to set these params
  rowkey_cnt_ = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  column_cnt_ = TEST_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  prepare_query_param(trans_version_range);
  param_.table_id_ = PKEY.table_id_;
  param_.is_multi_version_minor_merge_ = true;
  param_.out_cols_ = &columns;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint   dml          first_dml     flag    multi_version_row_flag\n"
      "998     GOGO   -11      0      99       999     T_DML_UPDATE T_DML_INSERT  EXIST   CF\n"
      "998     GOGO   -10      0      88       888     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -9       0      77       777     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -8       0      66       666     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -7       0      55       555     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -6       0      44       444     T_DML_UPDATE T_DML_INSERT  EXIST   L\n"; // Have Last Symbol

  ASSERT_EQ(OB_SUCCESS, complement_minor_sstable->scan(param_, context_, range, max_scanner));
  const ObStoreRow *read_row = nullptr;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*max_scanner, true));

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint   dml          first_dml     flag    multi_version_row_flag\n"
      "998     GOGO   -11      0      99       999     T_DML_UPDATE T_DML_INSERT  EXIST   CF\n"
      "998     GOGO   -10      0      88       888     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -9       0      77       777     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -8       0      66       666     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -7       0      55       555     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -6       0      44       444     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -5       0      33       333     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -4       0      22       222     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -3       0      11       111     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -2       0      0         0      T_DML_UPDATE T_DML_INSERT  EXIST   L\n";
  ASSERT_EQ(OB_SUCCESS, mini_sstable->scan(param_, context_, range, mini_scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*mini_scanner, true));
  max_scanner->~ObStoreRowIterator();
  mini_scanner->~ObStoreRowIterator();
}


TEST_F(TestMultiVersionMerge, test_mini_merge_with_gap_trans_version)
{
  prepare_schema();
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  storage::ObTablesHandle tables_handle;
  ObSSTableMergeInfoMgr::get_instance().init(1024 * 10);
  ObSSTable sstable1;
  int64_t trans_version = 0;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;
  table_key.log_id_range_.start_log_id_ = 0;
  table_key.log_id_range_.end_log_id_ = 10;
  table_key.log_id_range_.max_log_id_ = 10;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable *> array;
  wctx.tables_ = &array;

  CD cd(
    16, ObIntType, CS_TYPE_UTF8MB4_BIN,
    17, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    20, ObIntType, CS_TYPE_UTF8MB4_BIN,
    21, ObIntType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(
    I(998),
    V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI),
    I(1024),
    I(987)
    );
  for (int i = 0; i < columns.count(); ++i) {

    STORAGE_LOG(INFO, "columns", K(i), K(columns.at(i)));
  }
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj *>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  STORAGE_LOG(INFO, "row", K(row));
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = mem_ctx.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  ++trans_version;
  wctx.mem_ctx_->trans_end(true, trans_version);
  mem_ctx.free(wctx.mem_ctx_);

  int trans_version_cnt = 10;
  int trans_version_start_list[] = {
      2, 6, 9, 14, 85,
      102, 159, 190, 221, 281};
  int trans_version_end_list[] =   {
      5, 7, 10, 14, 87,
      157, 182, 201, 271, 290};

  for (int i = 0; i < 10; ++i) {
    row.set_dml(T_DML_UPDATE);
    mri.reset();

    row.row_val_.cells_[2].set_int(i * 11);
    row.row_val_.cells_[3].set_int(i * 111);
    mri.add_row(row);

    wctx.mem_ctx_ = mem_ctx.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(trans_version_start_list[i], 1000000 +
::oceanbase::common::ObTimeUtility::current_time()); ret = mt.set(wctx, table_schema_.get_table_id(),
rk.get_rowkey().get_obj_cnt() - 2, columns, mri); EXPECT_EQ(OB_SUCCESS, ret); wctx.mem_ctx_->trans_end(true,
trans_version_end_list[i]); mem_ctx.free(wctx.mem_ctx_);
  }
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&mt));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 500;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator *max_scanner = NULL;
  ObStoreRowIterator *mini_scanner = NULL;
  ObExtStoreRange range;

  ObMinorMergeMacroBlockBuilder builder;
  ObSSTable *complement_minor_sstable = nullptr;
  ObSSTable *mini_sstable = nullptr;
  mt.set_freeze_log_id(87);
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder,  0));
  build_two_sstable(merge_context, mini_sstable, complement_minor_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  // set because not use prepare_* func to set these params
  rowkey_cnt_ = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  column_cnt_ = TEST_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  prepare_query_param(trans_version_range);
  param_.table_id_ = PKEY.table_id_;
  param_.is_multi_version_minor_merge_ = true;
  param_.out_cols_ = &columns;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint   dml          first_dml     flag    multi_version_row_flag\n"
      "998     GOGO   -290      0      99       999     T_DML_UPDATE T_DML_INSERT  EXIST   CF\n"
      "998     GOGO   -271      0      88       888     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -201      0      77       777     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -182      0      66       666     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -157      0      55       555     T_DML_UPDATE T_DML_INSERT  EXIST   L\n";// Have Last Symbol

  ASSERT_EQ(OB_SUCCESS, complement_minor_sstable->scan(param_, context_, range, max_scanner));
  const ObStoreRow *read_row = nullptr;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*max_scanner, true));

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint   dml          first_dml     flag    multi_version_row_flag\n"
      "998     GOGO   -290      0      99       999     T_DML_UPDATE T_DML_INSERT  EXIST   CF\n"
      "998     GOGO   -271      0      88       888     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -201      0      77       777     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -182      0      66       666     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -157      0      55       555     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -87       0      44       444     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -14       0      33       333     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -10       0      22       222     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -7        0      11       111     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -5        0      0         0      T_DML_UPDATE T_DML_INSERT  EXIST   L\n";
  ASSERT_EQ(OB_SUCCESS, mini_sstable->scan(param_, context_, range, mini_scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*mini_scanner, true));

  max_scanner->~ObStoreRowIterator();
  mini_scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionMerge, test_mini_merge_with_empty_complement_minor_sstable)
{
  prepare_schema();
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  storage::ObTablesHandle tables_handle;
  ObSSTableMergeInfoMgr::get_instance().init(1024 * 10);
  ObSSTable sstable1;
  int64_t trans_version = 0;
  static const ObPartitionKey PKEY(table_schema_.get_table_id(), 1, 1);
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::MEMTABLE;
  table_key.pkey_ = PKEY;
  table_key.table_id_ = PKEY.table_id_;
  table_key.version_ = 1;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX - 2;
  table_key.log_id_range_.start_log_id_ = 0;
  table_key.log_id_range_.end_log_id_ = 10;
  table_key.log_id_range_.max_log_id_ = 10;

  ret = mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(table_key);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;
  ObArray<ObITable *> array;
  wctx.tables_ = &array;

  CD cd(
    16, ObIntType, CS_TYPE_UTF8MB4_BIN,
    17, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI,
    20, ObIntType, CS_TYPE_UTF8MB4_BIN,
    21, ObIntType, CS_TYPE_UTF8MB4_BIN
    );
  const ObIArray<share::schema::ObColDesc> &columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(
    I(998),
    V("GOGO", 4, CS_TYPE_UTF8MB4_GENERAL_CI),
    I(1024),
    I(987)
    );
  for (int i = 0; i < columns.count(); ++i) {

    STORAGE_LOG(INFO, "columns", K(i), K(columns.at(i)));
  }
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj *>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  STORAGE_LOG(INFO, "row", K(row));
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);
  wctx.mem_ctx_ = mem_ctx.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(trans_version, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, table_schema_.get_table_id(), rk.get_rowkey().get_obj_cnt() - 2, columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  ++trans_version;
  wctx.mem_ctx_->trans_end(true, trans_version);
  mem_ctx.free(wctx.mem_ctx_);

  int trans_version_cnt = 10;
  int trans_version_start_list[] = {
      2, 6, 9, 14, 85,
      102, 159, 190, 221, 281};
  int trans_version_end_list[] =   {
      5, 7, 10, 14, 87,
      157, 182, 201, 271, 290};

  for (int i = 0; i < 10; ++i) {
    row.set_dml(T_DML_UPDATE);
    mri.reset();

    row.row_val_.cells_[2].set_int(i * 11);
    row.row_val_.cells_[3].set_int(i * 111);
    mri.add_row(row);

    wctx.mem_ctx_ = mem_ctx.alloc();
    wctx.mem_ctx_->trans_begin();
    wctx.mem_ctx_->sub_trans_begin(trans_version_start_list[i], 1000000 +
::oceanbase::common::ObTimeUtility::current_time()); ret = mt.set(wctx, table_schema_.get_table_id(),
rk.get_rowkey().get_obj_cnt() - 2, columns, mri); EXPECT_EQ(OB_SUCCESS, ret); wctx.mem_ctx_->trans_end(true,
trans_version_end_list[i]); mem_ctx.free(wctx.mem_ctx_);
  }
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&mt));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 500;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator *max_scanner = NULL;
  ObStoreRowIterator *mini_scanner = NULL;
  ObExtStoreRange range;

  ObMinorMergeMacroBlockBuilder builder;
  ObSSTable *complement_minor_sstable = nullptr;
  ObSSTable *mini_sstable = nullptr;
  mt.set_freeze_log_id(290); // make max sstable empty
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder,  0));
  build_two_sstable(merge_context, mini_sstable, complement_minor_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  // set because not use prepare_* func to set these params
  rowkey_cnt_ = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  column_cnt_ = TEST_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  prepare_query_param(trans_version_range);
  param_.table_id_ = PKEY.table_id_;
  param_.is_multi_version_minor_merge_ = true;
  param_.out_cols_ = &columns;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  ASSERT_EQ(OB_SUCCESS, complement_minor_sstable->scan(param_, context_, range, max_scanner));
  const ObStoreRow *read_row = nullptr;
  ASSERT_EQ(OB_ITER_END, max_scanner->get_next_row(read_row));

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint   dml          first_dml     flag    multi_version_row_flag\n"
      "998     GOGO   -290      0      99       999     T_DML_UPDATE T_DML_INSERT  EXIST   CF\n"
      "998     GOGO   -271      0      88       888     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -201      0      77       777     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -182      0      66       666     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -157      0      55       555     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -87       0      44       444     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -14       0      33       333     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -10       0      22       222     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -7        0      11       111     T_DML_UPDATE T_DML_INSERT  EXIST   N\n"
      "998     GOGO   -5        0      0         0      T_DML_UPDATE T_DML_INSERT  EXIST   L\n";
  ASSERT_EQ(OB_SUCCESS, mini_sstable->scan(param_, context_, range, mini_scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*mini_scanner, true));

  max_scanner->~ObStoreRowIterator();
  mini_scanner->~ObStoreRowIterator();
} */

// TEST_F(TestMultiVersionMerge, rowkey_corss_two_macro_and_second_macro_is_filtered)
//{
//  ObMemtableCtxFactory mem_ctx;
//  ObSSTableMergeCtx merge_context;
//  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + 1;
//  storage::ObTablesHandle tables_handle;
//  ObSSTable sstable1;
//  const char *micro_data[3];
//  micro_data[0] =
//      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//      "0        var1  -8       2        NOP     EXIST   CLF\n"
//      "1        var1  -8       2        2       EXIST   CLF\n";
//
//  micro_data[1] =
//      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//      "2        var1  -8       2        2       EXIST   CF\n";
//
//  micro_data[2] =
//      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//      "2        var1  -6       3        2       EXIST   L\n";
//
//  prepare_data_start(sstable1, micro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
//  prepare_one_macro(micro_data, 2);
//  prepare_one_macro(&micro_data[2], 1);
//  prepare_data_end(sstable1);
//  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));
//
//  STORAGE_LOG(INFO, "finish prepare sstable1");
//
//  ObSSTable sstable2;
//  const char *micro_data2[1];
//  micro_data2[0] =
//      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//      "0        var1  -10       3        NOP     EXIST   CLF\n"
//      "2        var1  -10       3        NOP     EXIST   CLF\n"
//      "3        var1  -10       3        NOP     EXIST   CLF\n";
//
//  prepare_data_start(sstable2, micro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
//  prepare_one_macro(micro_data2, 1);
//  prepare_data_end(sstable2);
//  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
//  STORAGE_LOG(INFO, "finish prepare sstable2");
//
//  ObVersionRange trans_version_range;
//  trans_version_range.snapshot_version_ = 100;
//  trans_version_range.multi_version_start_ = 7;
//  trans_version_range.base_version_ = 7;
//
//  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
//  ObMockIterator res_iter;
//  ObStoreRowIterator *scanner = NULL;
//  ObExtStoreRange range;
//
//  const char *result1 =
//      "bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//      "0        var1  -10      3        NOP       EXIST   CF\n"
//      "0        var1  -8       2        NOP       EXIST   CL\n"
//      "1        var1  -8       2        2       EXIST   CLF\n"
//      "2        var1  -10      3        2       EXIST   CF\n"
//      "2        var1  -8      2        2        EXIST    C\n"
//      "2        var1  -6      3        2       EXIST   CL\n"
//      "3        var1  -10       3        NOP     EXIST   CLF\n";
//
//  // minor mrege
//
//  ObMacroBlockBuilder builder;
//  ObSSTable *merged_sstable = nullptr;
//  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder,  0/*idx*/));
//  build_sstable(merge_context, merged_sstable);
//
//  res_iter.reset();
//  range.get_range().set_whole_range();
//  trans_version_range.base_version_ = 0;
//  prepare_query_param(trans_version_range);
//  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
//  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
//  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
//  ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
//  scanner->~ObStoreRowIterator();
//}
//
TEST_F(TestMultiVersionMerge, rowkey_cross_three_macro_inc_merge)
{
  GCONF._enable_sparse_row = true;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* micro_data[4];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1  -8       0       2        NOP     EXIST   CLF\n"
                  "1        var1  -8       0       2        2       EXIST   CF\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -7       0       NOP      2       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -6       0       NOP      2       EXIST   N\n";

  micro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -5       0       2        NOP     EXIST   CL\n"
                  "2        var1  -5       0       2        NOP     EXIST   CLF\n";

  prepare_data_start(sstable1, micro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* micro_data2[1];
  micro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var1  -10      0        3        NOP     EXIST   CLF\n"
                   "2        var1  -10      0        3        NOP     EXIST   CLF\n";

  prepare_data_start(sstable2, micro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -10      0       3        NOP     EXIST   CF\n"
                        "0        var1  -8       0       2        NOP     EXIST   CL\n"
                        "1        var1  -8       0       2        2       EXIST   CF\n"
                        "1        var1  -7       0       2        NOP     EXIST   N\n"
                        "1        var1  -6       0       2        NOP     EXIST   N\n"  // 5
                        "1        var1  -5       0       2        NOP     EXIST   CL\n"
                        "2        var1  -10      0       3        NOP     EXIST   CF\n"
                        "2        var1  -5       0       2        NOP     EXIST   CL\n";
  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,  // 5
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20};
  int64_t result_col_cnt[] = {5, 5, 6, 5, 5, 5, 5, 5};
  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  if (GCONF._enable_sparse_row == true) {
    STORAGE_LOG(INFO, "sparse row");
  } else {
    STORAGE_LOG(INFO, "flat row");
  }
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0 /*idx*/));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1, '\\', result_col_id, result_col_cnt));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, rowkey_cross_three_macro_full_merge)
{
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* micro_data[4];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1  -8       0        2        NOP     EXIST   CLF\n"
                  "1        var1  -8       0        2        2       EXIST   CF\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -7       0        NOP      2       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -6       0        NOP      2       EXIST   N\n";

  micro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -5       0        2        NOP     EXIST    CL\n"
                  "2        var1  -5       0        2        NOP     EXIST    CLF\n";

  prepare_data_start(sstable1, micro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* micro_data2[1];
  micro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var1  -10      0         3        NOP     EXIST   CLF\n"
                   "2        var1  -10      0         3        NOP     EXIST   CLF\n";

  prepare_data_start(sstable2, micro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, true /*is_full_merge*/, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -10      0        3        NOP     EXIST   CF\n"
                        "0        var1  -8       0        2        NOP     EXIST   CL\n"
                        "1        var1  -8       0        2        2       EXIST   CF\n"
                        "1        var1  -7       0        2        NOP     EXIST   N\n"
                        "1        var1  -6       0        2        NOP     EXIST   N\n"  // 5
                        "1        var1  -5       0        2        NOP     EXIST   CL\n"
                        "2        var1  -10      0        3        NOP     EXIST   CF\n"
                        "2        var1  -5       0        2        NOP     EXIST   CL\n";
  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,  // 5
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20};
  int64_t result_col_cnt[] = {5, 5, 6, 5, 5, 5, 5, 5};
  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0 /*idx*/));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1, '\\', result_col_id, result_col_cnt));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, sub_version_range_for_logic_migrate)
{
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* micro_data[4];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1  -200     0      1        2       EXIST   CF\n"
                  "0        var1  -160     0      NOP      1       EXIST   N\n"
                  "0        var1  -150     0      NOP      2       EXIST   N\n"
                  "0        var1  -50      0      1        1       EXIST   CL\n";

  prepare_data_start(sstable1, micro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -200    0      1        2       EXIST   CF\n"
                        "0        var1  -160    0      1        NOP     EXIST   N\n"
                        "0        var1  -150    0      2        NOP     EXIST   N\n"
                        "0        var1  -50     0      1        1       EXIST   CL\n";

  uint16_t result_col_id[] = {16, 17, 7, 8, 20, 21, 16, 17, 7, 8, 21, 16, 17, 7, 8, 21, 16, 17, 7, 8, 20, 21};
  int64_t result_col_cnt[] = {6, 5, 5, 6};
  res_iter.reset();
  range.get_range().set_whole_range();

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  ASSERT_EQ(OB_SUCCESS, sstable1.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -200     0      1        2       EXIST   CF\n"
                        "0        var1  -160     0      1        NOP     EXIST   N\n"
                        "0        var1  -150     0      2        NOP     EXIST   N\n"
                        "0        var1  -50      0      1        1       EXIST   CL\n";
  uint16_t result_col_id2[] = {16, 17, 7, 8, 20, 21, 16, 17, 7, 8, 21, 16, 17, 7, 8, 21, 16, 17, 7, 8, 20, 21};
  int64_t result_col_cnt2[] = {6, 5, 5, 6};
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 100;
  trans_version_range.base_version_ = 100;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  ASSERT_EQ(OB_SUCCESS, sstable1.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result2, '\\', result_col_id2, result_col_cnt2));
  ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -200     0     1        2       EXIST   CF\n"
                        "0        var1  -160     0     1        NOP     EXIST   N\n"
                        "0        var1  -150     0     2        1       EXIST   CL\n";
  uint16_t result_col_id3[] = {
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,
      20,
  };
  int64_t result_col_cnt3[] = {6, 5, 6};
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 155;
  trans_version_range.base_version_ = 155;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  ASSERT_EQ(OB_SUCCESS, sstable1.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result3, '\\', result_col_id3, result_col_cnt3));
  ASSERT_TRUE(res_iter.equals(*scanner, true));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionMerge, parallel_minor_merge)
{
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* micro_data[4];
  micro_data[0] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1  -8      0       2        NOP     EXIST   CLF\n"
                  "1        var1  -8      0       2        2       EXIST   CF\n";

  micro_data[1] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -7      0       NOP      2       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint  bigint   bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -6      0        NOP      2       EXIST   N\n";

  micro_data[3] = "bigint  var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1       var1  -5      0       2        NOP     EXIST   CL\n"
                  "2       var1  -5      0       2        NOP     EXIST   CLF\n"
                  "3       var1  -5      0       2        NOP     EXIST   CLF\n";

  prepare_data_start(sstable1, micro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* micro_data2[1];
  micro_data2[0] = "bigint   var     bigint  bigint   bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var1    -10     0        3        NOP     EXIST   CLF\n"
                   "2        var1    -10     0        3        NOP     EXIST   CLF\n"
                   "4        var1    -10     0        3        NOP     EXIST   CLF\n";

  prepare_data_start(sstable2, micro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);

  const char* rowkey_data[1];
  rowkey_data[0] = "bigint   var   bigint  bigint   bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  -10     0        3        NOP     EXIST   CLF\n"
                   "2        var1  -10     0        3        NOP     EXIST   CLF\n";
  build_parallel_merge(merge_context, rowkey_data);

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint   bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -10     0        3        NOP     EXIST   CF\n"
                        "0        var1  -8      0        2        NOP     EXIST   CL\n"
                        "1        var1  -8      0        2        2       EXIST   CF\n"
                        "1        var1  -7      0        2        NOP     EXIST   N\n"
                        "1        var1  -6      0        2        NOP     EXIST   N\n"  // 5
                        "1        var1  -5      0        2        NOP     EXIST   CL\n"
                        "2        var1  -10     0        3        NOP     EXIST   CF\n"
                        "2        var1  -5      0        2        NOP     EXIST   CL\n"
                        "3        var1  -5      0        2        NOP     EXIST   CLF\n"
                        "4        var1  -10     0        3        NOP     EXIST   CLF\n";  // 10

  uint16_t result_col_id[] = {
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,  // 5
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20  // 10
  };
  int64_t result_col_cnt[] = {5, 5, 6, 5, 5, 5, 5, 5, 5, 5};

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  for (int64_t i = 0; i < merge_context.parallel_merge_ctx_.concurrent_cnt_; i++) {
    ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, i /*idx*/));
  }
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1, '\\', result_col_id, result_col_cnt));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, parallel_minor_merge_append)
{
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1, sstable2, sstable3, sstable4, sstable5;
  const char* micro_data[1];
  micro_data[0] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1  -2      0       2        2       EXIST   CLF\n"
                  "1        var1  -2      0       2        2       EXIST   CLF\n";

  const char* micro_data2[1];
  micro_data2[0] = "bigint   var   bigint bigint    bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var1  -3     0         2      2       EXIST   CLF\n"
                   "3        var1  -3     0         2      2       EXIST   CLF\n";

  const char* micro_data3[1];
  micro_data3[0] = "bigint   var   bigint bigint    bigint   bigint  flag    multi_version_row_flag\n"
                   "4        var1  -4     0         2      2       EXIST   CLF\n"
                   "5        var1  -4     0         2      2       EXIST   CLF\n";

  const char* micro_data4[1];
  micro_data4[0] = "bigint   var   bigint bigint    bigint   bigint  flag    multi_version_row_flag\n"
                   "6        var1  -5     0         2        2     EXIST   CLF\n"
                   "7        var1  -5     0         2        2     EXIST    CLF\n";
  const char* micro_data5[1];
  micro_data5[0] = "bigint   var   bigint bigint    bigint   bigint  flag    multi_version_row_flag\n"
                   "8        var1  -6     0         2        2     EXIST   CLF\n"
                   "9        var1  -6     0         2        2     EXIST    CLF\n";

  prepare_data_start(sstable1, micro_data, rowkey_cnt, 2, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(sstable1);
  prepare_data_start(sstable2, micro_data2, rowkey_cnt, 3, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(sstable2);
  prepare_data_start(sstable3, micro_data3, rowkey_cnt, 4, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(sstable3);
  prepare_data_start(sstable4, micro_data4, rowkey_cnt, 5, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data4, 1);
  prepare_data_end(sstable4);
  prepare_data_start(sstable5, micro_data5, rowkey_cnt, 6, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data5, 1);
  prepare_data_end(sstable5);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable4));
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable5));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);

  const char* rowkey_data[1];
  rowkey_data[0] = "bigint   var   bigint   bigint   bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var1  -10       0       3        NOP     EXIST   CLF\n"
                   "5        var1  -10       0       3        NOP     EXIST   CLF\n"
                   "8        var1  -10       0       3        NOP     EXIST   CLF\n";
  build_parallel_merge(merge_context, rowkey_data);

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -2      0      2        2       EXIST   CLF\n"
                        "1        var1  -2      0      2        2       EXIST   CLF\n"
                        "2        var1  -3      0      2        2       EXIST   CLF\n"
                        "3        var1  -3      0      2        2       EXIST   CLF\n"
                        "4        var1  -4      0      2        2       EXIST   CLF\n"  // 5
                        "5        var1  -4      0      2        2       EXIST   CLF\n"
                        "6        var1  -5      0      2        2       EXIST   CLF\n"
                        "7        var1  -5      0      2        2       EXIST   CLF\n"
                        "8        var1  -6      0      2        2       EXIST   CLF\n"
                        "9        var1  -6      0      2        2       EXIST   CLF\n";  // 10
  uint16_t result_col_id[] = {
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,  // 5
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21  // 10
  };
  int64_t result_col_cnt[] = {6, 6, 6, 6, 6, 6, 6, 6, 6, 6};

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  for (int64_t i = 0; i < merge_context.parallel_merge_ctx_.concurrent_cnt_; i++) {
    ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, i /*idx*/));
  }
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));

  if (OB_NOT_NULL(merged_sstable)) {
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1, '\\', result_col_id, result_col_cnt));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, parallel_minor_merge_cross)
{
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1, sstable2, sstable3, sstable4, sstable5;
  const char* micro_data[1];
  micro_data[0] = "bigint   var   bigint  bigint   bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1  -2      0       2        NOP       EXIST   CLF\n"
                  "1        var1  -6      0       2        2         EXIST   CF\n"
                  "1        var1  -5      0       2        NOP       EXIST   N\n"
                  "1        var1  -4      0       NOP      2         EXIST   N\n"
                  "1        var1  -3      0       NOP      3         EXIST   N\n"
                  "1        var1  -2      0       NOP      2         EXIST   CL\n"
                  "2        var1  -2      0       NOP      2         EXIST   CLF\n"
                  "2        var2  -2      0       NOP      2         EXIST   CLF\n"
                  "2        var3  -5      0       2        2         EXIST   CF\n"
                  "2        var3  -4      0       2        NOP       EXIST   N\n"
                  "2        var3  -2      0       NOP      2         EXIST   CL\n"
                  "3        var1  -2      0       NOP      2         EXIST   CLF\n"
                  "4        var1  -2      0       NOP      2         EXIST   CLF\n"
                  "5        var1  -2      0       NOP      2         EXIST   CLF\n";

  const char* micro_data2[1];
  micro_data2[0] = "bigint   var   bigint  bigint   bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  -8      0       3      3         EXIST   CLF\n"
                   "2        var1  -9      0       3      NOP       EXIST   CLF\n"
                   "3        var1  -9      0       3      3         EXIST   CLF\n";

  prepare_data_start(sstable1, micro_data, rowkey_cnt, 2, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(sstable1);
  prepare_data_start(sstable2, micro_data2, rowkey_cnt, 3, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);

  const char* rowkey_data[1];
  rowkey_data[0] = "bigint   var   bigint  bigint   bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var1  -10     0       3        NOP     EXIST   CLF\n"
                   "4        var1  -10     0       3        NOP     EXIST   CLF\n";
  build_parallel_merge(merge_context, rowkey_data);

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint   bigint   bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -2       0       2        NOP       EXIST   CLF\n"
                        "1        var1  -8       0       3        3         EXIST   CF\n"
                        "1        var1  -6       0       2        2         EXIST   C\n"
                        "1        var1  -5       0       2        NOP       EXIST   N\n"
                        "1        var1  -4       0       2        NOP       EXIST   N\n"  // 5
                        "1        var1  -3       0       3        NOP       EXIST   N\n"
                        "1        var1  -2       0       2        NOP       EXIST   CL\n"
                        "2        var1  -9       0       3        2         EXIST   CF\n"
                        "2        var1  -2       0       2        NOP       EXIST   CL\n"
                        "2        var2  -2       0       2        NOP       EXIST   CLF\n"  // 10
                        "2        var3  -5       0       2        2         EXIST   CF\n"
                        "2        var3  -4       0       2        NOP       EXIST   N\n"
                        "2        var3  -2       0       2        NOP       EXIST   CL\n"
                        "3        var1  -9       0       3        3         EXIST   CF\n"
                        "3        var1  -2       0       2        NOP       EXIST   CL\n"  // 15
                        "4        var1  -2       0       2        NOP       EXIST   CLF\n"
                        "5        var1  -2       0       2        NOP       EXIST   CLF\n";
  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      21,  // 5
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21,  // 10
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,  // 15
      16,
      17,
      7,
      8,
      21,
      16,
      17,
      7,
      8,
      21};
  int64_t result_col_cnt[] = {5, 6, 6, 5, 5, 5, 5, 6, 5, 5, 6, 5, 5, 6, 5, 5, 5};

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  for (int64_t i = 0; i < merge_context.parallel_merge_ctx_.concurrent_cnt_; i++) {
    ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, i /*idx*/));
  }
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1, '\\', result_col_id, result_col_cnt));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -12     NOP     NOP     EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -10     NOP     6       EXIST   U   trans_id_2\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -7      NOP     2       EXIST   U   trans_id_2\n"
                  "1        var1   MIN2   -6      7       2       EXIST   U   trans_id_2\n"
                  "1        var1   -4     0       NOP     9       EXIST   L   trans_id_0\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL   trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CLF   trans_id_0\n"
                   "1        var1  MIN     -16      8        NOP     EXIST   U   trans_id_1\n"
                   "2        var2  -9      -22      12       NOP     EXIST   F   trans_id_0\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L   trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::RUNNING, transaction::ObTransTableStatusType::COMMIT};

  int64_t commit_trans_version1[] = {INT64_MAX, 24};
  for (int i = 0; OB_SUCC(ret) && i < 2; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CLF\n"
                        "1        var1  MIN     -16     8        NOP     EXIST   U\n"
                        "1        var1  MIN     -12     NOP      NOP     EXIST   U\n"
                        "1        var1  -24     -10     7        6       EXIST   C\n"
                        "1        var1  -4      0       NOP      9       EXIST   CL\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans2)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -12     NOP     NOP     EXIST   U   trans_id_2\n"
                  "1        var1   MIN2   -10     NOP     6       EXIST   U   trans_id_1\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -7      NOP     2       EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -6      7       2       EXIST   U   trans_id_1\n"
                  "1        var1   -4     0       NOP     9       EXIST   L   trans_id_0\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL   trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CLF trans_id_0\n"
                   "1        var1   MIN    -89      NOP      9       EXIST   U   trans_id_3\n"
                   "1        var1   MIN2   -17      NOP      12      EXIST   U   trans_id_2\n"
                   "1        var1   MIN2   -16      NOP      8       EXIST   LU  trans_id_2\n"
                   "2        var2  -9      -22      19       NOP     EXIST   F   trans_id_0\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L   trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING};

  int64_t commit_trans_version1[] = {16, 24, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CLF\n"
                        "1        var1  MIN     -89     NOP      9       EXIST   U\n"  // trans_id_3
                        "1        var1  -24     -17     7        12      EXIST   C\n"  // trans_id_2
                        "1        var1  -16     -10     7        6       EXIST   C\n"  // trans_id_1
                        "1        var1  -4      0       NOP      9       EXIST   CL\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans_can_compact)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[5];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -14     NOP     59      EXIST   U   trans_id_4\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -13     NOP     28      EXIST   U   trans_id_3\n";

  macro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -12     NOP     71      EXIST   U   trans_id_2\n"
                  "1        var1   MIN2   -10     NOP     6       EXIST   U   trans_id_1\n";

  macro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -7      NOP     2       EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -6      7       2       EXIST   U   trans_id_1\n"
                  "1        var1   -4     0       NOP     9       EXIST   L   trans_id_0\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL  trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[3], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[4], 1, INT64_MAX, nullptr, nullptr, true);

  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CLF trans_id_0\n"
                   "1        var1   MIN    -89      NOP      9       EXIST   U   trans_id_5\n"
                   "1        var1   MIN2   -17      NOP      12      EXIST   U   trans_id_4\n"
                   "1        var1   MIN2   -16      NOP      8       EXIST   LU  trans_id_4\n"
                   "2        var2  -9      -22      19       NOP     EXIST   F   trans_id_0\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L   trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING};

  int64_t commit_trans_version1[] = {16, 24, 29, 35, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 5; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CLF\n"
                        "1        var1  MIN     -89     NOP      9       EXIST   U\n"  // trans_id_5
                        "1        var1  -35     -17     7        12      EXIST   C\n"  // trans_id_4
                        "1        var1  -29     -13     NOP      28      EXIST   N\n"  // trans_id_3
                        "1        var1  -24     -12     NOP      71      EXIST   N\n"  // trans_id_2
                        "1        var1  -16     -10     7        6       EXIST   C\n"  // trans_id_1
                        "1        var1  -4      0       NOP      9       EXIST   L\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans_can_compact2)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[5];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -14     NOP     59      EXIST   U   trans_id_4\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -13     81      28      EXIST   U   trans_id_3\n";

  macro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -12     NOP     71      EXIST   U   trans_id_2\n"
                  "1        var1   MIN2   -10     NOP     6       EXIST   U   trans_id_1\n";

  macro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -7      NOP     2       EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -6      7       2       EXIST   U   trans_id_1\n"
                  "1        var1   -4     0       NOP     9       EXIST   L   trans_id_0\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL  trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[3], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[4], 1, INT64_MAX, nullptr, nullptr, true);

  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CLF trans_id_0\n"
                   "1        var1   MIN    -89      NOP      9       EXIST   U   trans_id_5\n"
                   "1        var1   MIN2   -17      NOP      12      EXIST   U   trans_id_4\n"
                   "1        var1   MIN2   -16      NOP      8       EXIST   LU  trans_id_4\n"
                   "2        var2  -9      -22      19       NOP     EXIST   F   trans_id_0\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L   trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING};

  int64_t commit_trans_version1[] = {16, 24, 29, 35, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 5; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CLF\n"
                        "1        var1  MIN     -89     NOP      9       EXIST   U\n"  // trans_id_5
                        "1        var1  -35     -17     81       12      EXIST   C\n"  // trans_id_4
                        "1        var1  -29     -13     81       28      EXIST   C\n"  // trans_id_3
                        "1        var1  -24     -12     NOP      71      EXIST   N\n"  // trans_id_2
                        "1        var1  -16     -10     NOP      6       EXIST   N\n"  // trans_id_1
                        "1        var1  -16     -7      7        2       EXIST   C\n"
                        "1        var1  -4      0       NOP      9       EXIST   L\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans_can_not_compact)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[5];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -14     NOP     59      EXIST   U   trans_id_4\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -13     NOP     28      EXIST   U   trans_id_3\n";

  macro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -12     NOP     71      EXIST   U   trans_id_2\n"
                  "1        var1   MIN2   -10     NOP     6       EXIST   U   trans_id_1\n";

  macro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -7      NOP     2       EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -6      NOP     2       EXIST   U   trans_id_1\n"
                  "1        var1   -4     0       NOP     9       EXIST   L   trans_id_0\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL  trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[3], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[4], 1, INT64_MAX, nullptr, nullptr, true);

  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CLF trans_id_0\n"
                   "1        var1   MIN    -89      NOP      9       EXIST   U   trans_id_5\n"
                   "1        var1   MIN2   -17      NOP      12      EXIST   U   trans_id_4\n"
                   "1        var1   MIN2   -16      NOP      8       EXIST   LU  trans_id_4\n"
                   "2        var2  -9      -22      19       NOP     EXIST   F   trans_id_0\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L   trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN     -93     NOP      17      EXIST   U   trans_id_6\n"
                   "1        var1   MIN2    -91     NOP      72      EXIST   LU    trans_id_5\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF   trans_id_0\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING};

  int64_t commit_trans_version1[] = {16, 24, 29, 35, 61, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 6; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CLF\n"
                        "1        var1  MIN     -93     NOP      17      EXIST   U\n"  // trans_id_6
                        "1        var1  -61     -91     NOP      72      EXIST   C\n"  // trans_id_5
                        "1        var1  -35     -17     NOP      12      EXIST   C\n"  // trans_id_4
                        "1        var1  -29     -13     NOP      28      EXIST   N\n"  // trans_id_3
                        "1        var1  -24     -12     NOP      71      EXIST   N\n"  // trans_id_2
                        "1        var1  -16     -10     NOP      6       EXIST   C\n"  // trans_id_1
                        "1        var1  -4      0       NOP      9       EXIST   L\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_same_sql_sequence)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[1];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -6      18      2       EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -3      NOP     NOP     EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -2      NOP     19      EXIST   LU   trans_id_1\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);

  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[4];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -14     NOP     59      EXIST   U   trans_id_4\n";

  macro_data2[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN2   -13     NOP     28      EXIST   U   trans_id_3\n";

  macro_data2[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN2   -12     NOP     71      EXIST   U   trans_id_2\n"
                   "1        var1   MIN2   -10     NOP     6       EXIST   U   trans_id_1\n";

  macro_data2[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN2   -7      NOP     2       EXIST   U   trans_id_1\n"
                   "1        var1   MIN2   -6      NOP     2       EXIST   LU   trans_id_1\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_one_macro(&macro_data2[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data2[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data2[3], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT};

  int64_t commit_trans_version1[] = {16, 24, 29, 35};
  for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -35     -14     18       59      EXIST   C\n"    // trans_id_4
                        "1        var1  -29     -13     NOP      28      EXIST   N\n"    // trans_id_3
                        "1        var1  -24     -12     NOP      71      EXIST   N\n"    // trans_id_2
                        "1        var1  -16     -10     18       6       EXIST   CL\n";  // trans_id_1

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_magic_row)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[1];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN   -6      18      2       EXIST   U   trans_id_1\n"
                  "1        var1   MIN   -3      NOP     NOP     EXIST   U   trans_id_1\n"
                  "1        var1   MIN   -2      NOP     19      EXIST   LU  trans_id_1\n"
                  "2        var2   MIN   -9      18      0       EXIST   LU  trans_id_2\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);

  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -38     NOP     59      EXIST   LU   trans_id_3\n"
                   "2        var2   MIN    -38     NOP     59      EXIST   LU   trans_id_2\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -48     NOP     59      EXIST   LU   trans_id_3\n"
                   "2        var2   MIN    -71     18      1       EXIST   U    trans_id_4\n"
                   "2        var2   MAGIC  MAGIC   NOP     NOP     EXIST   LM   trans_id_0\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");

  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT};

  int64_t commit_trans_version1[] = {INT64_MAX, 29, INT64_MAX, 92};
  for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MAGIC  MAGIC   NOP      NOP     EXIST   LM\n"
                        "2        var2   -92    -71     18       1       EXIST   C\n"    // trans_id_4
                        "2        var2   -29    -9      18       0       EXIST   CL\n";  // trans_id_2

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_magic_row2)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[1];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -28     NOP     59      EXIST    U     trans_id_1\n"
                  "1        var1   -1     -6      18      2       EXIST   CLF    trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);

  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -38     NOP     59      EXIST   LU     trans_id_1\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF    trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -48     NOP     59      EXIST   U     trans_id_2\n"
                   "1        var1   -29    0       18      59      EXIST   CLF   trans_id_0\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF   trans_id_0\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObSSTable sstable4;
  const char* macro_data4[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -58     NOP     59      EXIST   U     trans_id_3\n"
                   "1        var1   -39    0       18      59      EXIST   CLF   trans_id_0\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF   trans_id_0\n";

  prepare_data_start(sstable4, macro_data3, rowkey_cnt, 30, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data4, 1);
  prepare_data_end(sstable4);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable4));
  STORAGE_LOG(INFO, "finish prepare sstable4");

  ObSSTable sstable5;
  const char* macro_data5[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -68     NOP     59      EXIST   LU     trans_id_4\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF    trans_id_0\n";

  prepare_data_start(sstable5, macro_data3, rowkey_cnt, 40, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data5, 1);
  prepare_data_end(sstable5);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable5));
  STORAGE_LOG(INFO, "finish prepare sstable5");

  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT};

  int64_t commit_trans_version1[] = {INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -39    0       18      59      EXIST   CF\n"
                        "1        var1   -29    0       18      59      EXIST   C \n"
                        "1        var1   -1     -6      18      2       EXIST   CL \n"
                        "2        var2   -92    -71     18      1       EXIST   CLF\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_merge_with_magic_row3)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[1];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -1      NOP     59      EXIST    U     trans_id_1\n"
                  "1        var1   -1     -6      18      2       EXIST    CLF   trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);

  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   -12    0       20      NOP     EXIST   CLF    trans_id_0\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF    trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -48     NOP     59      EXIST   U     trans_id_2\n"
                   "1        var1   -29    0       18      59      EXIST   CLF   trans_id_0\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF   trans_id_0\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObSSTable sstable4;
  const char* macro_data4[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -58     NOP     59      EXIST   U     trans_id_3\n"
                   "1        var1   -39    0       18      59      EXIST   CLF   trans_id_0\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF   trans_id_0\n";

  prepare_data_start(sstable4, macro_data3, rowkey_cnt, 30, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data4, 1);
  prepare_data_end(sstable4);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable4));
  STORAGE_LOG(INFO, "finish prepare sstable4");

  ObSSTable sstable5;
  const char* macro_data5[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "1        var1   MIN    -68     NOP     59      EXIST   LU     trans_id_4\n"
                   "2        var2   -92    -71     18      1       EXIST   CLF    trans_id_0\n";

  prepare_data_start(sstable5, macro_data3, rowkey_cnt, 40, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data5, 1);
  prepare_data_end(sstable5);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable5));
  STORAGE_LOG(INFO, "finish prepare sstable5");

  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT};

  int64_t commit_trans_version1[] = {12, INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -39    0       18      59      EXIST   CF\n"
                        "1        var1   -29    0       18      59      EXIST   C \n"
                        "1        var1   -12    0       20      59      EXIST   CL \n"
                        "2        var2   -92    -71     18      1       EXIST   CLF\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, sub_version_range_for_sstable_cut)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* micro_data[4];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1  -200     0      1        2       EXIST   CF\n"
                  "0        var1  -160     0      NOP      1       EXIST   N\n"
                  "0        var1  -150     0      NOP      2       EXIST   N\n"
                  "0        var1  -50      0      1        1       EXIST   CL\n";

  prepare_data_start(sstable1, micro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -160    0      1        1       EXIST   CF\n"
                        "0        var1  -150    0      2        NOP     EXIST   N\n"
                        "0        var1  -50     0      1        1       EXIST   CL\n";

  uint16_t result_col_id[] = {16, 17, 7, 8, 21, 20, 16, 17, 7, 8, 21, 16, 17, 7, 8, 20, 21};
  int64_t result_col_cnt[] = {6, 5, 6};
  res_iter.reset();
  range.get_range().set_whole_range();

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 160;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_query_param(trans_version_range);
  context_.query_flag_.is_sstable_cut_ = true;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  ASSERT_EQ(OB_SUCCESS, sstable1.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();

  const char* result2 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -160     0      1        1       EXIST   CF\n"
                        "0        var1  -150     0      2        NOP     EXIST   N\n"
                        "0        var1  -50      0      1        1       EXIST   CL\n";
  uint16_t result_col_id2[] = {16, 17, 7, 8, 21, 20, 16, 17, 7, 8, 21, 16, 17, 7, 8, 20, 21};
  int64_t result_col_cnt2[] = {6, 5, 6};
  trans_version_range.snapshot_version_ = 160;
  trans_version_range.multi_version_start_ = 100;
  trans_version_range.base_version_ = 100;
  prepare_query_param(trans_version_range);
  context_.query_flag_.is_sstable_cut_ = true;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  ASSERT_EQ(OB_SUCCESS, sstable1.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result2, '\\', result_col_id2, result_col_cnt2));
  ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var1  -160     0     1        1       EXIST   CF\n"
                        "0        var1  -150     0     2        1       EXIST   CL\n";
  uint16_t result_col_id3[] = {
      16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      21,
      20,
  };
  int64_t result_col_cnt3[] = {6, 6};
  trans_version_range.snapshot_version_ = 160;
  trans_version_range.multi_version_start_ = 155;
  trans_version_range.base_version_ = 155;
  prepare_query_param(trans_version_range);
  context_.query_flag_.is_sstable_cut_ = true;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  ASSERT_EQ(OB_SUCCESS, sstable1.scan(param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result3, '\\', result_col_id3, result_col_cnt3));
  ASSERT_TRUE(res_iter.equals(*scanner, true));
  scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionMerge, test_sstable_merge_cut_with_multi_trans)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -12     NOP     NOP     EXIST   U   trans_id_1\n"
                  "1        var1   MIN2   -10     NOP     6       EXIST   U   trans_id_2\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN2   -7      NOP     2       EXIST   U   trans_id_2\n"
                  "1        var1   MIN2   -6      7       2       EXIST   U   trans_id_2\n"
                  "1        var1   -4     0       NOP     9       EXIST   L   trans_id_0\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL   trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CLF   trans_id_0\n"
                   "1        var1  MIN     -16      8        NOP     EXIST   U   trans_id_1\n"
                   "2        var2  -9      -22      12       NOP     EXIST   F   trans_id_0\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L   trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::RUNNING, transaction::ObTransTableStatusType::COMMIT};

  int64_t commit_trans_version1[] = {INT64_MAX, 24};
  for (int i = 0; OB_SUCC(ret) && i < 2; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 20;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  context_.query_flag_.is_sstable_cut_ = true;
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CLF\n"
                        "1        var1  MIN     -16     8        NOP     EXIST   U\n"
                        "1        var1  MIN     -12     NOP      NOP     EXIST   U\n"
                        "1        var1  -4      0       NOP      9       EXIST   CL\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  context_.query_flag_.is_sstable_cut_ = true;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestMultiVersionMerge, test_trans_cross_sstable)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "0        var0   -9    -9      7       12      EXIST   CL  trans_id_0\n"
                  "1        var1   MIN   -12     -1     -20      EXIST   U   trans_id_1\n"
                  "1        var1   MIN   -10     NOP     6       EXIST   U   trans_id_1\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN   -7      NOP     2       EXIST   U   trans_id_1\n"
                  "1        var1   MIN   -6      7       2       EXIST   LU   trans_id_1\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                   "0        var0  -9      -11      NOP      NOP     DELETE  CLF   trans_id_0\n"
                   "1        var1  -38     -0       NOP      NOP     EXIST   CLF   trans_id_0\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 38))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  context_.query_flag_.is_sstable_cut_ = true;
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     NOP      NOP     DELETE   CLF\n"
                        "1        var1  -38     -0      -1       -20     EXIST    CLF\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  res_iter.reset();
  range.get_range().set_whole_range();
  prepare_query_param(trans_version_range);
  context_.query_flag_.is_sstable_cut_ = true;
  ASSERT_EQ(OB_SUCCESS, range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
  if (OB_NOT_NULL(merged_sstable)) {
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -rf test_partition_merge_multi_version.log*");
  OB_LOGGER.set_file_name("test_partition_merge_multi_version.log");
  OB_LOGGER.set_log_level("DEBUG");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
