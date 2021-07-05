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
class TestNewMinorFuserMerge : public ObMultiVersionSSTableTest {
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestNewMinorFuserMerge() : ObMultiVersionSSTableTest("test_new_minor_fuser")
  {}
  virtual ~TestNewMinorFuserMerge()
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
    test_trans_part_ctx_.set_partition_key(pkey);
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

void TestNewMinorFuserMerge::prepare_query_param(const ObVersionRange& trans_version_range)
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
  store_ctx_.trans_table_guard_->set_trans_state_table(&scan_trans_part_ctx_);
  context_.is_inited_ = true;
}

void TestNewMinorFuserMerge::prepare_schema()
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

void TestNewMinorFuserMerge::prepare_schema(const ObObjType* type_list)
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

void TestNewMinorFuserMerge::prepare_merge_context(const storage::ObTablesHandle& tables_handle,
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

int TestNewMinorFuserMerge::create_sstable(const storage::ObCreateSSTableParamWithTable& param, ObTableHandle& handle)
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

int TestNewMinorFuserMerge::init_tenant_mgr()
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

void TestNewMinorFuserMerge::build_sstable(ObSSTableMergeCtx& ctx, ObSSTable*& merged_sstable)
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

void TestNewMinorFuserMerge::build_two_sstable(
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

void TestNewMinorFuserMerge::build_parallel_merge(ObSSTableMergeCtx& ctx, const char** rowkey_data)
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

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_rowkey_across_macro_block)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1   -9     -8      2        NOP     EXIST   CL\n"
                  "1        var1   -7     -12     2        NOP     EXIST   N\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -7     -10     NOP      6       EXIST   N\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -7     -7      NOP      2       EXIST   N\n";

  macro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -7     -5      2        NOP     EXIST   L\n"
                  "2        var2   -8     -8      5        NOP     EXIST   L\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1);
  prepare_one_macro(&macro_data[2], 1);
  prepare_one_macro(&macro_data[3], 1);
  prepare_data_end(sstable1);

  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var1  -10      0        3        NOP     EXIST   CLF\n"
                   "1        var1  -7       -14      9        NOP     EXIST   L\n"
                   "2        var2  -8       -22      NOP      6       EXIST   L\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
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
                        "0        var1   -10     0       3        NOP     EXIST   CF\n"
                        "0        var1   -9      -8      2        NOP     EXIST   CL\n"
                        "1        var1   -7      -14     9        6       EXIST   CL\n"
                        "2        var2   -8      -22     5        6       EXIST   CL\n";
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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_rowkey_across_macro_block2)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var1   -9     -8      2       NOP     EXIST   CL\n"
                  "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -7     -10     NOP     6       EXIST   N\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -6     -7      NOP     2       EXIST   C\n";

  macro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -5     -5      NOP     9       EXIST   CL\n"
                  "2        var2   -8     -8      5       NOP     EXIST   L\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1);
  prepare_one_macro(&macro_data[2], 1);
  prepare_one_macro(&macro_data[3], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var1  -10      0        3        NOP     EXIST   CLF\n"
                   "1        var1  -7       -24      NOP      19      EXIST   L\n"
                   "2        var2  -8       -22      3        6       EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
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
                        "0        var1   -10     0       3        NOP     EXIST   CF\n"
                        "0        var1   -9      -8      2        NOP     EXIST   CL\n"
                        "1        var1   -7      -24     NOP      19      EXIST   C\n"
                        "1        var1   -6      -7      NOP      2       EXIST   C\n"
                        "1        var1   -5      -5      NOP      9       EXIST   CL\n"
                        "2        var2   -8      -22     3        6       EXIST   CL\n";
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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_rowkey_across_macro_block3)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -6     -8      2       7       EXIST   CL\n"
                  "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -7     -10     NOP     6       EXIST   N\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -6     -7      NOP     2       EXIST   C\n";

  macro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -5     -5      NOP     9       EXIST   CL\n"
                  "2        var2   -8     -8      5       NOP     EXIST   L\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1);
  prepare_one_macro(&macro_data[2], 1);
  prepare_one_macro(&macro_data[3], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[3];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -10      0        3        NOP     EXIST   CF\n"
                   "0        var0  -9       -24      NOP      19      EXIST   N\n"
                   "0        var0  -8       -22      3        6       EXIST   N\n";

  macro_data2[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -6       -16      3        NOP     EXIST   CL\n"
                   "1        var1  -12      -24      NOP      19      EXIST   CF\n"
                   "1        var1  -8       -22      3        6       EXIST   N\n";

  macro_data2[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  -7       -15      3        NOP     EXIST   CL\n"
                   "2        var2  -8       -22      NOP      6       EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_one_macro(&macro_data2[1], 1);
  prepare_one_macro(&macro_data2[2], 1);
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

  const char* result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
      "0        var0   -10     0       3        7       EXIST   CF\n"  // two compact row need to compact together
      "0        var0   -9      -24     NOP      19      EXIST   N\n"
      "0        var0   -8      -22     3        6       EXIST   N\n"
      "0        var0   -6      -16     3        7       EXIST   CL\n"
      "1        var1   -12     -24     NOP      19      EXIST   CF\n"
      "1        var1   -8      -22     3        6       EXIST   N\n"
      "1        var1   -7      -15     3        6       EXIST   C\n"
      "1        var1   -6      -7      NOP      2       EXIST   C\n"
      "1        var1   -5      -5      NOP      9       EXIST   CL\n"
      "2        var2   -8      -22     5        6       EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_multi_sstable)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -1     -9      7       NOP     EXIST   CLF\n"
                  "1        var1   -7     -12     5       28      EXIST   CF\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -4     0       5       2       EXIST   L\n";

  macro_data[2] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1);
  prepare_one_macro(&macro_data[2], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  -7      -24      NOP      21      EXIST   L\n"
                   "2        var2  -9      -22      18       NOP     EXIST   F\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  -12      0       NOP      19      EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 5;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -1      -9      7        NOP     EXIST   CLF\n"
                        "1        var1  -12     0       5        19      EXIST   CF\n"
                        "1        var1  -7      -24     5        21      EXIST   C\n"
                        "1        var1  -4      0       5        2       EXIST   CL\n"
                        "2        var2  -9      -22     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_multi_sstable2)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n"
                  "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   -4     0       NOP     9       EXIST   L\n";

  macro_data[2] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1);
  prepare_one_macro(&macro_data[2], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CL\n"
                   "1        var1  -7      -24      NOP      19      EXIST   L\n"
                   "2        var2  -9      -22      12       NOP     EXIST   F\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  -12      0       NOP      19      EXIST   CF\n"
                   "1        var1  -8       -2      1        6       EXIST   N\n"
                   "1        var1  -7       -35     7        NOP     EXIST   L\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CL\n"
                        "1        var1  -12     0       NOP      19      EXIST   CF\n"
                        "1        var1  -8      -2      1        6       EXIST   N\n"
                        "1        var1  -7      -35     7        19      EXIST   C\n"
                        "1        var1  -4      0       NOP      9       EXIST   CL\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_multi_sstable3)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[10];
  int index = 0;
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0   -9     -9      7       12      EXIST   CL\n";
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -10     NOP     6       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -7      NOP     2       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     0       NOP     9       EXIST   L\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -5      NOP     9       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -3      1       8       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < index; ++i) {
    prepare_one_macro(&macro_data[i], 1);
  }
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CL\n"
                   "1        var1  -7      -24      18       NOP     EXIST   CL\n"
                   "2        var2  -9      -22      NOP      9       EXIST   C\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  -12      0       NOP      19      EXIST   CF\n"
                   "1        var1  -8       -2      1        6       EXIST   N\n"
                   "1        var1  -7       -35     NOP      7       EXIST   CL\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
      "0        var0  -9      -11     9        12      EXIST   CL\n"
      "1        var1  -12     0       18       19      EXIST   CF\n"  // two compact rows with same rowkey & diff
                                                                      // trans_version need compact
      "1        var1  -8      -2      1        6       EXIST   N\n"
      "1        var1  -7      -35     18       7       EXIST   CL\n"
      "2        var2  -9      -25     18       9       EXIST   CF\n"
      "2        var2  -4      -15     NOP      7       EXIST   L\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_multi_sstable_has_same_row)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[10];
  int index = 0;
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0   -9     -9      7       12      EXIST   CL\n";
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -10     NOP     6       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -7      NOP     2       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     0       NOP     9       EXIST   L\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -5      NOP     9       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -3      1       8       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < index; ++i) {
    prepare_one_macro(&macro_data[i], 1);
  }
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -9       7        12      EXIST   CL\n"
                   "1        var1  -7      -24      18       NOP     EXIST   CL\n"
                   "2        var2  -9      -5       1        9       EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
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
                        "0        var0  -9      -9       7        12      EXIST   CL\n"
                        "1        var1  -7      -24     18       6       EXIST   CL\n"
                        "2        var2  -9      -5      1        9       EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_new_sstable_had_old_rows)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[10];
  int index = 0;
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0   -9     -9      7       12      EXIST   CL\n";
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -10     NOP     6       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -7      NOP     2       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     0       NOP     9       EXIST   L\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -5      NOP     9       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -3      1       8       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < index; ++i) {
    prepare_one_macro(&macro_data[i], 1);
  }
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -9       7        12      EXIST   CL\n"
                   "1        var1  -12     0        7        NOP     EXIST   CF\n"
                   "1        var1  -7      -24      18       NOP     EXIST   C\n"
                   "1        var1  -2      0        21       NOP     EXIST   N\n"
                   "1        var1  -1      0        91       12      EXIST   L\n"
                   "2        var2  -9      -5       1        9       EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 5;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -9       7        12      EXIST   CL\n"
                        "1        var1  -12     0        7        6       EXIST   CF\n"
                        "1        var1  -7      -24      18       6       EXIST   C\n"
                        "1        var1  -2      0        21       12      EXIST   CL\n"
                        "2        var2  -9      -5       1        9       EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_new_sstable_had_old_rows2)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[10];
  int index = 0;
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0   -9     -7      7       12      EXIST   CL\n";
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -10     NOP     6       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -7      NOP     2       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     0       NOP     9       EXIST   L\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -5      NOP     9       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -3      1       8       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < index; ++i) {
    prepare_one_macro(&macro_data[i], 1);
  }
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -14     -19      71       NOP     EXIST   N\n"
                   "0        var0  -9      -9       NOP      12      EXIST   CL\n"
                   "1        var1  -7      -24      NOP      68      EXIST   N\n"
                   "1        var1  -6      0        21       NOP     EXIST   N\n"
                   "1        var1  -4      0        91       12      EXIST   L\n"
                   "2        var2  -9      -5       1        9       EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
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
                        "0        var0  -14     -19      71       12      EXIST   C\n"
                        "0        var0  -9      -9       7        12      EXIST   CL\n"
                        "1        var1  -7      -24      21       68      EXIST   C\n"
                        "1        var1  -6      0        21       NOP     EXIST   N\n"
                        "1        var1  -4      0        91       12      EXIST   L\n"
                        "2        var2  -9      -5       1        9       EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_delete_row)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[10];
  int index = 0;
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0   -9     -7      7       12      EXIST   CL\n";
  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -10     NOP     6       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     -7      NOP     2       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -7     0       NOP     9       EXIST   L\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -5      NOP     9       EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -9     -3      1       8       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < index; ++i) {
    prepare_one_macro(&macro_data[i], 1);
  }
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -14     -19      71       NOP     EXIST   N\n"
                   "0        var0  -9      -9       NOP      12      EXIST   CL\n"
                   "1        var1  -7      -24      NOP      NOP     DELETE  N\n"
                   "1        var1  -6      0        21       NOP     EXIST   N\n"
                   "1        var1  -4      0        91       12      EXIST   L\n"
                   "2        var2  -9      -5       1        9       EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
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
                        "0        var0  -14     -19      71       12      EXIST   C\n"
                        "0        var0  -9      -9       7        12      EXIST   CL\n"
                        "1        var1  -7      -24      NOP      NOP     DELETE  C\n"
                        "1        var1  -6      0        21       NOP     EXIST   N\n"
                        "1        var1  -4      0        91       12      EXIST   L\n"
                        "2        var2  -9      -5       1        9       EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_test_allocator)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[10];
  int index = 0;
  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "0        var0   -9     -7      biubiu  NOP     EXIST   CL\n";
  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     -10     NOP     kkkkk3  EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     -7      NOP     pppp6   EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     0       char77  9       EXIST   L\n";

  macro_data[index++] = "bigint   var   bigint  bigint  var     var     flag    multi_version_row_flag\n"
                        "2        var2   -9     -5      NOP     nnnnn   EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint  bigint  var     var     flag    multi_version_row_flag\n"
                        "2        var2   -9     -3      1       8       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < index; ++i) {
    prepare_one_macro(&macro_data[i], 1);
  }
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint    var      var     flag    multi_version_row_flag\n"
                   "0        var0  -14     -19      bbbbb    NOP     EXIST   N\n"
                   "0        var0  -9      -9       NOP      huluhulu EXIST   CL\n"
                   "1        var1  -7      -24      NOP      NOP     EXIST   L\n"
                   "2        var2  -9      -5       1        NOP     EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  const ObObjType type_list[] = {ObIntType, ObVarcharType, ObVarcharType, ObVarcharType};
  prepare_merge_context(
      tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context, type_list);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint   var      var     flag    multi_version_row_flag\n"
                        "0        var0  -14     -19      bbbbb    huluhulu  EXIST   C\n"
                        "0        var0  -9      -9       biubiu   huluhulu  EXIST   CL\n"
                        "1        var1  -7      -24      char77   kkkkk3    EXIST   CL\n"
                        "2        var2  -9      -5       1        nnnnn     EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_test_allocator2)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[10];
  int index = 0;
  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "0        var0   -9     -7      biubiu  NOP     EXIST   CL\n";
  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     -12     NOP     NOP     EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     -10     NOP     hkgqhjkgqhkghqkhgkqh  EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     -7      NOP     pppp6   EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint bigint   var     var     flag    multi_version_row_flag\n"
                        "1        var1   -7     0       char77  9       EXIST   L\n";

  macro_data[index++] = "bigint   var   bigint  bigint  var     var     flag    multi_version_row_flag\n"
                        "2        var2   -9     -5      NOP     nnnnn   EXIST   N\n";

  macro_data[index++] = "bigint   var   bigint  bigint  var     var     flag    multi_version_row_flag\n"
                        "2        var2   -9     -3      1       8       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < index; ++i) {
    prepare_one_macro(&macro_data[i], 1);
  }
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint    var      var     flag    multi_version_row_flag\n"
                   "0        var0  -14     -19      bbbbb    NOP     EXIST   N\n"
                   "0        var0  -9      -9       NOP      huluhulu EXIST   CL\n"
                   "1        var1  -7      -24      NOP      NOP     EXIST   L\n"
                   "2        var2  -9      -5       1        NOP     EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint    var      var     flag    multi_version_row_flag\n"
                   "0        var0  -14     -39      NOP    akhgfajkhg  EXIST   CL\n"
                   "1        var1  -7      -40      qjkfjkqhgkq      NOP     EXIST   L\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 12, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  const ObObjType type_list[] = {ObIntType, ObVarcharType, ObVarcharType, ObVarcharType};
  prepare_merge_context(
      tables_handle, MINI_MINOR_MERGE, false /*is_full_merge*/, trans_version_range, merge_context, type_list);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint   var      var     flag    multi_version_row_flag\n"
                        "0        var0  -14     -39      bbbbb    akhgfajkhg  EXIST   C\n"
                        "0        var0  -9      -9       biubiu   huluhulu  EXIST   CL\n"
                        "1        var1  -7      -40      qjkfjkqhgkq hkgqhjkgqhkghqkhgkqh    EXIST   CL\n"
                        "2        var2  -9      -5       1        nnnnn     EXIST   CL\n";

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
    context_.read_out_type_ = FLAT_ROW_STORE;
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(param_, context_, range, scanner));
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
    ASSERT_TRUE(res_iter.equals(*scanner, true /*cmp multi version row flag*/));
    scanner->~ObStoreRowIterator();
  } else {
    STORAGE_LOG(ERROR, "merged_sstable is null");
  }
}

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_uncommitted_row_running)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n"
                  "1        var1   MIN    -12     NOP     NOP     EXIST   U\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -10     NOP     6       EXIST   U\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -7      NOP     2       EXIST   U\n"
                  "1        var1   MIN    -6      NOP     2       EXIST   U\n"
                  "1        var1   -4     0       NOP     9       EXIST   L\n";

  macro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[3], 1);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CL\n"
                   "1        var1  MIN     -26      NOP      11      EXIST   U\n"
                   "1        var1  MIN     -24      NOP      19      EXIST   LU\n"
                   "2        var2  -9      -22      12       NOP     EXIST   F\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  MIN      -35     7        NOP     EXIST   LU\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(transaction::ObTransTableStatusType::RUNNING))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(tables_handle, MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  ObMockIterator res_iter;
  ObStoreRowIterator* scanner = NULL;
  ObExtStoreRange range;

  const char* result1 = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                        "0        var0  -9      -11     9        12      EXIST   CL\n"
                        "1        var1  MIN     -35     7        NOP     EXIST   U\n"
                        "1        var1  MIN     -26     NOP      11      EXIST   U\n"
                        "1        var1  MIN     -24     NOP      19      EXIST   U\n"
                        "1        var1  MIN     -12     NOP      NOP     EXIST   U\n"
                        "1        var1  MIN     -10     NOP      6       EXIST   U\n"
                        "1        var1  MIN     -7      NOP      2       EXIST   U\n"
                        "1        var1  MIN     -6      NOP      2       EXIST   U\n"
                        "1        var1  -4      0       NOP      9       EXIST   CL\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   L\n";

  // minor mrege

  ObMacroBlockBuilder builder;
  ObSSTable* merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergeUtil::merge_partition(&mem_ctx, merge_context, builder, 0));
  build_sstable(merge_context, merged_sstable);

  scan_trans_part_ctx_.clear_all();
  scan_trans_part_ctx_.set_all_transaction_status(transaction::ObTransTableStatusType::RUNNING);
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

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_uncommitted_row_abort)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n"
                  "1        var1   MIN    -12     NOP     NOP     EXIST   U\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -10     NOP     6       EXIST   U\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -7      NOP     2       EXIST   U\n"
                  "1        var1   MIN    -6      NOP     2       EXIST   U\n"
                  "1        var1   -4     0       NOP     9       EXIST   CL\n";

  macro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[3], 1, INT64_MAX);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CL\n"
                   "1        var1  MIN     -26      NOP      11      EXIST   U\n"
                   "1        var1  MIN     -24      NOP      19      EXIST   LU\n"
                   "2        var2  -9      -22      12       NOP     EXIST   F\n"
                   "2        var2  -4      -15      NOP      7       EXIST   CL\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  MIN      -35     7        NOP     EXIST   LU\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(transaction::ObTransTableStatusType::ABORT))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
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
                        "0        var0  -9      -11     9        12      EXIST   CL\n"
                        "1        var1  -4      0       NOP      9       EXIST   CL\n"
                        "2        var2  -9      -25     18       7       EXIST   CF\n"
                        "2        var2  -4      -15     NOP      7       EXIST   CL\n";

  // minor mrege
  scan_trans_part_ctx_.clear_all();
  scan_trans_part_ctx_.set_all_transaction_status(transaction::ObTransTableStatusType::ABORT);
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

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_uncommitted_row_commit)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[4];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "0        var0   -9     -9      7       12      EXIST   CL\n"
                  "1        var1   MIN    -12     NOP     NOP     EXIST   U\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -10     NOP     6       EXIST   U\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -7      NOP     2       EXIST   U\n"
                  "1        var1   MIN    -6      NOP     2       EXIST   U\n"
                  "1        var1   -4     0       NOP     9       EXIST   L\n";

  macro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[3], 1, INT64_MAX);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CL\n"
                   "1        var1  MIN     -26      NOP      11      EXIST   U\n"
                   "1        var1  MIN     -24      NOP      19      EXIST   LU\n"
                   "2        var2  -9      -22      12       NOP     EXIST   F\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L\n";

  prepare_data_start(sstable2, macro_data2, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data2, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable2);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObSSTable sstable3;
  const char* macro_data3[1];
  macro_data3[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "1        var1  MIN      -35     7        NOP     EXIST   LU\n"
                   "2        var2  -9       -25     18       NOP     EXIST   CLF\n";

  prepare_data_start(sstable3, macro_data3, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data3, 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable3);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable3));
  STORAGE_LOG(INFO, "finish prepare sstable3");
  // make all trans running
  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(transaction::ObTransTableStatusType::COMMIT, 18))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
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
                        "0        var0  -9      -11     9        12      EXIST   CL\n"
                        "1        var1  -18     -35     7        11      EXIST   C\n"
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

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_uncommitted_row_commit2)
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

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -12     NOP     NOP     EXIST   U\n"
                  "1        var1   MIN    -10     NOP     6       EXIST   U\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -7      NOP     2       EXIST   U\n"
                  "1        var1   MIN    -6      7       2       EXIST   U\n"
                  "1        var1   -4     0       NOP     9       EXIST   L\n";

  macro_data[3] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2   -9     -5      NOP     9       EXIST   CL\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(macro_data, 1);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[3], 1, INT64_MAX);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObSSTable sstable2;
  const char* macro_data2[1];
  macro_data2[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                   "0        var0  -9      -11      9        NOP     EXIST   CL\n"
                   "2        var2  -9      -22      12       NOP     EXIST   F\n"
                   "2        var2  -4      -15      NOP      7       EXIST   L\n";

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

  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(transaction::ObTransTableStatusType::COMMIT, 18))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
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
                        "0        var0  -9      -11     9        12      EXIST   CL\n"
                        "1        var1  -18     -12     7        6       EXIST   C\n"
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

TEST_F(TestNewMinorFuserMerge, test_new_minor_fuser_with_multi_delete_row)
{
  GCONF._enable_sparse_row = false;
  ObMemtableCtxFactory mem_ctx;
  ObSSTableMergeCtx merge_context;
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  storage::ObTablesHandle tables_handle;
  ObSSTable sstable1;
  const char* macro_data[3];
  macro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -12     27      NOP     EXIST   U trans_id_1\n"
                  "1        var1   MIN    -11     24      NOP     EXIST   U trans_id_1\n";

  macro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -10     NOP     NOP     DELETE  U trans_id_2\n";

  macro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "1        var1   MIN    -7      NOP     2       EXIST   U trans_id_2\n"
                  "1        var1   MIN    -6      7       2       EXIST   U trans_id_2\n"
                  "1        var1   -4     0       NOP     9       EXIST   L trans_id_0\n";

  prepare_data_start(sstable1, macro_data, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);
  prepare_one_macro(&macro_data[0], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[1], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_one_macro(&macro_data[2], 1, INT64_MAX, nullptr, nullptr, true);
  prepare_data_end(sstable1);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));

  STORAGE_LOG(INFO, "finish prepare sstable1");

  int ret = OB_SUCCESS;
  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::COMMIT, transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version1[] = {40, 18};
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
                        "1        var1   -40    -12     27      NOP    EXIST   C\n"
                        "1        var1   -18    -10     NOP     NOP    DELETE  C\n"
                        "1        var1   -4     0       NOP     9      EXIST   CL\n";

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

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -rf test_new_minor_fuser.log*");
  OB_LOGGER.set_file_name("test_new_minor_fuser.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
