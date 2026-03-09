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

// 测试 ObFullTextIndexWritePipeline 和相关 Operator 的功能

#define USING_LOG_PREFIX STORAGE

#include "test_ddl_pipeline_base.h"
#include "storage/ddl/ob_ddl_sort_provider.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::observer;


namespace oceanbase
{

// Mock 函数定义
static const int64_t SLICE_IDX = 0;

int ObDDLRedoLogWriterCallback::wait()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObMacroBlockWriter::exec_callback(const ObStorageObjectHandle &macro_handle, ObMacroBlock *macro_block)
{
  int ret = OB_SUCCESS;
  return ret;
}

namespace unittest
{

class TestFulltextIndexPipelineAndOp : public ObSimpleClusterTestBase
{
public:
  TestFulltextIndexPipelineAndOp()
    : ObSimpleClusterTestBase("test_fulltext_index_pipeline_and_op_"),
      ls_id_(1001),
      tablet_id_(ObTabletID::INVALID_TABLET_ID),
      mem_context_(ROOT_CONTEXT),
      monitor_node_(),
      pipeline_(),
      allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "TFIPAO")),
      ddl_dag_(),
      table_schema_(),
      fts_macro_block_write_op_(&pipeline_),
      storage_schema_(nullptr)
  {}

  virtual ~TestFulltextIndexPipelineAndOp() = default;

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    // 如果共享租户还未创建，则创建它
    if (OB_INVALID_TENANT_ID == shared_tenant_id_) {
      LOG_INFO("create shared tenant start");
      ASSERT_EQ(OB_SUCCESS, create_tenant());
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(shared_tenant_id_));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
      LOG_INFO("create shared tenant end", K(shared_tenant_id_));
    }
    // 使用共享的租户 ID
    tenant_id_ = shared_tenant_id_;
  }

  virtual void TearDown() override
  {
    // 在租户上下文中清理 ddl_dag_ 资源，避免析构时 MTL() 返回空指针
    if (OB_INVALID_TENANT_ID != tenant_id_) {
      share::ObTenantSwitchGuard tenant_guard;
      if (OB_SUCCESS == tenant_guard.switch_to(tenant_id_)) {
        ddl_dag_.reuse();
      }
    }
    ObSimpleClusterTestBase::TearDown();
  }

  // 获取租户的 LS
  void get_tenant_ls(ObLS *&ls)
  {
    ls = nullptr;
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    share::ObLSID ls_id(1001);
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls = handle.get_ls());
  }

  int initialize_args_and_env(const ObObjType *col_types, const int64_t col_count, const int64_t rowkey_count, const bool need_multi_version_column_descs);

  int custom_ddl_dag();
protected:
  void prepare_table_schema(const ObObjType *col_types, const int64_t col_count, const int64_t rowkey_count);
  int create_tablet_to_ls();
  int initialize_operators();

  static const int64_t BDRS_MAX_BATCH_SIZE = 512;
  static uint64_t shared_tenant_id_;  // 共享的租户 ID
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  lib::MemoryContext mem_context_;
  sql::ObMonitorNode monitor_node_;
  storage::ObFullTextIndexWritePipeline pipeline_;
  ObArenaAllocator allocator_;
  observer::ObTableLoadDag ddl_dag_;
  ObTableSchema table_schema_;
  storage::ObDAGFtsMacroBlockWriteOp fts_macro_block_write_op_;
  ObStorageSchema *storage_schema_;
  // 用于 sort 算子的 RowMeta
  ObArenaAllocator row_meta_alloc_;
  RowMeta sk_row_meta_;
};

// 静态成员变量定义
uint64_t TestFulltextIndexPipelineAndOp::shared_tenant_id_ = OB_INVALID_TENANT_ID;

void TestFulltextIndexPipelineAndOp::prepare_table_schema(
    const ObObjType *col_obj_types,
    const int64_t col_count,
    const int64_t rowkey_count)
{
  ObColumnSchemaV2 column;
  uint64_t table_id = 500001;
  int64_t micro_block_size = 16 * 1024; // 16K

  // set data table schema
  table_schema_.reset();
  EXPECT_EQ(OB_SUCCESS, table_schema_.set_table_name("fulltext_index_test"));
  table_schema_.set_tenant_id(tenant_id_);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_count);
  table_schema_.set_max_used_column_id(col_count);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(ENCODING_ROW_STORE);

  // add column
  ObSqlString name_str;
  ObArray<share::schema::ObColDesc> cols_desc;
  for (int64_t i = 0; i < col_count; ++i) {
    ObObjType obj_type = col_obj_types[i];
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    name_str.assign_fmt("col%ld", i);
    EXPECT_EQ(OB_SUCCESS, column.set_column_name(name_str.ptr()));
    column.set_data_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObHexStringType == obj_type
        || ObNVarchar2Type == obj_type || ObNCharType == obj_type || ObTextType == obj_type) {
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      if (ObCharType == obj_type || ObVarcharType == obj_type) {
        const int64_t max_char_length = lib::is_oracle_mode()
                                        ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                        : OB_MAX_CHAR_LENGTH;
        column.set_data_length(max_char_length);
      }
    } else {
      column.set_collation_type(CS_TYPE_BINARY);
    }

    if (i < rowkey_count) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    EXPECT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

int TestFulltextIndexPipelineAndOp::initialize_args_and_env(
    const ObObjType *col_types,
    const int64_t col_count,
    const int64_t rowkey_count,
    const bool need_multi_version_column_descs)
{
  int ret = OB_SUCCESS;
  const int64_t px_thread_count = 16;
  uint64_t tenant_data_version = DATA_VERSION_4_4_0_0;
  int64_t snapshot_version = common::ObTimeUtility::current_time();
  const int64_t ddl_table_id = 500001;
  const int64_t ddl_task_id = 5000000;
  const int64_t ddl_execution_id = 0;

  allocator_.set_attr(ObMemAttr(tenant_id_, "TFIPAO"));
  prepare_table_schema(col_types, col_count, rowkey_count);
  ddl_dag_.ddl_task_param_.tenant_data_version_ = tenant_data_version;
  ddl_dag_.ddl_task_param_.is_no_logging_ = true;
  ddl_dag_.ddl_task_param_.ddl_task_id_ = ddl_task_id;
  ddl_dag_.ddl_task_param_.snapshot_version_ = snapshot_version;
  ddl_dag_.ddl_task_param_.schema_version_ = common::ObTimeUtility::current_time();
  ddl_dag_.direct_load_type_ = storage::ObDirectLoadMgrUtil::ddl_get_direct_load_type(GCTX.is_shared_storage_mode(), tenant_data_version);
  ddl_dag_.ddl_task_param_.target_table_id_ = ddl_table_id;
  ddl_dag_.ddl_task_param_.execution_id_ = ddl_execution_id;
  ddl_dag_.direct_load_type_ = ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL;
  ddl_dag_.set_start_time();
  ddl_dag_.ObDDLIndependentDag::is_inited_ = true;
  ddl_dag_.is_inited_ = true;
  ObDDLTabletContext *tablet_context = OB_NEWx(ObDDLTabletContext, &allocator_);
  ObStorageSchema *storage_schema = OB_NEWx(ObStorageSchema, &allocator_);

  if (nullptr == tablet_context || nullptr == storage_schema) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate an ObDDLTabletContext obj", K(ret), KP(tablet_context), KP(storage_schema));
  } else if (OB_FAIL(ddl_dag_.ObIndependentDag::cond_.init(ObWaitEventIds::INDEPENDENT_DAG_COND_WAIT))) {
    // 绕过 basic_init 中对 dag scheduler 的检查，手动初始化 ObIndependentDag
    LOG_WARN("fail to init cond", K(ret));
  } else if (FALSE_IT(ddl_dag_.ObIndependentDag::allocator_ = &allocator_)) {
  } else if (FALSE_IT(ddl_dag_.ObIndependentDag::is_inited_ = true)) {
  } else {
    tablet_context->ls_id_ = ls_id_;
    tablet_context->tablet_id_ = tablet_id_;
    if (OB_FAIL(storage_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("fail to init storage schema", K(ret));
    } else {
      tablet_context->tablet_param_.storage_schema_ = storage_schema;
      tablet_context->tablet_param_.tablet_transfer_seq_ = common::ObTimeUtility::current_time();
      tablet_context->tablet_param_.is_micro_index_clustered_ = true;
      tablet_context->tablet_param_.with_cs_replica_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    const ObMemAttr bucket_attr(tenant_id_, "TFIPAO");
    if (!ddl_dag_.tablet_context_map_.created() && OB_FAIL(ddl_dag_.tablet_context_map_.create(5, bucket_attr))) {
      LOG_WARN("fail to create tablet context map", K(ret));
    } else if (OB_FAIL(ddl_dag_.tablet_context_map_.set_refactored(tablet_context->tablet_id_, tablet_context))) {
      LOG_WARN("fail to set tablet context", K(ret), K(tablet_context->tablet_id_));
    }
  }

  if (OB_SUCC(ret)) { // prepare ddl table schema
    ObArray<share::schema::ObColDesc> cols_desc;
    if (need_multi_version_column_descs && OB_FAIL(table_schema_.get_multi_version_column_descs(cols_desc))) {
      LOG_WARN("fail to get cols desc", K(ret));
    } else if (!need_multi_version_column_descs && OB_FAIL(table_schema_.get_rowkey_column_ids(cols_desc))) {
      LOG_WARN("fail to get cols desc", K(ret));
    } else {
      ddl_dag_.ddl_table_schema_.table_item_.rowkey_column_num_ = table_schema_.get_rowkey_column_num();
      ddl_dag_.ddl_table_schema_.storage_schema_ = storage_schema;
      for (int64_t i = 0; OB_SUCC(ret) && i < cols_desc.count(); ++i) {
        const ObColDesc &col_desc = cols_desc.at(i);
        const schema::ObColumnSchemaV2 *column_schema = nullptr;
        ObColumnSchemaItem column_item;
        if (i < rowkey_count) {
          column_item.is_rowkey_column_ = true;
        } else {
          column_item.is_rowkey_column_ = false;
        }
        if (i >= ddl_dag_.ddl_table_schema_.table_item_.rowkey_column_num_
            && i < ddl_dag_.ddl_table_schema_.table_item_.rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
          column_item.col_type_ = col_desc.col_type_;
        } else if (OB_ISNULL(column_schema = table_schema_.get_column_schema(col_desc.col_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the column schema is null", K(ret), K(col_desc));
        } else {
          column_item.is_valid_ = true;
          column_item.col_type_ = column_schema->get_meta_type();
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ddl_dag_.ddl_table_schema_.column_items_.push_back(column_item))) {
            LOG_WARN("fail to push back column schema", K(ret));
          }
        }
      }
    }
  }

  LOG_INFO("initialize_args_and_env: before create_tablet_to_ls", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_),
    K(ddl_dag_.ddl_table_schema_.column_items_.count()), K(storage_schema->get_column_count()), K(table_schema_.get_column_count()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_tablet_to_ls())) {
      LOG_WARN("fail to create tablet", K(ret), K(tablet_id_));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t ddl_thread_count = 1;
    if (OB_FAIL(tablet_context->init(ls_id_,
                                     tablet_id_,
                                     ddl_thread_count,
                                     snapshot_version,
                                     ddl_dag_.direct_load_type_,
                                     ddl_dag_.ddl_table_schema_))) {
      LOG_WARN("fail to initilize tablet context",
          K(ret), K(ls_id_), K(tablet_id_));
    }
  }

  if (OB_SUCC(ret)) {
    pipeline_.set_dag(ddl_dag_);
    if (OB_FAIL(initialize_operators())) {
      LOG_WARN("fail to initialize operators", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    storage_schema_ = tablet_context->tablet_param_.storage_schema_;
  }
  return ret;
}

int TestFulltextIndexPipelineAndOp::custom_ddl_dag()
{
  int ret = OB_SUCCESS;
  ddl_dag_.ddl_table_schema_.table_item_.index_type_ = ObIndexType::INDEX_TYPE_FTS_DOC_WORD_LOCAL;
  ddl_dag_.fts_word_doc_ddl_table_schema_.table_item_.rowkey_column_num_ = 2;
  ddl_dag_.ddl_task_param_.max_batch_size_ = 256;
  // 创建 4 个 column items
  const int64_t column_count = 2;
  ObColumnSchemaItem col_items[column_count];

  // Column 0: VARCHAR, utf8mb4_general_ci, length=255
  col_items[0].is_valid_ = true;
  col_items[0].col_type_.set_type(ObVarcharType);
  col_items[0].col_type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  col_items[0].col_accuracy_.set_length(255);
  col_items[0].col_accuracy_.set_precision(-1);
  col_items[0].col_accuracy_.set_scale(-1);
  col_items[0].column_flags_ = 33554432;
  col_items[0].is_rowkey_column_ = true;
  col_items[0].is_nullable_ = true;

  // Column 1: VARCHAR, binary, length=16
  col_items[1].is_valid_ = true;
  col_items[1].col_type_.set_type(ObVarcharType);
  col_items[1].col_type_.set_collation_type(CS_TYPE_BINARY);
  col_items[1].col_accuracy_.set_length(16);
  col_items[1].col_accuracy_.set_precision(-1);
  col_items[1].col_accuracy_.set_scale(-1);
  col_items[1].column_flags_ = 4194304;
  col_items[1].is_rowkey_column_ = true;
  col_items[1].is_nullable_ = false;

  // 添加到 column_items_
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    if (OB_FAIL(ddl_dag_.fts_word_doc_ddl_table_schema_.column_items_.push_back(col_items[i]))) {
      LOG_WARN("push back column item failed", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_dag_.init_sort_provider())) {
      LOG_WARN("fail to init sort provider", K(ret));
    }
  }
  return ret;
}

int TestFulltextIndexPipelineAndOp::initialize_operators()
{
  int ret = OB_SUCCESS;

  // 构造 sk_row_meta_：第一列是定长 int64，第二列是变长 varchar
  common::ObArray<ColMetaInfo> expr_infos;
  if (OB_FAIL(expr_infos.reserve(2))) {
    LOG_WARN("fail to reserve expr_infos", K(ret));
  } else {
    // 第一列：定长 int64
    ColMetaInfo int_info(true /*is_fixed*/, static_cast<uint32_t>(sizeof(int64_t)));
    // 第二列：变长 varchar
    ColMetaInfo varchar_info(false /*is_fixed*/, 0 /*fixed_length*/);
    if (OB_FAIL(expr_infos.push_back(int_info))) {
      LOG_WARN("fail to push back int ColMetaInfo", K(ret));
    } else if (OB_FAIL(expr_infos.push_back(varchar_info))) {
      LOG_WARN("fail to push back varchar ColMetaInfo", K(ret));
    } else if (OB_FAIL(sk_row_meta_.init(expr_infos, /*extra_size*/0, true, &row_meta_alloc_))) {
      LOG_WARN("fail to init sk_row_meta_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(fts_macro_block_write_op_.init(tablet_id_, SLICE_IDX))) {
    LOG_WARN("fail to initialize fts macro block write operator", K(ret));
  }
  return ret;
}

int TestFulltextIndexPipelineAndOp::create_tablet_to_ls()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;

  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ret), K(ls_id_));
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle,
                                                     tablet_id_,
                                                     table_schema_,
                                                     allocator_,
                                                     ObTabletStatus::NORMAL,
                                                     share::SCN::min_scn(),
                                                     tablet_handle))) {
    LOG_WARN("fail to create tablet", K(ret), K(ls_id_), K(table_schema_));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle,
                                                             true /*try_create]*/))) {
    LOG_WARN("failed to create ddl kv mgr", K(ret));
  }
  return ret;
}

// Test case 1: test_fts_macro_block_write_op
TEST_F(TestFulltextIndexPipelineAndOp, test_fts_macro_block_write_op)
{
  // 切换到共享租户上下文
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  ObBatchRowsGen batch_rows_gen;
  blocksstable::ObBatchDatumRows batch_rows;
  ObChunk input_chunk;
  ObChunk output_chunk;
  ObPipelineOperator::ResultState result_state;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 2;
  const int64_t storage_col_count = 4;
  const int64_t batch_size = 512;
  const int64_t test_batch_count = 10;
  ObObjType col_types[col_count] = {ObIntType, ObVarcharType};
  ObObjType storage_col_types[storage_col_count] = {ObIntType, ObVarcharType, ObIntType, ObIntType};

  tablet_id_ = 2000005;
  EXPECT_EQ(OB_SUCCESS, batch_rows_gen.init(storage_col_types, storage_col_count, rowkey_count));
  EXPECT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count, false));

  // 创建 direct_load_batch_rows 对象
  ObDirectLoadBatchDatumRows direct_load_batch_rows;

  // Test with multiple batches
  for (int64_t i = 0; i < test_batch_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, batch_rows_gen.get_batch_rows(batch_rows, batch_size));
    input_chunk.type_ = ObChunk::DIRECT_LOAD_BATCH_DATUM_ROWS;
    input_chunk.direct_load_batch_rows_ = &direct_load_batch_rows;
    input_chunk.direct_load_batch_rows_->datum_rows_.shadow_copy(batch_rows);
    EXPECT_EQ(OB_SUCCESS, fts_macro_block_write_op_.execute(input_chunk, result_state, output_chunk));
    EXPECT_EQ(ObPipelineOperator::NEED_MORE_INPUT, result_state);
  }


  // 清除指针，防止析构时尝试释放栈上对象
  input_chunk.reset();
  output_chunk.reset();
}

// Test case 2: test_fulltext_index_pipeline
TEST_F(TestFulltextIndexPipelineAndOp, test_fulltext_index_pipeline)
{
  LOG_INFO("===== test_fulltext_index_pipeline START =====");

  // 切换到共享租户上下文
  LOG_INFO("Switching to tenant context", K(tenant_id_));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  ObBatchRowsGen batch_rows_gen;
  blocksstable::ObBatchDatumRows batch_rows;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 2;
  const int64_t storage_col_count = 4;
  const int64_t batch_size = 128;
  const int64_t direct_load_batch_count = 5;  // ObDirectLoadBatchDatumRows 数组长度
  ObObjType col_types[col_count] = {ObIntType, ObVarcharType};
  ObObjType storage_col_types[storage_col_count] = {ObIntType, ObVarcharType, ObIntType, ObIntType};
  tablet_id_ = 2000007;
  storage::ObFullTextIndexWritePipeline *task_ptr = nullptr;
  ObDDLSlice *ddl_slice = nullptr;
  bool is_new_slice = false;
  lib::MemoryContext task_mem_context(ROOT_CONTEXT);
  sql::ObMonitorNode task_monitor_node;

  LOG_INFO("Initializing args and env");
  EXPECT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count, false));
  LOG_INFO("initialize_args_and_env done, calling custom_ddl_dag");
  EXPECT_EQ(OB_SUCCESS, custom_ddl_dag());
  LOG_INFO("custom_ddl_dag done, initializing batch_rows_gen");
  EXPECT_EQ(OB_SUCCESS, batch_rows_gen.init(storage_col_types, storage_col_count, rowkey_count));
  LOG_INFO("batch_rows_gen init done");

  // 获取或创建 ddl_slice
  LOG_INFO("Getting tablet_context", K(tablet_id_));
  ObDDLTabletContext *tablet_context = nullptr;
  EXPECT_EQ(OB_SUCCESS, ddl_dag_.get_tablet_context(tablet_id_, tablet_context));
  LOG_INFO("Getting or creating ddl_slice", K(SLICE_IDX));
  EXPECT_EQ(OB_SUCCESS, tablet_context->get_or_create_slice(SLICE_IDX, ddl_slice, is_new_slice));
  LOG_INFO("ddl_slice created", K(is_new_slice));

  // 创建 ObDirectLoadBatchDatumRows 数组，长度为 5，并自动构造数据
  // 注意：每个元素需要独立的 batch_rows，因为 shadow_copy 是浅拷贝
  // 使用 OB_NEW 分配 direct_load_batch，因为 ObChunk::~ObChunk 会用 ob_free 释放
  LOG_INFO("Creating direct_load_batch arrays", K(direct_load_batch_count), K(batch_size));
  ObDirectLoadBatchDatumRows *direct_load_batch_arr[direct_load_batch_count];
  blocksstable::ObBatchDatumRows *batch_rows_arr[direct_load_batch_count];
  for (int64_t i = 0; i < direct_load_batch_count; ++i) {
    // 使用 OB_NEW 分配，与 ObChunk::~ObChunk 中的 ob_free 匹配
    direct_load_batch_arr[i] = OB_NEW(ObDirectLoadBatchDatumRows, ObMemAttr(tenant_id_, "ddl_test_dl"));
    batch_rows_arr[i] = OB_NEWx(blocksstable::ObBatchDatumRows, &allocator_);
    EXPECT_NE(nullptr, direct_load_batch_arr[i]);
    EXPECT_NE(nullptr, batch_rows_arr[i]);
    // 生成 batch_rows 数据（每个元素使用独立的 batch_rows）
    EXPECT_EQ(OB_SUCCESS, batch_rows_gen.get_batch_rows(*batch_rows_arr[i], batch_size, &allocator_));
    // 将数据拷贝到 direct_load_batch
    direct_load_batch_arr[i]->datum_rows_.shadow_copy(*batch_rows_arr[i]);
    LOG_INFO("Created batch", K(i));
  }

  // 将 direct_load_batch_arr 中的数据放入 ddl_slice 的 chunk_queue_
  LOG_INFO("Pushing chunks to ddl_slice", K(direct_load_batch_count));
  for (int64_t i = 0; i < direct_load_batch_count; ++i) {
    ObChunk *chunk = OB_NEW(ObChunk, ObMemAttr(tenant_id_, "ddl_test_chunk"));
    EXPECT_NE(nullptr, chunk);
    chunk->type_ = ObChunk::DIRECT_LOAD_BATCH_DATUM_ROWS;
    chunk->direct_load_batch_rows_ = direct_load_batch_arr[i];
    EXPECT_EQ(OB_SUCCESS, ddl_slice->push_chunk(chunk));
    LOG_INFO("Pushed chunk to ddl_slice", K(i));
  }

  // 推送 end_chunk
  LOG_INFO("Pushing end_chunk to ddl_slice");
  ObChunk *end_chunk = OB_NEW(ObChunk, ObMemAttr(tenant_id_, "ddl_end_chunk"));
  EXPECT_NE(nullptr, end_chunk);
  end_chunk->set_end_chunk();
  ObChunk *end_chunk_ptr = end_chunk;
  EXPECT_EQ(OB_SUCCESS, ddl_slice->push_chunk(end_chunk_ptr));
  LOG_INFO("end_chunk pushed successfully");

  // 创建 pipeline 任务并添加到 DAG（使用 dag allocator 分配）
  LOG_INFO("Creating pipeline task and adding to DAG");
  ddl_dag_.dag_status_ = ObIDag::DAG_STATUS_NODE_RUNNING;
  EXPECT_EQ(OB_SUCCESS, ddl_dag_.alloc_task(task_ptr));
  LOG_INFO("alloc_task done, calling task_ptr->init");
  // 调用 init 方法（内部会调用基类的 init 方法）
  EXPECT_EQ(OB_SUCCESS, task_ptr->init(ddl_slice));
  LOG_INFO("task_ptr->init done, calling add_task");
  EXPECT_EQ(OB_SUCCESS, ddl_dag_.add_task(*task_ptr));
  LOG_INFO("add_task done, calling ddl_dag_.process()");
  EXPECT_EQ(OB_SUCCESS, ddl_dag_.process());
  LOG_INFO("ddl_dag_.process() completed successfully");

  // ===== Verify that each chunk in ddl_slice is internally sorted =====
  LOG_INFO("===== Start verifying sorted chunks in ddl_slice =====");
  using StoreRow = sql::ObSortKeyStore<false>;
  using VerifyCompare = ObInvertedIndexCompare<StoreRow>;
  using ChunkType = sql::ObSortVecOpChunk<StoreRow, false>;

  // Get RowMeta from sort_provider for data comparison
  LOG_INFO("Getting sort_provider from ddl_dag_");
  ObDDLSortProvider *sort_provider = ddl_dag_.get_sort_provider();
  ASSERT_NE(nullptr, sort_provider);
  const sql::RowMeta &row_meta = sort_provider->get_sk_row_meta();
  LOG_INFO("Got sort_provider and row_meta");

  // Create and init compare object for verification
  LOG_INFO("Creating and initializing verify_compare");
  VerifyCompare verify_compare;
  ASSERT_EQ(OB_SUCCESS, verify_compare.init(&ddl_dag_.fts_word_doc_ddl_table_schema_, &row_meta, false/*enable_encode_sortkey*/));
  LOG_INFO("verify_compare initialized");

  // Access ddl_sort_chunks_ (private member accessible due to #define private public)
  ObArray<ObDDLSortChunk> &sorted_chunks = ddl_slice->ddl_sort_chunks_;
  LOG_INFO("Number of sorted chunks to verify", K(sorted_chunks.count()));
  ASSERT_GT(sorted_chunks.count(), 0) << "Should have at least one sorted chunk";

  // Verify each chunk is internally sorted
  LOG_INFO("Starting chunk-by-chunk verification loop");
  for (int64_t chunk_idx = 0; chunk_idx < sorted_chunks.count(); ++chunk_idx) {
    LOG_INFO("Verifying chunk", K(chunk_idx), "total_chunks", sorted_chunks.count());
    ObDDLSortChunk &ddl_chunk = sorted_chunks.at(chunk_idx);
    void *raw_chunk = ddl_chunk.get_sort_op_chunk();
    ASSERT_NE(nullptr, raw_chunk) << "Chunk " << chunk_idx << " should not be null";

    ChunkType *chunk = reinterpret_cast<ChunkType *>(raw_chunk);
    LOG_INFO("Initializing row iterator for chunk", K(chunk_idx));
    ASSERT_EQ(OB_SUCCESS, chunk->init_row_iter()) << "Failed to init row iterator for chunk " << chunk_idx;

    const StoreRow *prev_row = nullptr;
    int64_t row_count = 0;
    int ret = OB_SUCCESS;

    while (OB_SUCCESS == (ret = chunk->get_next_row())) {
      const StoreRow *curr_row = chunk->sk_row_;
      ASSERT_NE(nullptr, curr_row) << "Current row should not be null";

      if (prev_row != nullptr) {
        // verify_compare(l, r) returns true if l < r
        // If verify_compare(curr_row, prev_row) is true, it means curr_row < prev_row, i.e., out of order
        bool is_out_of_order = verify_compare(curr_row, prev_row);
        ASSERT_FALSE(is_out_of_order) << "Chunk " << chunk_idx << " row " << row_count
                                      << " is out of order compared to previous row";
      }

      prev_row = curr_row;
      row_count++;
    }

    ASSERT_EQ(OB_ITER_END, ret) << "Expected OB_ITER_END at end of chunk iteration";
    ASSERT_EQ(direct_load_batch_count * batch_size, row_count) << "Verify compare error code is not OB_SUCCESS";
    LOG_INFO("Chunk verification passed", K(chunk_idx), K(row_count));
  }
  LOG_INFO("===== All sorted chunks verified successfully =====");
  LOG_INFO("===== test_fulltext_index_pipeline END =====");
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_fulltext_index_create.log*");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_fulltext_index_create.log", true, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
