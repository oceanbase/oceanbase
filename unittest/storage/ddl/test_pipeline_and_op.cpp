/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <stdexcept>
#include <chrono>

#define private public
#define protected public

#include "storage/blocksstable/ob_dag_macro_block_writer.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/ddl/ob_cg_row_tmp_file.h"
#include "storage/ddl/ob_cg_block_tmp_files_iterator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "common/ob_version_def.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "share/ob_ls_id.h"
#include "storage/ddl/ob_cg_micro_block_write_op.h"
#include "storage/ddl/ob_cg_macro_block_write_op.h"
#include "storage/ddl/test_batch_rows_generater.h"
#include "storage/ddl/ob_pipeline.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "share/schema/ob_schema_utils.h"
#include "storage/ddl/ob_cg_macro_block_write_task.h"
#include "storage/ddl/test_mock_row_iters.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_insert_sstable_task.h"
#include "observer/table_load/dag/ob_table_load_dag_parallel_merger.h"

namespace oceanbase
{
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
class TestCGMicroMacroWriteOp : public ::testing::Test
{
public:
  enum OpsType {
    INVALID_OP = 0,
    WRITE_MICRO_AND_MACRO_OP = 1,
    DDL_WRITE_MACRO_BLOCK_OP = 2,
    SKIP_TYPE
  };

public:
  TestCGMicroMacroWriteOp() :
    ls_id_(1001),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(0),
    pipeline_(ObITask::ObITaskType::TASK_TYPE_DDL_WRITE_PIPELINE),
    allocator_(ObMemAttr(MTL_ID(), "TCGMMWOp")),
    ddl_dag_(),
    table_schema_(),
    micro_write_op_(&pipeline_),
    macro_write_op_(&pipeline_),
    ddl_write_macro_block_op_(&pipeline_),
    output_cg_row_files_data_(nullptr),
    cg_row_file_arr_(),
    storage_schema_(nullptr),
    scan_end_(false),
    ops_type_(INVALID_OP) { }
  virtual ~TestCGMicroMacroWriteOp() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  int initialize_args_and_env(const ObObjType *col_types, const int64_t col_count, const int64_t rowkey_count);
  int write_bdrs_into_cg_row_temp_files(const blocksstable::ObBatchDatumRows &bdrs);

private:
  void prepare_table_schema(const ObObjType *col_types, const int64_t col_count, const int64_t rowkey_count);
  int create_ls();
  int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  int create_tablet_to_ls();
  int create_pipeline();
  int initialize_write_ops();
  int initialize_cg_row_temp_file_env();
  int try_generate_output_chunk(bool scan_end = false);

private:
  static const int64_t TENANT_MEMORY = 16L * 1024L * 1024L * 1024L /* 16 GB */;
  static const int64_t BDRS_MAX_BATCH_SIZE = 512;
  static const int64_t CG_ROW_FILE_MEMEORY_LIMIT = 64 * 1024 /* 64 KB */;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t slice_idx_;
  ObPipeline pipeline_;
  ObArenaAllocator allocator_;
  observer::ObTableLoadDag ddl_dag_;
  ObTableSchema table_schema_;
  ObCGMicroBlockWriteOp micro_write_op_;
  ObDAGCGMacroBlockWriteOp macro_write_op_;
  ObDDLWriteMacroBlockOperator ddl_write_macro_block_op_;
  ObArray<ObCGRowFile *> *output_cg_row_files_data_;
  ObArray<ObCGRowFile *> cg_row_file_arr_;
  ObStorageSchema *storage_schema_;
  bool scan_end_;
  OpsType ops_type_;
};

void TestCGMicroMacroWriteOp::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "SetUpTestCase");
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  CHUNK_MGR.set_limit(TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), TENANT_MEMORY);
}

void TestCGMicroMacroWriteOp::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestCGMicroMacroWriteOp::SetUp()
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(true, MockTenantModuleEnv::get_instance().is_inited());
}

void TestCGMicroMacroWriteOp::TearDown()
{

}

void TestCGMicroMacroWriteOp::prepare_table_schema(const ObObjType *col_obj_types, const int64_t col_count, const int64_t rowkey_count)
{
  ObColumnSchemaV2 column;
  uint64_t table_id = 500001;
  int64_t micro_block_size = 16 * 1024; // 16K

  // set data table schema
  table_schema_.reset();
  EXPECT_EQ(OB_SUCCESS, table_schema_.set_table_name("micro_macro_write_op"));
  table_schema_.set_tenant_id(MTL_ID());
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_count);
  table_schema_.set_max_used_column_id(col_count);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(ENCODING_ROW_STORE);

  //add column
  ObSqlString name_str;
  ObArray<share::schema::ObColDesc> cols_desc;
  for(int64_t i = 0; i < col_count; ++i) {
    ObObjType obj_type = col_obj_types[i];
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    name_str.assign_fmt("tese%ld", i);
    EXPECT_EQ(OB_SUCCESS, column.set_column_name(name_str.ptr()));
    column.set_data_type(obj_type);

    if (ObVarcharType == obj_type || ObCharType == obj_type || ObHexStringType == obj_type
        || ObNVarchar2Type == obj_type || ObNCharType == obj_type || ObTextType == obj_type) {
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      if (ObCharType == obj_type) {
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
  EXPECT_EQ(OB_SUCCESS, ObSchemaUtils::build_add_each_column_group(table_schema_, table_schema_));
}

int TestCGMicroMacroWriteOp::initialize_args_and_env(const ObObjType *col_types, const int64_t col_count, const int64_t rowkey_count)
{
  int ret = OB_SUCCESS;
  const int64_t px_thread_count = 16;
  uint64_t tenant_data_version = DATA_VERSION_4_4_0_0;
  int64_t snapshot_version = common::ObTimeUtility::current_time();
  const int64_t ddl_table_id = 500001;
  const int64_t ddl_task_id = 5000000;
  const int64_t ddl_execution_id = 0;

  allocator_.set_attr(ObMemAttr(MTL_ID(), "TCGMMWOp"));
  prepare_table_schema(col_types, col_count, rowkey_count);
  ddl_dag_.ddl_task_param_.tenant_data_version_ = tenant_data_version;
  ddl_dag_.ddl_task_param_.is_no_logging_ = true;
  ddl_dag_.ddl_task_param_.ddl_task_id_ = ddl_task_id;
  ddl_dag_.ddl_task_param_.snapshot_version_ = snapshot_version;
  ddl_dag_.ddl_task_param_.schema_version_ = common::ObTimeUtility::current_time();
  ddl_dag_.direct_load_type_ = ObDirectLoadMgrUtil::ddl_get_direct_load_type(GCTX.is_shared_storage_mode(), tenant_data_version);
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
  } else if (OB_FAIL(ddl_dag_.ObIndependentDag::basic_init(allocator_))) {
    LOG_WARN("fail to initialize base dag", K(ret));
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
    const ObMemAttr bucket_attr(MTL_ID(), "TCGMMWOP");
    if (OB_FAIL(!ddl_dag_.tablet_context_map_.created() && ddl_dag_.tablet_context_map_.create(5, bucket_attr))) {
      LOG_WARN("fail to create tablet context map", K(ret));
    } else if (OB_FAIL(ddl_dag_.tablet_context_map_.set_refactored(tablet_context->tablet_id_, tablet_context))) {
      LOG_WARN("fail to set tablet context", K(ret), K(tablet_context->tablet_id_));
    }
  }

  if (OB_SUCC(ret)) { // prepare ddl table schema
    ObArray<share::schema::ObColDesc> cols_desc;
    if (OB_FAIL(table_schema_.get_multi_version_column_descs(cols_desc))) {
      LOG_WARN("fail to get cols desc", K(ret));
    } else {
      ddl_dag_.ddl_table_schema_.table_item_.rowkey_column_num_ = table_schema_.get_rowkey_column_num();
      ddl_dag_.ddl_table_schema_.storage_schema_ = storage_schema;
      for (int64_t i = 0; OB_SUCC(ret) && i < cols_desc.count(); ++i) {
        const ObColDesc &col_desc = cols_desc.at(i);
        const schema::ObColumnSchemaV2 *column_schema = nullptr;
        ObColumnSchemaItem column_item;
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_ls())) {
      LOG_WARN("fail to create ls", K(ret), K(ls_id_));
    } else if (OB_FAIL(create_tablet_to_ls())) {
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
    if (OB_FAIL(initialize_write_ops())) {
      LOG_WARN("fail to initialize write ops", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_pipeline())) {
      LOG_WARN("fail to create pipeline", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(initialize_cg_row_temp_file_env())) {
      LOG_WARN("fail to initialize cg row temp file env", K(ret));
    }
  }
  return ret;
}

int TestCGMicroMacroWriteOp::initialize_write_ops()
{
  int ret = OB_SUCCESS;
  if (INVALID_OP == ops_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the ops type is invalid", K(ret), K(ops_type_));
  } else if (WRITE_MICRO_AND_MACRO_OP == ops_type_) {
    if (OB_FAIL(micro_write_op_.init(tablet_id_, 0))) {
      LOG_WARN("fail to initialize micro write op", K(ret));
    } else if (OB_FAIL(macro_write_op_.init(tablet_id_, 0))) {
      LOG_WARN("fail to initialize macro write op", K(ret));
    }
  } else if (DDL_WRITE_MACRO_BLOCK_OP == ops_type_) {
    ObWriteMacroParam writer_param;
    if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, 0, -1, &ddl_dag_, 0/*max_batch_size*/, writer_param))) {
      LOG_WARN("fail to fill writer param", K(ret));
    } else if (OB_FAIL(ddl_write_macro_block_op_.init(writer_param))) {
      LOG_WARN("fail to initialize ddl write macro block op", K(ret));
    }
  }
  return ret;
}

int TestCGMicroMacroWriteOp::create_ls(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;

  ObLSService *ls_svr = MTL(ObLSService*);
  bool b_exist = false;
  ObLS *ls = nullptr;
  obrpc::ObCreateLSArg create_ls_arg;

  if (OB_FAIL(gen_create_ls_arg(tenant_id, ls_id, create_ls_arg))) {
    STORAGE_LOG(WARN, "failed to build create ls arg", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_svr->create_ls(create_ls_arg))) {
    STORAGE_LOG(WARN, "failed to create ls", K(create_ls_arg));
  } else if (OB_FAIL(ls_svr->check_ls_exist(ls_id, b_exist))) {
    STORAGE_LOG(WARN, "failed to check ls exist", K(ls_id));
  } else if (!b_exist) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, ls does not exist", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls", K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_handle));
  }

  // set member list
  if (OB_SUCC(ret)) {
    ObMemberList member_list;
    const int64_t paxos_replica_num = 1;
    (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
    GlobalLearnerList learner_list;
    if (OB_FAIL(ls->set_initial_member_list(member_list, paxos_replica_num, learner_list))) {
      STORAGE_LOG(WARN, "failed to set initial member list", K(ret),
          K(member_list), K(paxos_replica_num));
    }
  }

  // check leader
  STORAGE_LOG(INFO, "check leader");
  ObRole role;
  for (int i = 0; OB_SUCC(ret) && i < 15; i++) {
    int64_t proposal_id = 0;
    if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
      STORAGE_LOG(WARN, "failed to get role", K(ret));
    } else if (role == ObRole::LEADER) {
      break;
    }
    ::sleep(1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, role is not leader", K(ret), K(role));
  }

  return ret;
}

int TestCGMicroMacroWriteOp::create_ls()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  share::ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;

  if (OB_FAIL(create_ls(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("fail to create ls", K(ret), K(ls_id_));
  }
  return ret;
}

int TestCGMicroMacroWriteOp::create_tablet_to_ls()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;

  if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
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

int TestCGMicroMacroWriteOp::create_pipeline()
{
  int ret = OB_SUCCESS;
  if (INVALID_OP == ops_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the ops type is invalid", K(ret), K(ops_type_));
  } else if (WRITE_MICRO_AND_MACRO_OP == ops_type_) {
    if (OB_FAIL(pipeline_.add_op(&micro_write_op_)) ||
        OB_FAIL(pipeline_.add_op(&macro_write_op_))) {
      LOG_WARN("fail to add ops", K(ret));
    }
  } else if (DDL_WRITE_MACRO_BLOCK_OP == ops_type_) {
    if (OB_FAIL(pipeline_.add_op(&ddl_write_macro_block_op_))) {
      LOG_WARN("fail to add ddl write macro block op", K(ret));
    }
  }
  return ret;
}

int TestCGMicroMacroWriteOp::initialize_cg_row_temp_file_env()
{
  int ret = OB_SUCCESS;
  output_cg_row_files_data_ = OB_NEWx(ObArray<ObCGRowFile *>, &allocator_);
  if (OB_UNLIKELY(nullptr == output_cg_row_files_data_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate output cg row files data", K(ret));
  } else {
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(ddl_dag_.get_tablet_context(tablet_id_, tablet_context))) {
      LOG_WARN("fail to get tablet context", K(ret), K(tablet_id_));
    } else if (OB_UNLIKELY(nullptr == tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret));
    } else {
      storage_schema_ = tablet_context->tablet_param_.storage_schema_;
      if (OB_UNLIKELY(nullptr == storage_schema_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage schema is null", K(ret));
      } else {
        const int64_t cg_count = storage_schema_->get_column_group_count();
        if (cg_count <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cg count is invalid", K(ret), K(cg_count));
        } else if (OB_FAIL(cg_row_file_arr_.prepare_allocate(cg_count))) {
          LOG_WARN("fail to prepare allocate cg block files", K(ret), K(cg_count));
        } else {
          for (int i = 0; i < cg_row_file_arr_.count(); ++i) {
            cg_row_file_arr_.at(i) = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int TestCGMicroMacroWriteOp::write_bdrs_into_cg_row_temp_files(const blocksstable::ObBatchDatumRows &bdrs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema_->get_column_groups();
  ObBatchDatumRows cg_rows;
  cg_rows.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  cg_rows.mvcc_row_flag_ = bdrs.mvcc_row_flag_;
  cg_rows.row_count_ = bdrs.row_count_;
  cg_rows.trans_id_ = bdrs.trans_id_;

  for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_schemas.count(); ++cg_idx) {
    ObCGRowFile *cg_row_file = nullptr;
    const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(cg_idx);
    if (OB_UNLIKELY(cg_idx >= cg_row_file_arr_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg idx is invalid", K(ret), K(cg_idx), K(cg_row_file_arr_.count()));
    } else {
      cg_row_file = cg_row_file_arr_.at(cg_idx);
      if (nullptr == cg_row_file) {
        const ObIArray<ObColumnSchemaItem>  &all_column_schema_its = ddl_dag_.ddl_table_schema_.column_items_;
        cg_row_file = OB_NEW(ObCGRowFile, ObMemAttr(MTL_ID(), "CGRowFile"));
        if (OB_UNLIKELY(nullptr == cg_row_file)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate cg row file obj", K(ret));
        } else if (OB_FAIL(cg_row_file->open(all_column_schema_its,
                                             tablet_id_,
                                             slice_idx_,
                                             cg_idx,
                                             cg_schema,
                                             BDRS_MAX_BATCH_SIZE))) {
          LOG_WARN("fail to open cg block file", K(ret), K(tablet_id_), K(slice_idx_), K(cg_idx));
        } else {
          cg_row_file_arr_.at(cg_idx) = cg_row_file;
        }
      }
    }
    if (OB_SUCC(ret)) {
      cg_rows.vectors_.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < cg_schema.get_column_count(); ++j) {
        const int64_t column_idx = cg_schema.get_column_idx(j);
        if (OB_UNLIKELY(column_idx < 0 || column_idx >= bdrs.vectors_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("this is invalid column idx", K(ret), K(column_idx), K(cg_schema));
        } else if (OB_FAIL(cg_rows.vectors_.push_back(bdrs.vectors_.at(column_idx)))) {
          LOG_WARN("push back vector failed", K(ret), K(column_idx));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cg_row_file->append_batch(cg_rows))) {
          LOG_WARN("fail to append batch", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(try_generate_output_chunk(scan_end_))) {
    LOG_WARN("fail to generate output chunk", K(ret), K(scan_end_));
  }
  return ret;
}

int TestCGMicroMacroWriteOp::try_generate_output_chunk(bool scan_end)
{
  int ret = OB_SUCCESS;
  output_cg_row_files_data_->reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < cg_row_file_arr_.count(); ++i) {
    ObCGRowFile *&cg_row_file = cg_row_file_arr_.at(i);
    if (OB_UNLIKELY(nullptr == cg_row_file)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg block file is null", K(ret), K(i));
    } else if (scan_end || cg_row_file->get_mem_hold() >= CG_ROW_FILE_MEMEORY_LIMIT) {
      if (OB_FAIL(output_cg_row_files_data_->push_back(cg_row_file))) {
        LOG_WARN("fail to push back cg block files", K(ret), KPC(cg_row_file));
      } else {
        cg_row_file = nullptr;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObChunk output_chunk;
    if (!output_cg_row_files_data_->empty()) { // has output data
      output_chunk.cg_row_file_arr_ = output_cg_row_files_data_;
      output_chunk.type_ = ObChunk::CG_ROW_TMP_FILES;
      if (OB_FAIL(pipeline_.push(output_chunk))) {
        LOG_WARN("fail to push chunk", K(ret), K(output_chunk));
      }
      output_chunk.reset();
    }
  }
  return ret;
}

TEST_F(TestCGMicroMacroWriteOp, test_direct_load_mem_friend_pipeline)
{
  ObMockIters row_iters;
  ObITabletSliceRowIterator *row_iter = nullptr;
  ObBatchDatumRows batch_rows;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 6;
  const int64_t storage_col_count = col_count + 2;
  const int64_t batch_size = 512;
  const int64_t test_batch_count = 512;
  ObObjType col_types[col_count] = {ObIntType, ObIntType,
                                    ObVarcharType, ObVarcharType,
                                    ObVarcharType, ObVarcharType};
  ObObjType storage_col_types[storage_col_count] = {ObIntType, ObIntType,
                                                    ObIntType, ObIntType,
                                                    ObVarcharType, ObVarcharType,
                                                    ObVarcharType, ObVarcharType};

  tablet_id_ = 2000004;
  ops_type_ = SKIP_TYPE;
  observer::ObTableLoadMemoryFriendWriteMacroBlockPipeline *task_ptr = nullptr;
  EXPECT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));
  EXPECT_EQ(OB_SUCCESS, row_iters.init(5, tablet_id_, 0, batch_size, storage_col_types,
                                       storage_col_count, rowkey_count, true, test_batch_count));
  ddl_dag_.dag_status_ = ObIDag::DAG_STATUS_NODE_RUNNING;
  EXPECT_EQ(OB_SUCCESS, row_iters.get_next_iter(row_iter));
  EXPECT_EQ(OB_SUCCESS, ddl_dag_.alloc_task(task_ptr, row_iter));
  EXPECT_EQ(OB_SUCCESS, ddl_dag_.add_task(*task_ptr));
  EXPECT_EQ(OB_SUCCESS, ddl_dag_.process());
}

TEST_F(TestCGMicroMacroWriteOp, test_cg_micro_and_macro_write_op)
{
  ObBatchRowsGen batch_rows_gen;
  ObBatchDatumRows batch_rows;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 6;
  const int64_t storage_col_count = col_count + 2;
  const int64_t batch_size = 512;
  const int64_t test_batch_count = 512;
  ObObjType col_types[col_count] = {ObIntType, ObIntType,
                                    ObVarcharType, ObVarcharType,
                                    ObVarcharType, ObVarcharType};
  ObObjType storage_col_types[storage_col_count] = {ObIntType, ObIntType,
                                                    ObIntType, ObIntType,
                                                    ObVarcharType, ObVarcharType,
                                                    ObVarcharType, ObVarcharType};

  tablet_id_ = 2000001;
  ops_type_ = WRITE_MICRO_AND_MACRO_OP;
  EXPECT_EQ(OB_SUCCESS, batch_rows_gen.init(storage_col_types, storage_col_count, rowkey_count));
  EXPECT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));
  for (int64_t i = 0; i < test_batch_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, batch_rows_gen.get_batch_rows(batch_rows, batch_size));
    if (i + 1 == test_batch_count) {
      scan_end_ = true;
    }
    EXPECT_EQ(OB_SUCCESS, write_bdrs_into_cg_row_temp_files(batch_rows));
  }
  ObChunk end_chunk;
  end_chunk.set_end_chunk();
  EXPECT_EQ(OB_SUCCESS, pipeline_.push(end_chunk));
}

TEST_F(TestCGMicroMacroWriteOp, test_cg_row_files_generater)
{
  bool is_slice_end = false;
  ObDDLChunk output_chunk;
  ObBatchRowsGen batch_rows_gen;
  ObCGRowFilesGenerater cg_row_files_generater;
  ObBatchDatumRows batch_rows;
  const int64_t cg_row_file_memory_limit = 64L * 1024L;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 6;
  const int64_t storage_col_count = col_count + 2;
  const int64_t batch_size = 512;
  const int64_t test_batch_count = 512;
  ObObjType col_types[col_count] = {ObIntType, ObIntType,
                                    ObVarcharType, ObVarcharType,
                                    ObVarcharType, ObVarcharType};
  ObObjType storage_col_types[storage_col_count] = {ObIntType, ObIntType,
                                                    ObIntType, ObIntType,
                                                    ObVarcharType, ObVarcharType,
                                                    ObVarcharType, ObVarcharType};

  tablet_id_ = 2000002;
  ops_type_ = WRITE_MICRO_AND_MACRO_OP;
  EXPECT_EQ(OB_SUCCESS, batch_rows_gen.init(storage_col_types, storage_col_count, rowkey_count));
  EXPECT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));
  EXPECT_EQ(OB_SUCCESS, cg_row_files_generater.init(tablet_id_,
                                                    slice_idx_,
                                                    storage_schema_,
                                                    batch_size,
                                                    cg_row_file_memory_limit,
                                                    ddl_dag_.ddl_table_schema_.column_items_,
                                                    false/*is_generation_sync_output*/,
                                                    false/*is_sorted_table_load_with_column_store_replica*/));
  for (int64_t i = 0; i < test_batch_count; ++i) {
    EXPECT_EQ(OB_SUCCESS, batch_rows_gen.get_batch_rows(batch_rows, batch_size));
    if (i + 1 == test_batch_count) {
      is_slice_end = true;
    }
    EXPECT_EQ(OB_SUCCESS, cg_row_files_generater.append_batch(batch_rows, is_slice_end, output_chunk));
    if (nullptr != output_chunk.chunk_data_) {
      EXPECT_EQ(OB_SUCCESS, pipeline_.push(*output_chunk.chunk_data_));
    }
  }
  if (output_chunk.is_slice_end_) {
    ObChunk end_chunk;
    end_chunk.set_end_chunk();
    EXPECT_EQ(OB_SUCCESS, pipeline_.push(end_chunk));
    LOG_INFO("slice end in the test_cg_row_files_generater");
  }
}

// TEST_F(TestCGMicroMacroWriteOp, test_ddl_write_macro_block_op)
// {
  // ObBatchRowsGen batch_rows_gen;
  // ObBatchDatumRows batch_rows;
  // ObChunk input_chunk;
  // const int64_t rowkey_count = 2;
  // const int64_t col_count = 6;
  // const int64_t storage_col_count = col_count + 2;
  // const int64_t batch_size = 512;
  // const int64_t test_batch_count = 512;
  // ObObjType col_types[col_count] = {ObIntType, ObIntType,
  //                                   ObVarcharType, ObVarcharType,
  //                                   ObVarcharType, ObVarcharType};
  // ObObjType storage_col_types[storage_col_count] = {ObIntType, ObIntType,
  //                                                   ObIntType, ObIntType,
  //                                                   ObVarcharType, ObVarcharType,
  //                                                   ObVarcharType, ObVarcharType};

  // tablet_id_ = 2000003;
  // ops_type_ = DDL_WRITE_MACRO_BLOCK_OP;
  // EXPECT_EQ(OB_SUCCESS, batch_rows_gen.init(storage_col_types, storage_col_count, rowkey_count));
  // EXPECT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));
  // for (int64_t i = 0; i < test_batch_count; ++i) {
  //   EXPECT_EQ(OB_SUCCESS, batch_rows_gen.get_batch_rows(batch_rows, batch_size));
  //   input_chunk.type_ = ObChunk::BATCH_VECTOR;
  //   input_chunk.batch_vector_ = &batch_rows;
  //   EXPECT_EQ(OB_SUCCESS, pipeline_.push(input_chunk));
  // }
  // ObChunk end_chunk;
  // end_chunk.set_end_chunk();
  // EXPECT_EQ(OB_SUCCESS, pipeline_.push(end_chunk));
// }

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_pipeline_and_op.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_pipeline_and_op.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}