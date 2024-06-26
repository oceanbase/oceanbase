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

#define USING_LOG_PREFIX RS

#include "rootserver/ddl_task/ob_fts_index_build_task.h"
#include "share/ob_ddl_common.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_index_builder.h"
#include "storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{
/***************         ObFtsIndexBuildTask        *************/

ObFtsIndexBuildTask::ObFtsIndexBuildTask()
  : ObDDLTask(ObDDLType::DDL_CREATE_FTS_INDEX),
    index_table_id_(target_object_id_),
    rowkey_doc_aux_table_id_(OB_INVALID_ID),
    doc_rowkey_aux_table_id_(OB_INVALID_ID),
    fts_index_aux_table_id_(OB_INVALID_ID),
    fts_doc_word_aux_table_id_(OB_INVALID_ID),
    rowkey_doc_schema_generated_(false),
    doc_rowkey_schema_generated_(false),
    fts_index_aux_schema_generated_(false),
    fts_doc_word_schema_generated_(false),
    rowkey_doc_task_submitted_(false),
    doc_rowkey_task_submitted_(false),
    fts_index_aux_task_submitted_(false),
    fts_doc_word_task_submitted_(false),
    drop_index_task_id_(0),
    drop_index_task_submitted_(false),
    root_service_(nullptr),
    create_index_arg_(),
    dependent_task_result_map_()
{
}

ObFtsIndexBuildTask::~ObFtsIndexBuildTask()
{
}

int ObFtsIndexBuildTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t schema_version,
    const int64_t parallelism,
    const int64_t consumer_group_id,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const int64_t parent_task_id /* = 0 */,
    const int64_t task_status /* PREPARE */,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_format_version = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root_service is null", K(ret), KP(root_service_));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service", K(ret));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID ||
                         task_id <= 0 ||
                         OB_ISNULL(data_table_schema) ||
                         OB_ISNULL(index_schema) ||
                         schema_version <= 0 ||
                         parallelism <= 0 ||
                         consumer_group_id < 0 ||
                         !create_index_arg.is_valid() ||
                         task_status < ObDDLTaskStatus::PREPARE ||
                         task_status > ObDDLTaskStatus::SUCCESS ||
                         snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id),
        KPC(data_table_schema), KPC(index_schema), K(schema_version), K(parallelism),
        K(consumer_group_id), K(create_index_arg.is_valid()), K(create_index_arg),
        K(task_status), K(snapshot_version));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_,
                                         create_index_arg,
                                         create_index_arg_))) {
    LOG_WARN("fail to copy create index arg", K(ret), K(create_index_arg));
  } else if (OB_FAIL(ObShareUtil::fetch_current_data_version(*GCTX.sql_proxy_,
                                                             tenant_id,
                                                             tenant_data_format_version))) {
    LOG_WARN("get min data version failed", K(ret), K(tenant_id));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = tenant_id;
    task_id_ = task_id;
    schema_version_ = schema_version;
    parallelism_ = parallelism;
    consumer_group_id_ = consumer_group_id;
    parent_task_id_ = parent_task_id;
    if (snapshot_version > 0) {
      snapshot_version_ = snapshot_version;
    }
    object_id_ = data_table_schema->get_table_id();
    target_object_id_ = index_schema->get_table_id();
    index_table_id_ = index_schema->get_table_id();
    create_index_arg_.exec_tenant_id_ = tenant_id;
    fts_index_aux_table_id_ = index_table_id_;
    // fts_index aux schema already generated before ddl task begin
    fts_index_aux_schema_generated_ = true;
    task_version_ = OB_FTS_INDEX_BUILD_TASK_VERSION;
    start_time_ = ObTimeUtility::current_time();
    data_format_version_ = tenant_data_format_version;
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(task_status_ = static_cast<ObDDLTaskStatus>(task_status))) {
    } else if (OB_FAIL(init_ddl_task_monitor_info(index_schema->get_table_id()))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      dst_tenant_id_ = tenant_id_;
      dst_schema_version_ = schema_version_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = task_record.object_id_;
  const uint64_t index_table_id = task_record.target_object_id_;
  const int64_t schema_version = task_record.schema_version_;
  int64_t pos = 0;
  const ObTableSchema *data_schema = nullptr;
  const char *ddl_type_str = nullptr;
  const char *target_name = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root_service is null", K(ret), KP(root_service_));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service", K(ret));
  } else if (!task_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_,
                                                     task_record.message_.ptr(),
                                                     task_record.message_.length(),
                                                     pos))) {
    LOG_WARN("deserialize params from message failed", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().
                     get_tenant_schema_guard(task_record.tenant_id_,
                                             schema_guard,
                                             schema_version))) {
    LOG_WARN("fail to get schema guard", K(ret), K(index_table_id),
        K(schema_version));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(task_record.tenant_id_,
                                                   data_table_id,
                                                   data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(data_table_id));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), K(data_schema));
  } else {
    tenant_id_ = task_record.tenant_id_;
    task_id_ = task_record.task_id_;
    schema_version_ = schema_version;
    parent_task_id_ = task_record.parent_task_id_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    object_id_ = data_table_id;
    target_object_id_ = index_table_id;
    index_table_id_ = index_table_id;
    fts_index_aux_table_id_ = index_table_id_;
    fts_index_aux_schema_generated_ = true;
    execution_id_ = task_record.execution_id_;
    ret_code_ = task_record.ret_code_;
    start_time_ = ObTimeUtility::current_time();
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_ddl_task_monitor_info(index_table_id))) {
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      is_inited_ = true;
      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::process()
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = create_index_arg_.index_type_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check health failed", K(ret));
  } else if (!share::schema::is_fts_index(index_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect index type is of fts index", K(ret), K(index_type));
  } else if (!need_retry()) {
    // by pass
  } else {
    // switch case for diff create_index_arg, since there are 4 aux fts tables
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
    case ObDDLTaskStatus::PREPARE: {
      if (OB_FAIL(prepare())) {
        LOG_WARN("prepare failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_ROWKEY_DOC_SCHEMA: {
      if (OB_FAIL(prepare_rowkey_doc_table())) {
        LOG_WARN("generate schema failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_ROWKEY_DOC_TABLE_COMPLEMENT: {
      if (OB_FAIL(wait_aux_table_complement())) {
        LOG_WARN("wait rowkey_doc table complement failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA: {
      if (OB_FAIL(prepare_aux_index_tables())) {
        LOG_WARN("generate schema failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_AUX_TABLE_COMPLEMENT: {
      if (OB_FAIL(wait_aux_table_complement())) {
        LOG_WARN("wait aux fts table complement failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::VALIDATE_CHECKSUM: {
      if (OB_FAIL(validate_checksum())) {
        LOG_WARN("validate checksum failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::FAIL: {
      if (OB_FAIL(clean_on_failed())) {
        LOG_WARN("clean failed_task failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::SUCCESS: {
      if (OB_FAIL(succ())) {
        LOG_WARN("clean task on finish failed", K(ret), K(*this));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K(status), K(*this));
    }
    } // end switch
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

bool ObFtsIndexBuildTask::is_valid() const
{
  return is_inited_ && !trace_id_.is_invalid();
}

int ObFtsIndexBuildTask::deep_copy_index_arg(
    common::ObIAllocator &allocator,
    const obrpc::ObCreateIndexArg &source_arg,
    obrpc::ObCreateIndexArg &dest_arg)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = source_arg.get_serialize_size();
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(serialize_size));
  } else if (OB_FAIL(source_arg.serialize(buf, serialize_size, pos))) {
    LOG_WARN("serialize alter table arg", K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(dest_arg.deserialize(buf, serialize_size, pos))) {
    LOG_WARN("deserialize alter table arg failed", K(ret));
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    allocator.free(buf);
  }
  return ret;
}

int ObFtsIndexBuildTask::check_health()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service, not need retry", K(ret));
    need_retry_ = false; // only stop run the task, need not clean up task context
  } else if (OB_FAIL(refresh_status())) { // refresh task status
    LOG_WARN("refresh status failed", K(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else {
    ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;
    bool is_data_table_exist = false;
    bool is_all_indexes_exist = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_,
                                                       schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                      object_id_,
                                                      is_data_table_exist))) {
      LOG_WARN("check data table exist failed", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_FAIL(check_aux_table_schemas_exist(is_all_indexes_exist))) {
      LOG_WARN("check aux index table exist failed", K(ret), K(tenant_id_));
    } else if (!is_data_table_exist || !is_all_indexes_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table or index table not exist", K(ret), K(is_data_table_exist),
          K(is_all_indexes_exist));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     index_table_id_,
                                                     index_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(index_table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index schema is null, but index table exist", K(ret),
          K(index_table_id_));
    } else if (ObIndexStatus::INDEX_STATUS_INDEX_ERROR == index_schema->get_index_status()) {
      ret = OB_SUCCESS == ret_code_ ? OB_ERR_ADD_INDEX : ret_code_;
      LOG_WARN("index status error", K(ret), K(index_table_id_),
          K(index_schema->get_index_status()));
    }
    #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = check_errsim_error();
      }
    #endif
    if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      switch_status(new_status, false, ret);
      LOG_WARN("switch status to build_failed", K(ret), K(old_status), K(new_status));
    }
    if (ObDDLTaskStatus::FAIL == static_cast<ObDDLTaskStatus>(task_status_) ||
        ObDDLTaskStatus::SUCCESS == static_cast<ObDDLTaskStatus>(task_status_)) {
      ret = OB_SUCCESS; // allow clean up
    }
  }
  check_ddl_task_execute_too_long();
  return ret;
}

int ObFtsIndexBuildTask::check_aux_table_schemas_exist(bool &is_all_exist)
{
  int ret = OB_SUCCESS;
  is_all_exist = false;
  const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
  ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
  } else {
    bool rowkey_doc_exist = true;
    bool doc_rowkey_exist = true;
    bool fts_index_aux_exist = true;
    bool fts_doc_word_exist = true;
    if (status <= ObDDLTaskStatus::GENERATE_ROWKEY_DOC_SCHEMA) {
      if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                 fts_index_aux_table_id_,
                                                 fts_index_aux_exist))) {
        LOG_WARN("check data table exist failed", K(ret), K(tenant_id_),
            K(fts_index_aux_table_id_));
      } else {
        is_all_exist = fts_index_aux_exist;
      }
    } else if (status <= ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA) {
      if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                 rowkey_doc_aux_table_id_,
                                                 rowkey_doc_exist))) {
        LOG_WARN("check data table exist failed", K(ret), K(tenant_id_),
            K(rowkey_doc_aux_table_id_));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                 fts_index_aux_table_id_,
                                                 fts_index_aux_exist))) {
        LOG_WARN("check data table exist failed", K(ret), K(tenant_id_),
            K(fts_index_aux_table_id_));
      } else {
        is_all_exist = (rowkey_doc_exist && fts_index_aux_exist);
      }
    } else {
      if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                 rowkey_doc_aux_table_id_,
                                                 rowkey_doc_exist))) {
        LOG_WARN("check data table exist failed", K(ret), K(tenant_id_),
            K(rowkey_doc_aux_table_id_));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        doc_rowkey_aux_table_id_,
                                                        doc_rowkey_exist))) {
        LOG_WARN("check data table exist failed", K(ret), K(tenant_id_),
            K(doc_rowkey_aux_table_id_));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        fts_index_aux_table_id_,
                                                        fts_index_aux_exist))) {
        LOG_WARN("check data table exist failed", K(ret), K(tenant_id_),
            K(fts_index_aux_table_id_));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        fts_doc_word_aux_table_id_,
                                                        fts_doc_word_exist))) {
        LOG_WARN("check data table exist failed", K(ret), K(tenant_id_),
            K(fts_doc_word_aux_table_id_));
      } else {
        is_all_exist = (rowkey_doc_exist && doc_rowkey_exist &&
                        fts_index_aux_exist && fts_doc_word_exist);
        if (!is_all_exist) {
          LOG_WARN("fts aux table not exist", K(rowkey_doc_exist),
              K(doc_rowkey_exist), K(fts_index_aux_exist), K(fts_doc_word_exist));
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::get_next_status(share::ObDDLTaskStatus &next_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObIndexType index_type = create_index_arg_.index_type_;
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
      case ObDDLTaskStatus::PREPARE: {
        next_status = ObDDLTaskStatus::GENERATE_ROWKEY_DOC_SCHEMA;
        break;
      }
      case ObDDLTaskStatus::GENERATE_ROWKEY_DOC_SCHEMA: {
        next_status = ObDDLTaskStatus::WAIT_ROWKEY_DOC_TABLE_COMPLEMENT;
        break;
      }
      case ObDDLTaskStatus::WAIT_ROWKEY_DOC_TABLE_COMPLEMENT: {
        next_status = ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA;
        break;
      }
      case ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA: {
        next_status = ObDDLTaskStatus::WAIT_AUX_TABLE_COMPLEMENT;
        break;
      }
      case ObDDLTaskStatus::WAIT_AUX_TABLE_COMPLEMENT: {
        next_status = ObDDLTaskStatus::VALIDATE_CHECKSUM;
        break;
      }
      case ObDDLTaskStatus::VALIDATE_CHECKSUM: {
        next_status = ObDDLTaskStatus::SUCCESS;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not expected status", K(ret), K(status), K(*this));
      }
    } // end switch
  }
  return ret;
}

int ObFtsIndexBuildTask::prepare()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::PREPARE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else {
    state_finished = true;
  }

  if (state_finished) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("prepare finished", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::prepare_rowkey_doc_table()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  const int64_t num_fts_child_task = 4;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::GENERATE_ROWKEY_DOC_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (!dependent_task_result_map_.created() &&
             OB_FAIL(dependent_task_result_map_.create(num_fts_child_task,
                                                       lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else {
    const uint64_t data_table_id = object_id_;
    int64_t ddl_rpc_timeout = 0;
    SMART_VARS_4((obrpc::ObCreateIndexArg, rowkey_doc_arg),
                 (ObDDLTaskRecord, rowkey_doc_task_record),
                 (obrpc::ObGenerateAuxIndexSchemaArg, arg),
                 (obrpc::ObGenerateAuxIndexSchemaRes, res)) {
      ObDDLService &ddl_service = root_service_->get_ddl_service();
      arg.tenant_id_ = tenant_id_;
      arg.exec_tenant_id_ = tenant_id_;
      arg.data_table_id_ = data_table_id;
      arg.task_id_ = task_id_;
      obrpc::ObCommonRpcProxy *common_rpc = nullptr;
      if (OB_FAIL(construct_rowkey_doc_arg(rowkey_doc_arg))) {
        LOG_WARN("failed to construct rowkey doc id arg", K(ret));
      } else if (!rowkey_doc_schema_generated_) {
        if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_,
                                                   data_table_id,
                                                   ddl_rpc_timeout))) {
          LOG_WARN("get ddl rpc timeout fail", K(ret));
        } else if (OB_ISNULL(root_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("root_service is nullptr", K(ret));
        } else if (OB_FAIL(arg.create_index_arg_.assign(rowkey_doc_arg))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to assign create index arg", K(ret));
        } else if (OB_FALSE_IT(common_rpc = root_service_->get_ddl_service().get_common_rpc())) {
        } else if (OB_ISNULL(common_rpc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("common rpc is nullptr", K(ret));
        } else if (OB_FAIL(common_rpc-> to(obrpc::ObRpcProxy::myaddr_).
                           timeout(ddl_rpc_timeout).generate_aux_index_schema(arg,
                                                                              res))) {
          LOG_WARN("generate fts aux index schema failed", K(ret), K(arg));
        } else if (res.schema_generated_) {
          rowkey_doc_schema_generated_ = true;
          rowkey_doc_aux_table_id_ = res.aux_table_id_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!rowkey_doc_schema_generated_ ) {
      } else if (rowkey_doc_task_submitted_) {
      } else if (OB_FAIL(submit_build_aux_index_task(rowkey_doc_arg,
                                                     rowkey_doc_task_record,
                                                     rowkey_doc_task_submitted_))) {
        LOG_WARN("fail to submit build rowkey doc id index task", K(ret));
      } else {
        state_finished = true;
      }
    }
  }
  if (state_finished) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("generate schema finished", K(ret), K(parent_task_id_), K(task_id_),
          K(*this));
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::prepare_aux_index_tables()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  const int64_t num_fts_child_task = 4;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (!dependent_task_result_map_.created() &&
             OB_FAIL(dependent_task_result_map_.create(num_fts_child_task,
                                                       lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else {
    const uint64_t data_table_id = object_id_;
    int64_t ddl_rpc_timeout = 0;
    SMART_VARS_3((obrpc::ObCreateIndexArg, doc_rowkey_arg),
                 (obrpc::ObCreateIndexArg, fts_index_aux_arg),
                 (obrpc::ObCreateIndexArg, fts_doc_word_arg)) {
    SMART_VARS_3((ObDDLTaskRecord, doc_rowkey_task_record),
                 (ObDDLTaskRecord, fts_index_aux_task_record),
                 (ObDDLTaskRecord, fts_doc_word_task_record)) {
    SMART_VARS_4((obrpc::ObGenerateAuxIndexSchemaArg, doc_rowkey_schema_arg),
                 (obrpc::ObGenerateAuxIndexSchemaRes, doc_rowkey_schema_res),
                 (obrpc::ObGenerateAuxIndexSchemaArg, fts_doc_word_schema_arg),
                 (obrpc::ObGenerateAuxIndexSchemaRes, fts_doc_word_schema_res)) {
      ObDDLService &ddl_service = root_service_->get_ddl_service();
      doc_rowkey_schema_arg.tenant_id_ = tenant_id_;
      doc_rowkey_schema_arg.data_table_id_ = data_table_id;
      doc_rowkey_schema_arg.exec_tenant_id_ = tenant_id_;
      doc_rowkey_schema_arg.task_id_ = task_id_;
      fts_doc_word_schema_arg.tenant_id_ = tenant_id_;
      fts_doc_word_schema_arg.data_table_id_ = data_table_id;
      fts_doc_word_schema_arg.exec_tenant_id_ = tenant_id_;
      fts_doc_word_schema_arg.task_id_ = task_id_;
      obrpc::ObCommonRpcProxy *common_rpc = nullptr;
      if (OB_ISNULL(root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("root_service is nullptr", K(ret));
      } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_,
                                                        data_table_id,
                                                        ddl_rpc_timeout))) {
        LOG_WARN("get ddl rpc timeout fail", K(ret));
      } else if (OB_FAIL(construct_doc_rowkey_arg(doc_rowkey_arg))) {
        LOG_WARN("fail to construct doc id rowkey arg", K(ret));
      } else if (!doc_rowkey_schema_generated_) {
        if (OB_FAIL(doc_rowkey_schema_arg.create_index_arg_.assign(doc_rowkey_arg))) {
          LOG_WARN("fail to assign create index arg", K(ret));
        } else if (OB_FALSE_IT(common_rpc = root_service_->get_ddl_service().get_common_rpc())) {
        } else if (OB_ISNULL(common_rpc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("common rpc is nullptr", K(ret));
        } else if (OB_FAIL(common_rpc->to(obrpc::ObRpcProxy::myaddr_).
                           timeout(ddl_rpc_timeout).
                           generate_aux_index_schema(doc_rowkey_schema_arg,
                                                     doc_rowkey_schema_res))) {
          LOG_WARN("generate fts doc rowkey schema failed", K(ret), K(doc_rowkey_schema_arg));
        } else if (doc_rowkey_schema_res.schema_generated_) {
          doc_rowkey_schema_generated_ = true;
          doc_rowkey_aux_table_id_ = doc_rowkey_schema_res.aux_table_id_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!doc_rowkey_schema_generated_) {
      } else if (doc_rowkey_task_submitted_) {
      } else if (OB_FAIL(submit_build_aux_index_task(doc_rowkey_arg,
                                                     doc_rowkey_task_record,
                                                     doc_rowkey_task_submitted_))) {
        LOG_WARN("fail to submit build doc id rowkey index task", K(ret));
      }
      // NOTE unlike other 3 aux index schemas which require rpc to generate schema,
      //      fts index schema is generated before this ddl task start
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(construct_fts_index_aux_arg(fts_index_aux_arg))) {
        LOG_WARN("fail to construct fts index aux arg", K(ret));
      } else if (fts_index_aux_task_submitted_) {
      } else if (OB_FAIL(submit_build_aux_index_task(fts_index_aux_arg,
                                                     fts_index_aux_task_record,
                                                     fts_index_aux_task_submitted_))) {
        LOG_WARN("fail to submit build fts index aux task", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(construct_fts_doc_word_arg(fts_doc_word_arg))) {
        LOG_WARN("fail to construct fts doc word arg", K(ret));
      } else if (!fts_doc_word_schema_generated_) {
        if (OB_FAIL(fts_doc_word_schema_arg.create_index_arg_.assign(fts_doc_word_arg))) {
          LOG_WARN("fail to assign create index arg", K(ret));
        } else if (OB_FAIL(common_rpc-> to(obrpc::ObRpcProxy::myaddr_).
                           timeout(ddl_rpc_timeout).
                           generate_aux_index_schema(fts_doc_word_schema_arg,
                                                     fts_doc_word_schema_res))) {
          LOG_WARN("generate fts doc word schema failed", K(ret), K(fts_doc_word_schema_arg));
        } else if (fts_doc_word_schema_res.schema_generated_) {
          fts_doc_word_schema_generated_ = true;
          fts_doc_word_aux_table_id_ = fts_doc_word_schema_res.aux_table_id_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!fts_doc_word_schema_generated_) {
      } else if (fts_doc_word_task_submitted_) {
      } else if (OB_FAIL(submit_build_aux_index_task(fts_doc_word_arg,
                                                     fts_doc_word_task_record,
                                                     fts_doc_word_task_submitted_))) {
        LOG_WARN("fail to submit build fts doc word index task", K(ret));
      }
      if (doc_rowkey_task_submitted_ && fts_index_aux_task_submitted_ &&
          fts_doc_word_task_submitted_) {
        state_finished = true;
      }
    }
  } // SMART_VARS
  } // SMART_VARS
  } // SMART_VARS
  if (state_finished) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("generate schema finished", K(ret), K(parent_task_id_), K(task_id_),
          K(*this));
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::construct_rowkey_doc_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_ROWKEY_DOC_ID_LOCAL)) {
  } else if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(arg,
                                                                        &allocator_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObFtsIndexBuildTask::construct_doc_rowkey_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_DOC_ID_ROWKEY_LOCAL)) {
  } else if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(arg,
                                                                        &allocator_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObFtsIndexBuildTask::construct_fts_index_aux_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_FTS_INDEX_LOCAL)) {
  } else if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(arg,
                                                                        &allocator_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObFtsIndexBuildTask::construct_fts_doc_word_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_FTS_DOC_WORD_LOCAL)) {
  } else if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(arg,
                                                                        &allocator_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObFtsIndexBuildTask::record_index_table_id(
    const obrpc::ObCreateIndexArg *create_index_arg,
    uint64_t &aux_table_id)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = create_index_arg->index_type_;
  if (share::schema::is_rowkey_doc_aux(index_type)) {
    rowkey_doc_aux_table_id_ = aux_table_id;
  } else if (share::schema::is_doc_rowkey_aux(index_type)) {
    doc_rowkey_aux_table_id_ = aux_table_id;
  } else if (share::schema::is_fts_index_aux(index_type)) {
    fts_index_aux_table_id_ = aux_table_id;
  } else if (share::schema::is_fts_doc_word_aux(index_type)) {
    fts_doc_word_aux_table_id_ = aux_table_id;
  }
  return ret;
}

int ObFtsIndexBuildTask::get_index_table_id(
    const obrpc::ObCreateIndexArg *create_index_arg,
    uint64_t &index_table_id)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = create_index_arg->index_type_;
  if (share::schema::is_rowkey_doc_aux(index_type)) {
    index_table_id = rowkey_doc_aux_table_id_;
  } else if (share::schema::is_doc_rowkey_aux(index_type)) {
    index_table_id = doc_rowkey_aux_table_id_;
  } else if (share::schema::is_fts_index_aux(index_type)) {
    index_table_id = fts_index_aux_table_id_;
  } else if (share::schema::is_fts_doc_word_aux(index_type)) {
    index_table_id = fts_doc_word_aux_table_id_;
  }
  return ret;
}

// wait data complement of aux index tables
int ObFtsIndexBuildTask::wait_aux_table_complement()
{
  using task_iter = common::hash::ObHashMap<uint64_t, DependTaskStatus>::const_iterator;
  int ret = OB_SUCCESS;
  bool child_task_failed = false;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::WAIT_ROWKEY_DOC_TABLE_COMPLEMENT != task_status_ &&
             ObDDLTaskStatus::WAIT_AUX_TABLE_COMPLEMENT != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else {
    int64_t finished_task_cnt = 0;
    for (task_iter iter = dependent_task_result_map_.begin();
        OB_SUCC(ret) && iter != dependent_task_result_map_.end(); ++iter) {
      const uint64_t task_key = iter->first;
      const int64_t target_object_id = -1;
      const int64_t child_task_id = iter->second.task_id_;
      if (iter->second.ret_code_ == INT64_MAX) {
        // maybe ddl already finish when switching rs
        HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
          int64_t unused_user_msg_len = 0;
          ObAddr unused_addr;
          if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                      dst_tenant_id_,
                                                      child_task_id,
                                                      target_object_id,
                                                      unused_addr,
                                                      false /* is_ddl_retry_task */,
                                                      *GCTX.sql_proxy_,
                                                      error_message,
                                                      unused_user_msg_len))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("ddl task not finish", K(dst_tenant_id_), K(task_key),
                  K(child_task_id), K(target_object_id));
            } else {
              LOG_WARN("fail to get ddl error message", K(ret), K(task_key),
                  K(child_task_id), K(target_object_id));
            }
          } else {
            finished_task_cnt++;
            if (error_message.ret_code_ != OB_SUCCESS) {
              ret = error_message.ret_code_;
              child_task_failed = true;
              state_finished = true;
              break;
            }
          }
        }
      } else {
        finished_task_cnt++;
        if (iter->second.ret_code_ != OB_SUCCESS) {
          ret = iter->second.ret_code_;
        }
      }
    }
    if (finished_task_cnt == dependent_task_result_map_.size() || OB_FAIL(ret)) {
      // 1. all child tasks finish.
      // 2. the parent task exits if any child task fails.
      state_finished = true;
    }
  }

  if (state_finished) {
    ObDDLTaskStatus next_status;
    // 1. get next_status
    if (child_task_failed) {
      next_status = ObDDLTaskStatus::FAIL;
    } else {
      if (OB_FAIL(get_next_status(next_status))) {
        LOG_WARN("failed to get next status", K(ret));
      }
    }
    // 2. switch to next_status
    if (OB_FAIL(ret)) {
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("wait aux table complement finished", K(ret), K(parent_task_id_),
          K(task_id_), K(*this));
    }
  }
  return ret;
}

// submit child task of build aux index table
int ObFtsIndexBuildTask::submit_build_aux_index_task(
    const obrpc::ObCreateIndexArg &create_index_arg,
    ObDDLTaskRecord &task_record,
    bool &task_submitted)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  uint64_t index_table_id = 0;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().
                                           get_tenant_schema_guard(tenant_id_,
                                                                   schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                   object_id_,
                                                   data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(object_id_));
  } else if (OB_FAIL(get_index_table_id(&create_index_arg, index_table_id))) {
    LOG_WARN("fail to get index table id", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                   index_table_id,
                                                   index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
  } else if (OB_ISNULL(data_schema) || OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret), K(data_schema), K(index_schema),
        K(object_id_), K(index_table_id));
  } else {
    ObCreateDDLTaskParam param(tenant_id_,
                               ObDDLType::DDL_CREATE_INDEX,
                               data_schema,
                               index_schema,
                               0/*object_id*/,
                               index_schema->get_schema_version(),
                               parallelism_,
                               consumer_group_id_,
                               &allocator_,
                               &create_index_arg,
                               task_id_);
    param.tenant_data_version_ = data_format_version_;
    if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().
                create_ddl_task(param, *GCTX.sql_proxy_, task_record))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("submit create index ddl task failed", K(ret));
      }
    } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().
                       schedule_ddl_task(task_record))) {
      LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
    } else {
      TCWLockGuard guard(lock_);
      DependTaskStatus status;
      // check if child task is already added
      if (OB_FAIL(dependent_task_result_map_.get_refactored(index_table_id,
                                                            status))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          status.task_id_ = task_record.task_id_;
          if (OB_FAIL(dependent_task_result_map_.set_refactored(index_table_id,
                                                                status))) {
            LOG_WARN("set dependent task map failed", K(ret), K(index_table_id));
          }
        } else {
          LOG_WARN("get from dependent task map failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        task_submitted = true;
        LOG_INFO("add build fts index task", K(ret), K(index_table_id),
            K(create_index_arg.index_name_), K(index_schema->get_index_type()),
            K(status), K(index_schema->get_schema_version()),
            K(data_schema->get_schema_version()), K(param.schema_version_));
      }
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::on_child_task_finish(
    const uint64_t child_task_key,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFtsIndexBuildTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == child_task_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(child_task_key));
  } else {
    TCWLockGuard guard(lock_);
    int64_t org_ret = INT64_MAX;
    DependTaskStatus status;
    if (OB_FAIL(dependent_task_result_map_.get_refactored(child_task_key,
                                                          status))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      LOG_WARN("get from dependent_task_result_map failed", K(ret),
          K(child_task_key));
    } else if (org_ret != INT64_MAX && org_ret != ret_code) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, ddl result triggers twice", K(ret),
          K(child_task_key));
    } else if (FALSE_IT(status.ret_code_ = ret_code)) {
    } else if (OB_FAIL(dependent_task_result_map_.set_refactored(child_task_key,
                                                                 status,
                                                                 true/*overwrite*/))) {
      LOG_WARN("set dependent_task_result_map failed", K(ret), K(child_task_key));
    } else {
      LOG_INFO("child task finish successfully", K(child_task_key));
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::update_index_status_in_schema(
    const ObTableSchema &index_schema,
    const ObIndexStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    obrpc::ObUpdateIndexStatusArg arg;
    arg.index_table_id_ = index_schema.get_table_id();
    arg.status_ = new_status;
    arg.exec_tenant_id_ = tenant_id_;
    arg.in_offline_ddl_white_list_ = true;
    arg.task_id_ = task_id_;
    int64_t ddl_rpc_timeout = 0;
    int64_t tmp_timeout = 0;
    if (INDEX_STATUS_AVAILABLE == new_status) {
      const bool is_create_index_syntax = create_index_arg_.ddl_stmt_str_.trim().prefix_match_ci("create");
      if (create_index_arg_.ddl_stmt_str_.empty()) {
        // alter table syntax.
      } else if (OB_UNLIKELY(!is_create_index_syntax)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), "ddl_stmt_str",
            create_index_arg_.ddl_stmt_str_, K(create_index_arg_));
      } else {
        // For create index syntax, create_index_arg_ will record the user sql,
        // and generate the ddl_stmt_str when anabling index.
        // For alter table add index syntax, create_index_arg_ will not record
        // the user sql, and generate the ddl_stmt_str when generating index schema.
        arg.ddl_stmt_str_ = create_index_arg_.ddl_stmt_str_;
      }
    }

    DEBUG_SYNC(BEFORE_UPDATE_GLOBAL_INDEX_STATUS);
    obrpc::ObCommonRpcProxy *common_rpc = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(index_schema.get_all_part_num(),
                                                      ddl_rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout fail", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_,
                                                      index_schema.get_data_table_id(),
                                                      tmp_timeout))) {
      LOG_WARN("get ddl rpc timeout fail", K(ret));
    } else if (OB_FALSE_IT(ddl_rpc_timeout += tmp_timeout)) {
    } else if (OB_FALSE_IT(common_rpc = root_service_->get_ddl_service().get_common_rpc())) {
    } else if (OB_ISNULL(common_rpc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc is nullptr", K(ret));
    } else if (OB_FAIL(common_rpc->to(GCTX.self_addr()).timeout(ddl_rpc_timeout).
                       update_index_status(arg))) {
      LOG_WARN("update index status failed", K(ret), K(arg));
    } else {
      LOG_INFO("notify index status changed finish", K(new_status),
          K(index_table_id_), K(ddl_rpc_timeout), "ddl_stmt_str", arg.ddl_stmt_str_);
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::serialize_params_to_message(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int8_t rowkey_doc_generated = static_cast<int8_t>(rowkey_doc_schema_generated_);
  int8_t doc_rowkey_generated = static_cast<int8_t>(doc_rowkey_schema_generated_);
  int8_t fts_index_aux_generated = static_cast<int8_t>(fts_index_aux_schema_generated_);
  int8_t fts_doc_word_generated = static_cast<int8_t>(fts_doc_word_schema_generated_);
  int8_t rowkey_doc_submitted = static_cast<int8_t>(rowkey_doc_task_submitted_);
  int8_t doc_rowkey_submitted = static_cast<int8_t>(doc_rowkey_task_submitted_);
  int8_t fts_index_aux_submitted = static_cast<int8_t>(fts_index_aux_task_submitted_);
  int8_t fts_doc_word_submitted = static_cast<int8_t>(fts_doc_word_task_submitted_);
  int8_t drop_index_submitted = static_cast<int8_t>(drop_index_task_submitted_);
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("ObDDLTask serialize failed", K(ret));
  } else if (OB_FAIL(create_index_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize create index arg failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           rowkey_doc_aux_table_id_))) {
    LOG_WARN("serialize rowkey doc table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           doc_rowkey_aux_table_id_))) {
    LOG_WARN("serialize doc rowkey table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           fts_index_aux_table_id_))) {
    LOG_WARN("serialize fts index table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           fts_doc_word_aux_table_id_))) {
    LOG_WARN("serialize fts doc word table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              rowkey_doc_generated))) {
    LOG_WARN("serialize rowkey doc schema generated failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              doc_rowkey_generated))) {
    LOG_WARN("serialize doc rowkey schema generated failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              fts_index_aux_generated))) {
    LOG_WARN("serialize fts index aux schema generated failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              fts_doc_word_generated))) {
    LOG_WARN("serialize fts doc word schema generated failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              rowkey_doc_submitted))) {
    LOG_WARN("serialize rowkey doc task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              doc_rowkey_submitted))) {
    LOG_WARN("serialize doc rowkey task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              fts_index_aux_submitted))) {
    LOG_WARN("serialize fts index aux task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              fts_doc_word_submitted))) {
    LOG_WARN("serialize fts doc word task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              drop_index_submitted))) {
    LOG_WARN("serialize drop fts index task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               drop_index_task_id_))) {
    LOG_WARN("serialize drop index task id failed", K(ret));
  }
  return ret;
}

int ObFtsIndexBuildTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t rowkey_doc_generated = 0;
  int8_t doc_rowkey_generated = 0;
  int8_t fts_index_aux_generated = 0;
  int8_t fts_doc_word_generated = 0;
  int8_t rowkey_doc_submitted = 0;
  int8_t doc_rowkey_submitted = 0;
  int8_t fts_index_aux_submitted = 0;
  int8_t fts_doc_word_submitted = 0;
  int8_t drop_index_submitted = 0;
  obrpc::ObCreateIndexArg tmp_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) ||
                  nullptr == buf ||
                  data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id,
                                                                buf,
                                                                data_len,
                                                                pos))) {
    LOG_WARN("ObDDLTask deserlize failed", K(ret));
  } else if (OB_FAIL(tmp_arg.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize table failed", K(ret));
  } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_arg))) {
    LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, tmp_arg, create_index_arg_))) {
    LOG_WARN("deep copy create index arg failed", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len, pos, rowkey_doc_aux_table_id_))) {
    LOG_WARN("fail to deserialize rowkey doc table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           doc_rowkey_aux_table_id_))) {
    LOG_WARN("fail to deserialize doc rowkey table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           fts_index_aux_table_id_))) {
    LOG_WARN("fail to deserialize fts index aux table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           fts_doc_word_aux_table_id_))) {
    LOG_WARN("fail to deserialize fts doc word table id", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &rowkey_doc_generated))) {
    LOG_WARN("fail to deserialize rowkey doc schema generated", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &doc_rowkey_generated))) {
    LOG_WARN("fail to deserialize doc rowkey schema generated", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &fts_index_aux_generated))) {
    LOG_WARN("fail to deserialize fts index aux schema generated", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &fts_doc_word_generated))) {
    LOG_WARN("fail to deserialize fts doc word schema generated", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &rowkey_doc_submitted))) {
    LOG_WARN("fail to deserialize rowkey doc task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &doc_rowkey_submitted))) {
    LOG_WARN("fail to deserialize doc rowkey task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &fts_index_aux_submitted))) {
    LOG_WARN("fail to deserialize fts index aux task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &fts_doc_word_submitted))) {
    LOG_WARN("fail to deserialize fts doc word task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &drop_index_submitted))) {
    LOG_WARN("fail to deserialize drop fts index task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &drop_index_task_id_))) {
    LOG_WARN("fail to deserialize drop fts index task id", K(ret));
  } else {
    rowkey_doc_schema_generated_ = rowkey_doc_generated;
    doc_rowkey_schema_generated_ = doc_rowkey_generated;
    fts_index_aux_schema_generated_ = fts_index_aux_generated;
    fts_doc_word_schema_generated_ = fts_doc_word_generated;
    rowkey_doc_task_submitted_ = rowkey_doc_submitted;
    doc_rowkey_task_submitted_ = doc_rowkey_submitted;
    fts_index_aux_task_submitted_ = fts_index_aux_submitted;
    fts_doc_word_task_submitted_ = fts_doc_word_submitted;
    drop_index_task_submitted_ = drop_index_submitted;
  }
  return ret;
}

int64_t ObFtsIndexBuildTask::get_serialize_param_size() const
{
  int8_t rowkey_doc_generated = static_cast<int8_t>(rowkey_doc_schema_generated_);
  int8_t doc_rowkey_generated = static_cast<int8_t>(doc_rowkey_schema_generated_);
  int8_t fts_index_aux_generated = static_cast<int8_t>(fts_index_aux_schema_generated_);
  int8_t fts_doc_word_generated = static_cast<int8_t>(fts_doc_word_schema_generated_);
  int8_t rowkey_doc_submitted = static_cast<int8_t>(rowkey_doc_task_submitted_);
  int8_t doc_rowkey_submitted = static_cast<int8_t>(doc_rowkey_task_submitted_);
  int8_t fts_index_aux_submitted = static_cast<int8_t>(fts_index_aux_task_submitted_);
  int8_t fts_doc_word_submitted = static_cast<int8_t>(fts_doc_word_task_submitted_);
  int8_t drop_index_submitted = static_cast<int8_t>(drop_index_task_submitted_);
  return create_index_arg_.get_serialize_size()
      + ObDDLTask::get_serialize_param_size()
      + serialization::encoded_length(rowkey_doc_aux_table_id_)
      + serialization::encoded_length(doc_rowkey_aux_table_id_)
      + serialization::encoded_length(fts_index_aux_table_id_)
      + serialization::encoded_length(fts_doc_word_aux_table_id_)
      + serialization::encoded_length_i8(rowkey_doc_generated)
      + serialization::encoded_length_i8(doc_rowkey_generated)
      + serialization::encoded_length_i8(fts_index_aux_generated)
      + serialization::encoded_length_i8(fts_doc_word_generated)
      + serialization::encoded_length_i8(rowkey_doc_submitted)
      + serialization::encoded_length_i8(doc_rowkey_submitted)
      + serialization::encoded_length_i8(fts_index_aux_submitted)
      + serialization::encoded_length_i8(fts_doc_word_submitted)
      + serialization::encoded_length_i8(drop_index_submitted)
      + serialization::encoded_length_i64(drop_index_task_id_);
}

int ObFtsIndexBuildTask::clean_on_failed()
{
  using task_iter = common::hash::ObHashMap<uint64_t, DependTaskStatus>::const_iterator;
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::FAIL != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else {
    // 1. cancel ongoing build index task
    for (task_iter iter = dependent_task_result_map_.begin();
        OB_SUCC(ret) && iter != dependent_task_result_map_.end(); ++iter) {
      const uint64_t task_key = iter->first;
      const int64_t target_object_id = -1;
      const int64_t child_task_id = iter->second.task_id_;
      if (iter->second.ret_code_ == INT64_MAX) {
        // maybe ddl already finish when switching rs
        HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
          int64_t unused_user_msg_len = 0;
          ObAddr unused_addr;
          if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                      dst_tenant_id_,
                                                      child_task_id,
                                                      target_object_id,
                                                      unused_addr,
                                                      false /* is_ddl_retry_task */,
                                                      *GCTX.sql_proxy_,
                                                      error_message,
                                                      unused_user_msg_len))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              // ongoing child task
              ret = OB_SUCCESS;
              ObMySQLTransaction trans;
              if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(),
                                      dst_tenant_id_))) {
                LOG_WARN("start transaction failed", K(ret));
              } else if (OB_FAIL(ObDDLTaskRecordOperator::update_task_status(
                                 trans, dst_tenant_id_, child_task_id, ObDDLTaskStatus::FAIL))) {
                LOG_WARN("update child task status failed", K(ret), K(child_task_id));
              } else {
                int tmp_ret = trans.end(true/*commit*/);
                if (OB_SUCCESS != tmp_ret) {
                  ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
                }
                LOG_INFO("cancel not finished ddl task", K(dst_tenant_id_),
                    K(task_key), K(child_task_id), K(target_object_id));
              }
            } else {
              LOG_WARN("fail to get ddl error message", K(ret), K(task_key),
                  K(child_task_id), K(target_object_id));
            }
          }
        }
      }
    }
    // 2. drop already built index
    if (OB_FAIL(ret)) {
    } else if (!drop_index_task_submitted_) {
      if (OB_FAIL(submit_drop_fts_index_task())) {
        LOG_WARN("failed to drop fts index", K(ret));
      }
    } else {
      bool drop_index_finished = false;
      if (OB_FAIL(wait_drop_index_finish(drop_index_finished))) {
        LOG_WARN("failed to wait drop index task finish", K(ret));
      } else if (drop_index_finished) {
        state_finished = true;
      }
    }
  }
  // judge index status to choose clean_on_failed() and drop index
  if (OB_SUCC(ret) && state_finished) {
    if (OB_FAIL(cleanup())) {
      LOG_WARN("cleanup failed", K(ret));
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::submit_drop_fts_index_task()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *fts_index_aux_schema = nullptr;
  const ObTableSchema *rowkey_doc_schema = nullptr;
  const ObTableSchema *doc_rowkey_schema = nullptr;
  const ObTableSchema *fts_doc_word_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().
                                           get_tenant_schema_guard(tenant_id_,
                                                                   schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret));
  } else {
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                              fts_index_aux_table_id_,
                                              fts_index_aux_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get table schema", K(ret), K(fts_index_aux_table_id_));
      }
    } else if (OB_INVALID_ID != rowkey_doc_aux_table_id_ &&
               OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     rowkey_doc_aux_table_id_,
                                                     rowkey_doc_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get table schema", K(ret), K(rowkey_doc_aux_table_id_));
      }
    } else if (OB_INVALID_ID != doc_rowkey_aux_table_id_ &&
               OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     doc_rowkey_aux_table_id_,
                                                     doc_rowkey_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get table schema", K(ret), K(doc_rowkey_aux_table_id_));
      }
    } else if (OB_INVALID_ID != fts_doc_word_aux_table_id_ &&
               OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     fts_doc_word_aux_table_id_,
                                                     fts_doc_word_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get table schema", K(ret), K(fts_doc_word_aux_table_id_));
      }
    }
    ObDDLTaskRecord task_record;
    ObCreateDDLTaskParam param(tenant_id_,
                               ObDDLType::DDL_DROP_FTS_INDEX,
                               fts_index_aux_schema,
                               nullptr,
                               0/*object_id*/,
                               fts_index_aux_schema->get_schema_version(),
                               parallelism_,
                               consumer_group_id_,
                               &allocator_);
    param.tenant_data_version_ = data_format_version_;
    param.aux_rowkey_doc_schema_ = rowkey_doc_schema;
    param.aux_doc_rowkey_schema_ = doc_rowkey_schema;
    param.aux_doc_word_schema_ = fts_doc_word_schema;
    if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().
                create_ddl_task(param, *GCTX.sql_proxy_, task_record))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("submit drop fts index ddl task failed", K(ret));
      }
    } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().
                       schedule_ddl_task(task_record))) {
      LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
    } else {
      if (OB_SUCC(ret)) {
        drop_index_task_submitted_ = true;
        drop_index_task_id_ = task_record.task_id_;
        LOG_INFO("add drop fts index task", K(ret), K(fts_index_aux_table_id_),
            K(rowkey_doc_aux_table_id_), K(doc_rowkey_aux_table_id_),
            K(fts_doc_word_aux_table_id_), K(create_index_arg_.index_name_),
            K(fts_index_aux_schema->get_schema_version()), K(param.schema_version_));
      }
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::wait_drop_index_finish(bool &is_finish)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::FAIL != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_INVALID_ID == drop_index_task_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop index task id is invalid", K(ret));
  } else {
    HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
      const int64_t target_object_id = -1;
      int64_t unused_user_msg_len = 0;
      ObAddr unused_addr;
      if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                  dst_tenant_id_,
                                                  drop_index_task_id_,
                                                  target_object_id,
                                                  unused_addr,
                                                  false /* is_ddl_retry_task */,
                                                  *GCTX.sql_proxy_,
                                                  error_message,
                                                  unused_user_msg_len))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("ddl task not finish", K(dst_tenant_id_), K(drop_index_task_id_));
        } else {
          LOG_WARN("fail to get ddl error message", K(ret), K(drop_index_task_id_));
        }
      } else {
        if (error_message.ret_code_ != OB_SUCCESS) {
          ret = error_message.ret_code_;
          drop_index_task_submitted_ = false; // retry
        } else {
          is_finish = true;
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::succ()
{
  return cleanup();
}

int ObFtsIndexBuildTask::validate_checksum()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::VALIDATE_CHECKSUM != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else {
    // TODO hanxuan validate checksum, set next status to FAIL if validation failed
    if (OB_SUCC(ret)) {
      state_finished = true;
    }
  }
  if (state_finished) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("validate checksum finished", K(ret), K(parent_task_id_),
          K(task_id_), K(*this));
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::collect_longops_stat(ObLongopsValue &value)
{
  return OB_SUCCESS;
}

int ObFtsIndexBuildTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else {
    const uint64_t data_table_id = object_id_;
    const uint64_t index_table_id = fts_index_aux_table_id_;
    ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_schema = nullptr;
    const ObTableSchema *index_schema = nullptr;
    int64_t refreshed_schema_version = 0;
    ObTableLockOwnerID owner_id;
    ObMySQLTransaction trans;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_,
                                                       schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     data_table_id,
                                                     data_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(data_table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     index_table_id,
                                                     index_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
    } else if (OB_ISNULL(data_schema) || OB_ISNULL(index_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KPC(data_schema), KPC(index_schema));
    } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), dst_tenant_id_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                   task_id_))) {
      LOG_WARN("failed to get owner id", K(ret), K(task_id_));
    } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(*data_schema,
                                                            *index_schema,
                                                            owner_id,
                                                            trans))) {
      LOG_WARN("failed to unlock online ddl lock", K(ret));
    }
    if (trans.is_started()) {
      int tmp_ret = trans.end(true/*commit*/);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }

  DEBUG_SYNC(CREATE_INDEX_SUCCESS);

  if(OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(),
                                                            tenant_id_,
                                                            task_id_))) {
    LOG_WARN("delete task record failed", K(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }

  if (OB_SUCC(ret) && parent_task_id_ > 0) {
    const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
    root_service_->get_ddl_task_scheduler().on_ddl_task_finish(parent_task_id,
                                                               get_task_key(),
                                                               ret_code_, trace_id_);
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}

void ObFtsIndexBuildTask::flt_set_task_span_tag() const
{
}

void ObFtsIndexBuildTask::flt_set_status_span_tag() const
{
}

} // end namespace rootserver
} // end namespace oceanbase
