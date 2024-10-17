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
    domain_index_aux_table_id_(OB_INVALID_ID),
    fts_doc_word_aux_table_id_(OB_INVALID_ID),
    rowkey_doc_task_submitted_(false),
    doc_rowkey_task_submitted_(false),
    domain_index_aux_task_submitted_(false),
    fts_doc_word_task_submitted_(false),
    rowkey_doc_task_id_(0),
    doc_rowkey_task_id_(0),
    domain_index_aux_task_id_(0),
    fts_doc_word_task_id_(0),
    drop_index_task_id_(-1),
    drop_index_task_submitted_(false),
    root_service_(nullptr),
    is_rowkey_doc_succ_(false),
    is_doc_rowkey_succ_(false),
    is_domain_aux_succ_(false),
    is_fts_doc_word_succ_(false),
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
    const uint64_t tenant_data_version,
    const int64_t parent_task_id /* = 0 */,
    const int64_t task_status /* PREPARE */,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
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
  } else {
    if (index_schema && index_schema->is_multivalue_index_aux()) {
      task_type_ = DDL_CREATE_MULTIVALUE_INDEX;
    }
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
    if (index_schema->is_rowkey_doc_id()) {
      rowkey_doc_aux_table_id_ = index_table_id_;
    } else if (index_schema->is_fts_index_aux()) {
      domain_index_aux_table_id_ = index_table_id_;
    } else if (index_schema->is_multivalue_index_aux()) {
      domain_index_aux_table_id_ = index_table_id_;
    }
    task_version_ = OB_FTS_INDEX_BUILD_TASK_VERSION;
    start_time_ = ObTimeUtility::current_time();
    data_format_version_ = tenant_data_version;
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
  const char *ddl_type_str = nullptr;
  const char *target_name = nullptr;
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
  } else {
    task_type_ = task_record.ddl_type_;
    tenant_id_ = task_record.tenant_id_;
    task_id_ = task_record.task_id_;
    schema_version_ = schema_version;
    parent_task_id_ = task_record.parent_task_id_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    object_id_ = data_table_id;
    target_object_id_ = index_table_id;
    index_table_id_ = index_table_id;
    execution_id_ = task_record.execution_id_;
    ret_code_ = task_record.ret_code_;
    start_time_ = ObTimeUtility::current_time();
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_ddl_task_monitor_info(index_table_id))) {
      LOG_WARN("init ddl task monitor info failed", K(ret), K(index_table_id));
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
  } else if (!is_fts_index(index_type) && !is_multivalue_index(index_type)) {
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
  const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
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
  } else if (status == ObDDLTaskStatus::FAIL) {
    /*already failed, and have submitted drop index task, do nothing*/
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
    } else if (status != ObDDLTaskStatus::FAIL && (!is_data_table_exist || !is_all_indexes_exist)) {
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
      LOG_WARN("index status error", K(ret), K(index_table_id_), K(index_schema->get_table_name_str()),
          K(index_schema->get_index_status()));
    }
    #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = check_errsim_error();
      }
    #endif

    if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)
      && static_cast<ObDDLTaskStatus>(task_status_) != ObDDLTaskStatus::FAIL) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      switch_status(new_status, false, ret);
      LOG_WARN("switch status to build_failed", K(ret), K(old_status), K(new_status), K(task_status_) , K(*this));
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
    bool domain_index_aux_exist = true;
    bool fts_doc_word_exist = true;
    if (status <= ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA) {
      is_all_exist = true;
      if (OB_INVALID_ID != rowkey_doc_aux_table_id_) {
        if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                   rowkey_doc_aux_table_id_,
                                                   rowkey_doc_exist))) {
          LOG_WARN("check rowkey doc table exist failed", K(ret), K(tenant_id_),
              K(rowkey_doc_aux_table_id_));
        } else {
          is_all_exist &= rowkey_doc_exist;
        }
      }
      if (OB_INVALID_ID != domain_index_aux_table_id_) {
        if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                   domain_index_aux_table_id_,
                                                   domain_index_aux_exist))) {
          LOG_WARN("check fts index aux table exist failed", K(ret), K(tenant_id_),
              K(domain_index_aux_table_id_));
        } else {
          is_all_exist &= domain_index_aux_exist;
        }
      }
    } else {
      if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                 rowkey_doc_aux_table_id_,
                                                 rowkey_doc_exist))) {
        LOG_WARN("check rowkey doc table exist failed", K(ret), K(tenant_id_),
            K(rowkey_doc_aux_table_id_));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        doc_rowkey_aux_table_id_,
                                                        doc_rowkey_exist))) {
        LOG_WARN("check doc rowkey table exist failed", K(ret), K(tenant_id_),
            K(doc_rowkey_aux_table_id_));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        domain_index_aux_table_id_,
                                                        domain_index_aux_exist))) {
        LOG_WARN("check fts index aux table exist failed", K(ret), K(tenant_id_),
            K(domain_index_aux_table_id_));
      } else if (is_fts_task()
        && OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                  fts_doc_word_aux_table_id_,
                                                  fts_doc_word_exist))) {
        LOG_WARN("check fts doc word table exist failed", K(ret), K(tenant_id_),
            K(fts_doc_word_aux_table_id_));
      } else {
        is_all_exist = (rowkey_doc_exist && doc_rowkey_exist &&
                        domain_index_aux_exist && fts_doc_word_exist);
        if (!is_all_exist) {
          LOG_WARN("fts aux table not exist", K(rowkey_doc_exist),
              K(doc_rowkey_exist), K(domain_index_aux_exist), K(fts_doc_word_exist));
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
        // skip checksum internal
        // ref todo @ verify_children_checksum
        next_status = ObDDLTaskStatus::SUCCESS;
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

  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status = static_cast<ObDDLTaskStatus>(task_status_);
    ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
    if (OB_FAIL(ret)) {
      next_status = ObIDDLTask::in_ddl_retry_white_list(ret) ? next_status : ObDDLTaskStatus::FAIL;
    } else if (OB_FAIL(get_next_status(next_status))) {
      next_status = ObDDLTaskStatus::FAIL;
      LOG_WARN("failed to get next status", K(ret), K(next_status));
    }

    (void)switch_status(next_status, true, ret);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObFtsIndexBuildTask::prepare_aux_table(
    const ObIndexType index_type,
    bool &task_submitted,
    uint64_t &aux_table_id,
    int64_t &task_id)
{
  int ret = OB_SUCCESS;
  const int64_t num_fts_child_task = 4;
  const uint64_t data_table_id = object_id_;
  int64_t ddl_rpc_timeout = 0;
  ObDDLService &ddl_service = root_service_->get_ddl_service();
  obrpc::ObCommonRpcProxy *common_rpc = nullptr;
  if (!dependent_task_result_map_.created() &&
      OB_FAIL(dependent_task_result_map_.create(num_fts_child_task,
                                                lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service is nullptr", K(ret));
  } else if (OB_FALSE_IT(common_rpc = root_service_->get_ddl_service().get_common_rpc())) {
  } else if (OB_ISNULL(common_rpc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc is nullptr", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_,
                                                    data_table_id,
                                                    ddl_rpc_timeout))) {
    LOG_WARN("get ddl rpc timeout fail", K(ret));
  } else {
    SMART_VARS_3((obrpc::ObCreateIndexArg, index_arg),
                 (obrpc::ObCreateAuxIndexArg, arg),
                 (obrpc::ObCreateAuxIndexRes, res)) {
      arg.tenant_id_ = tenant_id_;
      arg.exec_tenant_id_ = tenant_id_;
      arg.data_table_id_ = data_table_id;
      arg.task_id_ = task_id_;
      if (task_submitted) {
        // do nothing
      } else if (OB_FAIL(construct_create_index_arg(index_type, index_arg))) {
        LOG_WARN("failed to construct rowkey doc id arg", K(ret));
      } else if (OB_FAIL(arg.create_index_arg_.assign(index_arg))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to assign create index arg", K(ret));
      } else if (OB_FAIL(common_rpc-> to(obrpc::ObRpcProxy::myaddr_).
                         timeout(ddl_rpc_timeout).create_aux_index(arg, res))) {
        LOG_WARN("generate fts aux index schema failed", K(ret), K(arg));
      } else if (res.schema_generated_) {
        task_submitted = true;
        aux_table_id = res.aux_table_id_;
        if (res.ddl_task_id_ < 0) {
          // rowkey_doc/doc_rowkey table already exist and data is ready
          task_id = OB_INVALID_ID;
        } else { // need to wait data complement finish
          task_id = res.ddl_task_id_;
          TCWLockGuard guard(lock_);
          DependTaskStatus status;
          // check if child task is already added
          if (OB_FAIL(dependent_task_result_map_.get_refactored(aux_table_id,
                                                                status))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              status.task_id_ = res.ddl_task_id_;
              if (OB_FAIL(dependent_task_result_map_.set_refactored(aux_table_id,
                                                                    status))) {
                LOG_WARN("set dependent task map failed", K(ret), K(aux_table_id));
              }
            } else {
              LOG_WARN("get from dependent task map failed", K(ret), K(dependent_task_result_map_.size()));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuildTask::prepare_rowkey_doc_table()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  const ObIndexType index_type = ObIndexType::INDEX_TYPE_ROWKEY_DOC_ID_LOCAL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::GENERATE_ROWKEY_DOC_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(prepare_aux_table(index_type,
                                       rowkey_doc_task_submitted_,
                                       rowkey_doc_aux_table_id_,
                                       rowkey_doc_task_id_))) {
    LOG_WARN("failed to prepare aux table", K(ret), K(index_type),
        K(rowkey_doc_task_submitted_), K(rowkey_doc_aux_table_id_));
  }
  if (OB_SUCC(ret) && rowkey_doc_task_submitted_) {
    state_finished = true;
  }
  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status = static_cast<ObDDLTaskStatus>(task_status_);
    ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
    if (OB_FAIL(ret)) {
      next_status = ObIDDLTask::in_ddl_retry_white_list(ret) ? next_status : ObDDLTaskStatus::FAIL;
    } else if (OB_FAIL(get_next_status(next_status))) {
      next_status = ObDDLTaskStatus::FAIL;
      LOG_WARN("failed to get next status", K(ret), K(next_status));
    }
    (void)switch_status(next_status, next_status != ObDDLTaskStatus::FAIL, ret);
    LOG_WARN("generate schema finished", K(ret), K(parent_task_id_), K(task_id_), K(old_status), K(next_status), K(*this));

    ret = OB_SUCCESS;
  }
  return ret;
}

int ObFtsIndexBuildTask::prepare_aux_index_tables()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  const ObIndexType doc_rowkey_type = ObIndexType::INDEX_TYPE_DOC_ID_ROWKEY_LOCAL;
  const ObIndexType domain_index_aux_type = is_fts_task() ?
    ObIndexType::INDEX_TYPE_FTS_INDEX_LOCAL : ObIndexType::INDEX_TYPE_NORMAL_MULTIVALUE_LOCAL ;
  const ObIndexType fts_doc_word_type = ObIndexType::INDEX_TYPE_FTS_DOC_WORD_LOCAL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(prepare_aux_table(doc_rowkey_type,
                                       doc_rowkey_task_submitted_,
                                       doc_rowkey_aux_table_id_,
                                       doc_rowkey_task_id_))) {
    LOG_WARN("failed to prepare aux table", K(ret),
        K(doc_rowkey_task_submitted_), K(doc_rowkey_aux_table_id_));
  } else if (OB_FAIL(prepare_aux_table(domain_index_aux_type,
                                       domain_index_aux_task_submitted_,
                                       domain_index_aux_table_id_,
                                       domain_index_aux_task_id_))) {
    LOG_WARN("failed to prepare aux table", K(ret),
        K(domain_index_aux_task_submitted_), K(domain_index_aux_table_id_));
  } else if (!is_fts_task()) {
    fts_doc_word_task_submitted_ = true;
  } else if (OB_FAIL(prepare_aux_table(fts_doc_word_type,
                                       fts_doc_word_task_submitted_,
                                       fts_doc_word_aux_table_id_,
                                       fts_doc_word_task_id_))) {
    LOG_WARN("failed to prepare aux table", K(ret),
        K(fts_doc_word_task_submitted_), K(fts_doc_word_aux_table_id_));
  }
  if (OB_SUCC(ret) && doc_rowkey_task_submitted_ &&
      domain_index_aux_task_submitted_ && fts_doc_word_task_submitted_) {
    state_finished = true;
  }

  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status = static_cast<ObDDLTaskStatus>(task_status_);
    ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
    if (OB_FAIL(ret)) {
      next_status = ObIDDLTask::in_ddl_retry_white_list(ret) ? next_status : ObDDLTaskStatus::FAIL;
    } else if (OB_FAIL(get_next_status(next_status))) {
      next_status = ObDDLTaskStatus::FAIL;
      LOG_WARN("failed to get next status", K(ret), K(next_status));
    }
    (void)switch_status(next_status, next_status != ObDDLTaskStatus::FAIL, ret);
    LOG_WARN("generate schema finished", K(ret), K(parent_task_id_), K(task_id_), K(old_status), K(next_status), K(*this));

    ret = OB_SUCCESS;
  }
  return ret;
}

int ObFtsIndexBuildTask::construct_create_index_arg(
    const ObIndexType index_type,
    obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (share::schema::is_rowkey_doc_aux(index_type)) {
    if (OB_FAIL(construct_rowkey_doc_arg(arg))) {
      LOG_WARN("failed to construct rowkey doc arg", K(ret));
    }
  } else if (share::schema::is_doc_rowkey_aux(index_type)) {
    if (OB_FAIL(construct_doc_rowkey_arg(arg))) {
      LOG_WARN("failed to construct doc rowkey arg", K(ret));
    }
  } else if (share::schema::is_fts_index_aux(index_type) ||
    share::schema::is_multivalue_index_aux(index_type)) {
    if (OB_FAIL(construct_domain_index_aux_arg(arg))) {
      LOG_WARN("failed to construct fts index aux arg", K(ret));
    }
  } else if (is_fts_task() && share::schema::is_fts_doc_word_aux(index_type)) {
    if (OB_FAIL(construct_fts_doc_word_arg(arg))) {
      LOG_WARN("failed to construct fts doc word arg", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("undexpected index type", K(ret), K(index_type));
  }
  return ret;
}

int ObFtsIndexBuildTask::construct_rowkey_doc_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_ROWKEY_DOC_ID_LOCAL)) {
  } else if (FALSE_IT(arg.index_option_.parser_name_.reset())) {
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
  } else if (FALSE_IT(arg.index_option_.parser_name_.reset())) {
  } else if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(arg,
                                                                        &allocator_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObFtsIndexBuildTask::construct_domain_index_aux_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (is_fts_task()) {
    arg.index_type_ = INDEX_TYPE_FTS_INDEX_LOCAL;
    if (OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(arg, &allocator_))) {
      LOG_WARN("failed to generate index name", K(ret));
    }
  } else {
    arg.index_type_ = INDEX_TYPE_NORMAL_MULTIVALUE_LOCAL;
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
  } else if (share::schema::is_fts_index_aux(index_type) ||
    is_multivalue_index_aux(index_type)) {
    domain_index_aux_table_id_ = aux_table_id;
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
  } else if (share::schema::is_fts_index_aux(index_type) ||
    is_multivalue_index_aux(index_type)) {
    index_table_id = domain_index_aux_table_id_;
  } else if (share::schema::is_fts_doc_word_aux(index_type)) {
    index_table_id = fts_doc_word_aux_table_id_;
  }
  return ret;
}

// wait data complement of aux index tables
int ObFtsIndexBuildTask::wait_aux_table_complement()
{
  using task_iter = common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus>::const_iterator;
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
          child_task_failed = true;
          state_finished = true;
        }
      }
    }
    if (finished_task_cnt == dependent_task_result_map_.size() || OB_FAIL(ret)) {
      // 1. all child tasks finish.
      // 2. the parent task exits if any child task fails.
      state_finished = true;
    }
  }

  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status;
    ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
    // 1. get next_status
    if (child_task_failed || OB_FAIL(ret)) {
      next_status = ObDDLTaskStatus::FAIL;
      if (!ObIDDLTask::in_ddl_retry_white_list(ret)) {
        next_status = ObDDLTaskStatus::FAIL;
      }
    } else {
      if (OB_FAIL(get_next_status(next_status))) {
        next_status = ObDDLTaskStatus::FAIL;
        LOG_WARN("failed to get next status", K(ret));
      }
    }

    (void)switch_status(next_status, next_status != ObDDLTaskStatus::FAIL, ret);
    LOG_WARN("wait aux table complement finished", K(ret), K(parent_task_id_),
            K(task_id_), K(old_status), K(next_status), K(*this));
    ret = OB_SUCCESS;
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
      share::ObDomainDependTaskStatus status;
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
    share::ObDomainDependTaskStatus status;
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
          K(arg.index_table_id_), K(ddl_rpc_timeout), "ddl_stmt_str", arg.ddl_stmt_str_);
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
  int8_t rowkey_doc_submitted = static_cast<int8_t>(rowkey_doc_task_submitted_);
  int8_t doc_rowkey_submitted = static_cast<int8_t>(doc_rowkey_task_submitted_);
  int8_t fts_index_aux_submitted = static_cast<int8_t>(domain_index_aux_task_submitted_);
  int8_t fts_doc_word_submitted = static_cast<int8_t>(fts_doc_word_task_submitted_);
  int8_t drop_index_submitted = static_cast<int8_t>(drop_index_task_submitted_);

  int8_t is_rowkey_doc_succ = static_cast<int8_t>(is_rowkey_doc_succ_);
  int8_t is_doc_rowkey_succ = static_cast<int8_t>(is_doc_rowkey_succ_);
  int8_t is_domain_aux_succ = static_cast<int8_t>(is_domain_aux_succ_);
  int8_t is_fts_doc_word_succ = static_cast<int8_t>(is_fts_doc_word_succ_);

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
                                           domain_index_aux_table_id_))) {
    LOG_WARN("serialize fts index table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           fts_doc_word_aux_table_id_))) {
    LOG_WARN("serialize fts doc word table id failed", K(ret));
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
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               rowkey_doc_task_id_))) {
    LOG_WARN("serialize rowkey doc task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               doc_rowkey_task_id_))) {
    LOG_WARN("serialize doc rowkey task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               domain_index_aux_task_id_))) {
    LOG_WARN("serialize fts index aux task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               fts_doc_word_task_id_))) {
    LOG_WARN("serialize fts doc word task id failed", K(ret));
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
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              is_rowkey_doc_succ))) {
    LOG_WARN("serialize drop fts index task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              is_doc_rowkey_succ))) {
    LOG_WARN("serialize drop fts index task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              is_domain_aux_succ))) {
    LOG_WARN("serialize drop fts index task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              is_fts_doc_word_succ))) {
    LOG_WARN("serialize drop fts index task submitted failed", K(ret));
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
  const int64_t num_fts_child_task = 4;
  int8_t rowkey_doc_submitted = 0;
  int8_t doc_rowkey_submitted = 0;
  int8_t fts_index_aux_submitted = 0;
  int8_t fts_doc_word_submitted = 0;
  int8_t drop_index_submitted = 0;

  int8_t is_rowkey_doc_succ = false;
  int8_t is_doc_rowkey_succ = false;
  int8_t is_domain_aux_succ = false;
  int8_t is_fts_doc_word_succ = false;
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
                                           domain_index_aux_table_id_))) {
    LOG_WARN("fail to deserialize fts index aux table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           fts_doc_word_aux_table_id_))) {
    LOG_WARN("fail to deserialize fts doc word table id", K(ret));
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
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &rowkey_doc_task_id_))) {
    LOG_WARN("fail to deserialize rowkey doc task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &doc_rowkey_task_id_))) {
    LOG_WARN("fail to deserialize doc rowkey task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &domain_index_aux_task_id_))) {
    LOG_WARN("fail to deserialize fts index aux task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &fts_doc_word_task_id_))) {
    LOG_WARN("fail to deserialize fts doc word task id", K(ret));
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
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &is_rowkey_doc_succ))) {
    LOG_WARN("fail to deserialize rowkey doc task succ", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &is_doc_rowkey_succ))) {
    LOG_WARN("fail to deserialize doc rowkey task succ", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &is_domain_aux_succ))) {
    LOG_WARN("fail to deserialize fts index aux task succ", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &is_fts_doc_word_succ))) {
    LOG_WARN("fail to deserialize fts doc word task succ", K(ret));
  } else if (!dependent_task_result_map_.created() &&
             OB_FAIL(dependent_task_result_map_.create(num_fts_child_task,
                                                       lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else {
    if (OB_SUCC(ret) && rowkey_doc_task_id_ > 0) {
      DependTaskStatus rowkey_doc_status;
      rowkey_doc_status.task_id_ = rowkey_doc_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(rowkey_doc_aux_table_id_,
                                                            rowkey_doc_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(rowkey_doc_aux_table_id_),
            K(rowkey_doc_status));
      }
    }
    if (OB_SUCC(ret) && doc_rowkey_task_id_ > 0) {
      DependTaskStatus doc_rowkey_status;
      doc_rowkey_status.task_id_ = doc_rowkey_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(doc_rowkey_aux_table_id_,
                                                            doc_rowkey_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(doc_rowkey_aux_table_id_),
            K(doc_rowkey_status));
      }
    }
    if (OB_SUCC(ret) && domain_index_aux_task_id_ > 0) {
      DependTaskStatus fts_index_aux_status;
      fts_index_aux_status.task_id_ = domain_index_aux_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(domain_index_aux_table_id_,
                                                            fts_index_aux_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(domain_index_aux_table_id_),
            K(fts_index_aux_status));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_fts_task() ) {
      fts_doc_word_submitted = true;
    } else if (fts_doc_word_task_id_ > 0) {
      DependTaskStatus fts_doc_word_status;
      fts_doc_word_status.task_id_ = fts_doc_word_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(fts_doc_word_aux_table_id_,
                                                            fts_doc_word_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(fts_doc_word_aux_table_id_),
            K(fts_doc_word_status));
      }
    }
    rowkey_doc_task_submitted_ = rowkey_doc_submitted;
    doc_rowkey_task_submitted_ = doc_rowkey_submitted;
    domain_index_aux_task_submitted_ = fts_index_aux_submitted;
    fts_doc_word_task_submitted_ = fts_doc_word_submitted;
    drop_index_task_submitted_ = drop_index_submitted;
    is_rowkey_doc_succ_ = is_rowkey_doc_succ;
    is_doc_rowkey_succ_ = is_doc_rowkey_succ;
    is_domain_aux_succ_ = is_domain_aux_succ;
    is_fts_doc_word_succ_ = is_fts_doc_word_succ;
  }
  return ret;
}

int64_t ObFtsIndexBuildTask::get_serialize_param_size() const
{
  int8_t rowkey_doc_submitted = static_cast<int8_t>(rowkey_doc_task_submitted_);
  int8_t doc_rowkey_submitted = static_cast<int8_t>(doc_rowkey_task_submitted_);
  int8_t fts_index_aux_submitted = static_cast<int8_t>(domain_index_aux_task_submitted_);
  int8_t fts_doc_word_submitted = static_cast<int8_t>(fts_doc_word_task_submitted_);
  int8_t drop_index_submitted = static_cast<int8_t>(drop_index_task_submitted_);

  int8_t is_rowkey_doc_succ = static_cast<int8_t>(is_rowkey_doc_succ_);
  int8_t is_doc_rowkey_succ = static_cast<int8_t>(is_doc_rowkey_succ_);
  int8_t is_domain_aux_succ = static_cast<int8_t>(is_domain_aux_succ_);
  int8_t is_fts_doc_word_succ = static_cast<int8_t>(is_fts_doc_word_succ_);

  return create_index_arg_.get_serialize_size()
      + ObDDLTask::get_serialize_param_size()
      + serialization::encoded_length(rowkey_doc_aux_table_id_)
      + serialization::encoded_length(doc_rowkey_aux_table_id_)
      + serialization::encoded_length(domain_index_aux_table_id_)
      + serialization::encoded_length(fts_doc_word_aux_table_id_)
      + serialization::encoded_length_i8(rowkey_doc_submitted)
      + serialization::encoded_length_i8(doc_rowkey_submitted)
      + serialization::encoded_length_i8(fts_index_aux_submitted)
      + serialization::encoded_length_i8(fts_doc_word_submitted)
      + serialization::encoded_length_i64(rowkey_doc_task_id_)
      + serialization::encoded_length_i64(doc_rowkey_task_id_)
      + serialization::encoded_length_i64(domain_index_aux_task_id_)
      + serialization::encoded_length_i64(fts_doc_word_task_id_)
      + serialization::encoded_length_i8(drop_index_submitted)
      + serialization::encoded_length_i64(drop_index_task_id_)
      + serialization::encoded_length_i8(is_rowkey_doc_succ)
      + serialization::encoded_length_i8(is_doc_rowkey_succ)
      + serialization::encoded_length_i8(is_domain_aux_succ)
      + serialization::encoded_length_i8(is_fts_doc_word_succ);
}

int ObFtsIndexBuildTask::get_task_status(int64_t task_id, uint64_t aux_table_id, bool& is_succ)
{
  int ret = OB_SUCCESS;

  if (task_id != 0) {
    HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
      int64_t unused_user_msg_len = 0;
      ObAddr unused_addr;
      const int64_t target_object_id = -1;
      if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                  dst_tenant_id_,
                                                  task_id,
                                                  target_object_id,
                                                  unused_addr,
                                                  false /* is_ddl_retry_task */,
                                                  *GCTX.sql_proxy_,
                                                  error_message,
                                                  unused_user_msg_len))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("ddl task not finish", K(ret), K(aux_table_id), K(task_id), K(task_id_));
        } else {
          LOG_WARN("fail to get ddl error message", K(ret), K(aux_table_id), K(task_id), K(task_id_));
        }
      } else {
        if (error_message.ret_code_ == OB_SUCCESS) {
          is_succ = true;
        }
        LOG_WARN("create index task status", K(ret), K(task_id), K(aux_table_id), K(error_message.ret_code_));
      }
    }
  }

  return ret;
}

int ObFtsIndexBuildTask::get_task_status()
{
  int ret = OB_SUCCESS;
  if (rowkey_doc_task_id_ > 0 &&
      OB_FAIL(get_task_status(rowkey_doc_task_id_, rowkey_doc_aux_table_id_, is_rowkey_doc_succ_))) {
    LOG_WARN("get task status", K(ret), K(rowkey_doc_task_id_), K(rowkey_doc_aux_table_id_));
  } else if (doc_rowkey_task_id_ > 0 &&
      OB_FAIL(get_task_status(doc_rowkey_task_id_, doc_rowkey_aux_table_id_, is_doc_rowkey_succ_))) {
    LOG_WARN("get task status", K(ret), K(doc_rowkey_task_id_), K(doc_rowkey_aux_table_id_));
  } else if (domain_index_aux_task_id_ > 0 &&
      OB_FAIL(get_task_status(domain_index_aux_task_id_, domain_index_aux_table_id_, is_domain_aux_succ_))) {
    LOG_WARN("get task status", K(ret), K(domain_index_aux_task_id_), K(domain_index_aux_table_id_));
  } else if (fts_doc_word_task_id_ > 0 &&
      OB_FAIL(get_task_status(fts_doc_word_task_id_, fts_doc_word_aux_table_id_, is_fts_doc_word_succ_))) {
    LOG_WARN("get task status", K(ret), K(fts_doc_word_task_id_), K(fts_doc_word_aux_table_id_));
  }
  return ret;
}

int ObFtsIndexBuildTask::clean_on_failed()
{
  using task_iter = common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus>::const_iterator;
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
                    K(task_key), K(child_task_id), K(target_object_id), K(dependent_task_result_map_.size()));
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
  const ObTableSchema *domain_index_aux_schema = nullptr;
  const ObTableSchema *rowkey_doc_schema = nullptr;
  const ObTableSchema *doc_rowkey_schema = nullptr;
  const ObTableSchema *fts_doc_word_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  ObString index_name;

  if (OB_FAIL(get_task_status())) {
    LOG_WARN("failed to get fts index status", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().
                                           get_tenant_schema_guard(tenant_id_,
                                                                   schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret));
  } else {
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                              domain_index_aux_table_id_,
                                              domain_index_aux_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get table schema", K(ret), K(domain_index_aux_table_id_));
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
    } else if (is_fts_task() &&
               OB_INVALID_ID != fts_doc_word_aux_table_id_ &&
               OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     fts_doc_word_aux_table_id_,
                                                     fts_doc_word_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get table schema", K(ret), K(fts_doc_word_aux_table_id_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(domain_index_aux_schema)) {
    // TODO yunshan.tys fix create drop fts index when fts index schema is not generated
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fts index aux schema is nullptr, fail to roll back", K(ret));
    } else if (OB_FAIL(domain_index_aux_schema->get_index_name(index_name))) {
      LOG_WARN("fail to get domain index name", K(ret), KPC(domain_index_aux_schema));
    } else if (!is_rowkey_doc_succ_ && !is_doc_rowkey_succ_ && !is_fts_doc_word_succ_ && !is_domain_aux_succ_) {
      drop_index_task_submitted_ = true;
      LOG_WARN("all aux table not create success, needn't submit task", K(ret), K(is_fts_task()), K(*this));
    } else {
      ObDDLType ddl_type = is_fts_task() ? ObDDLType::DDL_DROP_FTS_INDEX : ObDDLType::DDL_DROP_MULVALUE_INDEX;
      ObDDLTaskRecord task_record;
      obrpc::ObDropIndexArg drop_index_arg;
      drop_index_arg.index_name_ = index_name;
      drop_index_arg.tenant_id_ = domain_index_aux_schema->get_tenant_id();
      drop_index_arg.index_table_id_ = domain_index_aux_schema->get_table_id();

      ObCreateDDLTaskParam param(tenant_id_,
                                 ddl_type,
                                 is_domain_aux_succ_? domain_index_aux_schema : nullptr,
                                 nullptr,
                                 0/*object_id*/,
                                 domain_index_aux_schema->get_schema_version(),
                                 parallelism_,
                                 consumer_group_id_,
                                 &allocator_,
                                 &drop_index_arg);
      param.tenant_data_version_ = data_format_version_;
      param.aux_rowkey_doc_schema_ = is_rowkey_doc_succ_ ? rowkey_doc_schema : nullptr;
      param.aux_doc_rowkey_schema_ = is_doc_rowkey_succ_ ? doc_rowkey_schema : nullptr;
      param.aux_doc_word_schema_ = is_fts_doc_word_succ_ ? fts_doc_word_schema : nullptr;
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
          LOG_INFO("add drop fts index task", K(ret), K(domain_index_aux_table_id_),
              K(rowkey_doc_aux_table_id_), K(doc_rowkey_aux_table_id_),
              K(fts_doc_word_aux_table_id_), K(create_index_arg_.index_name_),
              K(domain_index_aux_schema->get_schema_version()), K(param.schema_version_));
        }
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
  } else if (-1 == drop_index_task_id_) {
     is_finish = true;
  } else if (!is_rowkey_doc_succ_ && !is_doc_rowkey_succ_ && !is_fts_doc_word_succ_ && !is_domain_aux_succ_) {
    is_finish = true;
    LOG_WARN("all aux table not create success, needn't submit task", K(ret), K(is_fts_task()), K(*this));
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
  // TODO
  // remove , as 1st, 2nd table get difference snapshot leading checksum not equal
  // issue 2024080900104086008
  int ret = OB_SUCCESS;
  ObDDLTaskStatus next_status;
  ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
  if (OB_FAIL(get_next_status(next_status))) {
      next_status = ObDDLTaskStatus::FAIL;
      LOG_WARN("failed to get next status", K(ret));
  }
  (void)switch_status(next_status, next_status != ObDDLTaskStatus::FAIL, ret);
  LOG_WARN("validate checksum finished", K(ret), K(parent_task_id_), K(old_status), K(task_id_), K(*this));
  ret = OB_SUCCESS;

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
    const uint64_t index_table_id = domain_index_aux_table_id_;
    ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_schema = nullptr;
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
    } else if (OB_ISNULL(data_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get table schema", K(ret), KPC(data_schema));
    } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), dst_tenant_id_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                   task_id_))) {
      LOG_WARN("failed to get owner id", K(ret), K(task_id_));
    } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(*data_schema,
                                                            index_table_id,
                                                            false /* is_global_index = false */,
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

int ObFtsIndexBuildTask::verify_children_checksum() const
{
  // TODO
  // remove , as 1st, 2nd table get difference snapshot leading checksum not equal
  // issue 2024080900104086008
  return OB_SUCCESS;
}

int ObFtsIndexBuildTask::check_column_checksum(const ColumnChecksumInfo &a, const ColumnChecksumInfo &b) const
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, int64_t> a_table_column_checksums;
  hash::ObHashMap<int64_t, int64_t> b_table_column_checksums;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)
      || OB_UNLIKELY(!a.is_valid())
      || OB_UNLIKELY(!b.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id_), K(a), K(b));
  } else if (OB_FAIL(a_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, "FtsCKSMap", "FtsCKSMap", tenant_id_))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(b_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, "FtsCKSMap", "FtsCKSMap", tenant_id_))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(ObDDLChecksumOperator::get_table_column_checksum_without_execution_id(tenant_id_, a.table_id_, a.table_id_,
          a.task_id_, false/*is_unique_index_checking*/, a_table_column_checksums, root_service_->get_sql_proxy()))) {
    LOG_WARN("fail to get table column checksum", K(ret), K(a));
  } else if (OB_FAIL(ObDDLChecksumOperator::get_table_column_checksum_without_execution_id(tenant_id_, b.table_id_, b.table_id_,
          b.task_id_, false/*is_unique_index_checking*/, b_table_column_checksums, root_service_->get_sql_proxy()))) {
    LOG_WARN("fail to get table column checksum", K(ret), K(b));
  } else {
    for (hash::ObHashMap<int64_t, int64_t>::const_iterator iter = b_table_column_checksums.begin();
      OB_SUCC(ret) && iter != b_table_column_checksums.end(); ++iter) {
      int64_t a_table_column_checksum = 0;
      if (OB_FAIL(a_table_column_checksums.get_refactored(iter->first, a_table_column_checksum))) {
        LOG_WARN("fail to get data table column checksum", K(ret), "column_id", iter->first);
      } else if (a_table_column_checksum != iter->second) {
        ret = OB_CHECKSUM_ERROR;
        LOG_ERROR("column checksum is not equal", K(ret), K(a.table_id_), K(b.table_id_),
            "column_id", iter->first, K(a_table_column_checksum),
            "b_table_column_checksum", iter->second);
      }
    }
  }
  if (a_table_column_checksums.created()) {
    a_table_column_checksums.destroy();
  }
  if (b_table_column_checksums.created()) {
    b_table_column_checksums.destroy();
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
