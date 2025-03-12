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

#include "rootserver/ddl_task/ob_vec_index_build_task.h"
#include "share/ob_ddl_sim_point.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "rootserver/ob_root_service.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/ob_vec_index_builder_util.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{
/***************         ObVecIndexBuildTask        *************/

ObVecIndexBuildTask::ObVecIndexBuildTask()
  : ObDDLTask(ObDDLType::DDL_CREATE_VEC_INDEX),
    index_table_id_(target_object_id_),
    rowkey_vid_aux_table_id_(OB_INVALID_ID),
    vid_rowkey_aux_table_id_(OB_INVALID_ID),
    delta_buffer_table_id_(OB_INVALID_ID),
    index_id_table_id_(OB_INVALID_ID),
    index_snapshot_data_table_id_(OB_INVALID_ID),
    rowkey_vid_task_submitted_(false),
    vid_rowkey_task_submitted_(false),
    delta_buffer_task_submitted_(false),
    index_id_task_submitted_(false),
    index_snapshot_data_task_submitted_(false),
    rowkey_vid_task_id_(0),
    vid_rowkey_task_id_(0),
    delta_buffer_task_id_(0),
    index_id_task_id_(0),
    index_snapshot_task_id_(0),
    drop_index_task_submitted_(false),
    drop_index_task_id_(-1),
    is_rebuild_index_(false),
    root_service_(nullptr),
    create_index_arg_(),
    dependent_task_result_map_()
{
}

ObVecIndexBuildTask::~ObVecIndexBuildTask()
{
}

int ObVecIndexBuildTask::init(
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
  const bool is_rebuild_index = create_index_arg.is_rebuild_index_;
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
                         !(tenant_data_version > 0) ||
                         task_status < ObDDLTaskStatus::PREPARE ||
                         task_status > ObDDLTaskStatus::SUCCESS ||
                         snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id),
        KPC(data_table_schema), KPC(index_schema), K(schema_version), K(parallelism),
        K(consumer_group_id), K(create_index_arg.is_valid()), K(create_index_arg),
        K(task_status), K(snapshot_version), K(is_rebuild_index));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_,
                                         create_index_arg,
                                         create_index_arg_))) {
    LOG_WARN("fail to copy create index arg", K(ret), K(create_index_arg));
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
    if (index_schema->is_vec_rowkey_vid_type()) {
      rowkey_vid_aux_table_id_ = index_table_id_;
    } else if (index_schema->is_vec_delta_buffer_type()) {
      delta_buffer_table_id_ = index_table_id_;
    }
    task_version_ = OB_VEC_INDEX_BUILD_TASK_VERSION;
    start_time_ = ObTimeUtility::current_time();
    data_format_version_ = tenant_data_version;
    is_rebuild_index_ = is_rebuild_index;
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

int ObVecIndexBuildTask::init(const ObDDLTaskRecord &task_record)
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
      LOG_WARN("init ddl task monitor info failed", K(ret));
    } else {
      is_inited_ = true;
      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObVecIndexBuildTask::process()
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = create_index_arg_.index_type_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check health failed", K(ret));
  } else if (!share::schema::is_vec_index(index_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect index type is of vec index", K(ret), K(index_type));
  } else if (!need_retry()) {
    // by pass
  } else {
    // switch case for diff create_index_arg, since there are 5 aux tables
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
    case ObDDLTaskStatus::PREPARE: {
      if (OB_FAIL(prepare())) {
        LOG_WARN("prepare failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_ROWKEY_VID_SCHEMA: {
      if (OB_FAIL(prepare_rowkey_vid_table())) {
        LOG_WARN("generate schema failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_ROWKEY_VID_TABLE_COMPLEMENT: {
      if (OB_FAIL(wait_aux_table_complement())) {
        LOG_WARN("wait rowkey_vid table complement failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_VEC_AUX_SCHEMA: {
      if (OB_FAIL(prepare_aux_index_tables())) {
        LOG_WARN("generate schema failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_VEC_AUX_TABLE_COMPLEMENT: {
      if (OB_FAIL(wait_aux_table_complement())) {
        LOG_WARN("wait aux table complement failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_VID_ROWKEY_SCHEMA: {
      if (OB_FAIL(prepare_vid_rowkey_table())) {
        LOG_WARN("generate schema failed", K(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_VID_ROWKEY_TABLE_COMPLEMENT: {
      if (OB_FAIL(wait_aux_table_complement())) {
        LOG_WARN("wait aux table complement failed", K(ret), K(*this));
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

bool ObVecIndexBuildTask::is_valid() const
{
  return is_inited_ && !trace_id_.is_invalid();
}

int ObVecIndexBuildTask::deep_copy_index_arg(
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

int ObVecIndexBuildTask::check_health()
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
      LOG_WARN("fail to get index_schema", K(ret), K(index_table_id_));
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
    if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)
      && static_cast<ObDDLTaskStatus>(task_status_) != ObDDLTaskStatus::FAIL) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      (void)switch_status(new_status, false, ret);
      LOG_WARN("switch status to build_failed", K(ret), KP(this), K(old_status), K(new_status));
    }
    if (ObDDLTaskStatus::FAIL == static_cast<ObDDLTaskStatus>(task_status_) ||
        ObDDLTaskStatus::SUCCESS == static_cast<ObDDLTaskStatus>(task_status_)) {
      ret = OB_SUCCESS; // allow clean up
    }
  }
  check_ddl_task_execute_too_long();
  return ret;
}

int ObVecIndexBuildTask::check_aux_table_schemas_exist(bool &is_all_exist)
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
    bool rowkey_vid_exist = true;
    bool vid_rowkey_exist = true;
    bool delta_buffer_aux_exist = true;
    bool index_id_exist = true;
    bool index_snapshot_data_exist = true;
    if (status <= ObDDLTaskStatus::GENERATE_VEC_AUX_SCHEMA) {
      is_all_exist = true;
      if (OB_INVALID_ID != rowkey_vid_aux_table_id_) {
        if (!is_rebuild_index_ &&
            OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                   rowkey_vid_aux_table_id_,
                                                   rowkey_vid_exist))) {
          LOG_WARN("check rowkey vid table exist failed", K(ret), K(tenant_id_),
              K(rowkey_vid_aux_table_id_));
        } else {
          is_all_exist &= rowkey_vid_exist;
        }
      }
      if (OB_INVALID_ID != delta_buffer_table_id_) {
        if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                   delta_buffer_table_id_,
                                                   delta_buffer_aux_exist))) {
          LOG_WARN("check delta buf index aux table exist failed", K(ret), K(tenant_id_),
              K(delta_buffer_table_id_));
        } else {
          is_all_exist &= delta_buffer_aux_exist;
        }
      }
    } else if (status <= ObDDLTaskStatus::WAIT_VID_ROWKEY_TABLE_COMPLEMENT) {
      if (!is_rebuild_index_ &&
          OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                 rowkey_vid_aux_table_id_,
                                                 rowkey_vid_exist))) {
        LOG_WARN("check rowkey_vid table exist failed", K(ret), K(tenant_id_),
            K(rowkey_vid_aux_table_id_), K(status));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        delta_buffer_table_id_,
                                                        delta_buffer_aux_exist))) {
        LOG_WARN("check delta buffer table exist failed", K(ret), K(tenant_id_),
            K(delta_buffer_table_id_), K(status));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        index_id_table_id_,
                                                        index_id_exist))) {
        LOG_WARN("check index id table exist failed", K(ret), K(tenant_id_),
            K(index_id_table_id_), K(status));
      } else {
        // is_all_exist = rowkey_vid_exist;
        is_all_exist = (rowkey_vid_exist && delta_buffer_aux_exist
                        && index_id_exist);
      }
    } else if (status == ObDDLTaskStatus::VALIDATE_CHECKSUM) {
      if (!is_rebuild_index_ &&
          OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                 rowkey_vid_aux_table_id_,
                                                 rowkey_vid_exist))) {
        LOG_WARN("check rowkey vid table exist failed", K(ret), K(tenant_id_),
            K(rowkey_vid_aux_table_id_), K(status));
      } else if (!is_rebuild_index_ &&
                 OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        vid_rowkey_aux_table_id_,
                                                        vid_rowkey_exist))) {
        LOG_WARN("check vid rowkey table exist failed", K(ret), K(tenant_id_),
            K(vid_rowkey_aux_table_id_), K(status));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        delta_buffer_table_id_,
                                                        delta_buffer_aux_exist))) {
        LOG_WARN("check delta buffer table exist failed", K(ret), K(tenant_id_),
            K(delta_buffer_table_id_), K(status));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        index_id_table_id_,
                                                        index_id_exist))) {
        LOG_WARN("check index id table exist failed", K(ret), K(tenant_id_),
            K(index_id_table_id_), K(status));
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_,
                                                        index_snapshot_data_table_id_,
                                                        index_snapshot_data_exist))) {
        LOG_WARN("check index snapshot table exist failed", K(ret), K(tenant_id_),
            K(index_snapshot_data_table_id_), K(status));
      } else {
        is_all_exist = (rowkey_vid_exist && vid_rowkey_exist &&
                        delta_buffer_aux_exist && index_id_exist &&
                        index_snapshot_data_exist);
      }
    }
    if (!is_all_exist) {
      LOG_WARN("vec aux table not exist", K(status), K(rowkey_vid_exist),
                K(vid_rowkey_exist), K(delta_buffer_aux_exist),
                K(index_id_exist), K(index_snapshot_data_exist), K(status),
                K(rowkey_vid_aux_table_id_), K(vid_rowkey_aux_table_id_),
                K(delta_buffer_table_id_), K(index_id_table_id_),
                K(index_snapshot_data_table_id_), K(drop_index_task_submitted_));
    }
  }
  return ret;
}

int ObVecIndexBuildTask::get_next_status(share::ObDDLTaskStatus &next_status)
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
        next_status = ObDDLTaskStatus::GENERATE_ROWKEY_VID_SCHEMA;
        break;
      }
      case ObDDLTaskStatus::GENERATE_ROWKEY_VID_SCHEMA: {
        next_status = ObDDLTaskStatus::WAIT_ROWKEY_VID_TABLE_COMPLEMENT;
        break;
      }
      case ObDDLTaskStatus::WAIT_ROWKEY_VID_TABLE_COMPLEMENT: {
        next_status = ObDDLTaskStatus::GENERATE_VEC_AUX_SCHEMA;
        break;
      }
      case ObDDLTaskStatus::GENERATE_VEC_AUX_SCHEMA: {
        next_status = ObDDLTaskStatus::WAIT_VEC_AUX_TABLE_COMPLEMENT;
        break;
      }
      case ObDDLTaskStatus::WAIT_VEC_AUX_TABLE_COMPLEMENT: {
        next_status = ObDDLTaskStatus::GENERATE_VID_ROWKEY_SCHEMA;
        break;
      }
      case ObDDLTaskStatus::GENERATE_VID_ROWKEY_SCHEMA: {
        next_status = ObDDLTaskStatus::WAIT_VID_ROWKEY_TABLE_COMPLEMENT;
        break;
      }
      case ObDDLTaskStatus::WAIT_VID_ROWKEY_TABLE_COMPLEMENT: {
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

int ObVecIndexBuildTask::prepare()
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
  DEBUG_SYNC(BUILD_VECTOR_INDEX_PREPARE_STATUS);
  #ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_POST_VEC_INDEX_PREPARE_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] build vec index fail to prepare", K(ret));
    }
  }
  #endif
  if (state_finished && OB_SUCC(ret)) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("prepare finished", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    }
  } else if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
    (void)switch_status(ObDDLTaskStatus::FAIL, false, ret);  // allow clean up
    LOG_INFO("prepare failed", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexBuildTask::prepare_aux_table(const ObIndexType index_type,
                                            bool &task_submitted,
                                            uint64_t &aux_table_id,
                                            int64_t &res_task_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(obrpc::ObCreateIndexArg, index_arg) {
    if (OB_FAIL(construct_create_index_arg(index_type, index_arg))) {
      LOG_WARN("failed to construct rowkey doc id arg", K(ret));
    } else if (OB_FAIL(ObDomainIndexBuilderUtil::prepare_aux_table(task_submitted,
                                                                   aux_table_id,
                                                                   res_task_id,
                                                                   lock_,
                                                                   object_id_,
                                                                   tenant_id_,
                                                                   task_id_,
                                                                   index_arg,
                                                                   root_service_,
                                                                   dependent_task_result_map_,
                                                                   obrpc::ObRpcProxy::myaddr_,
                                                                   OB_VEC_INDEX_BUILD_CHILD_TASK_NUM))) {
      LOG_WARN("fail to prepare_aux_table", K(ret), K(index_type));
    }
  } // samart var
  return ret;
}

int ObVecIndexBuildTask::prepare_rowkey_vid_table()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  const ObIndexType index_type = ObIndexType::INDEX_TYPE_VEC_ROWKEY_VID_LOCAL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::GENERATE_ROWKEY_VID_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (is_rebuild_index_) {
    LOG_DEBUG("skip prepare_rowkey_vid_table, is rebuild index");
  } else if (OB_FAIL(prepare_aux_table(index_type,
                                       rowkey_vid_task_submitted_,
                                       rowkey_vid_aux_table_id_,
                                       rowkey_vid_task_id_))) {
    LOG_WARN("failed to prepare rowkey vid aux table", K(ret), K(index_type),
        K(rowkey_vid_task_submitted_), K(rowkey_vid_aux_table_id_));
  }
  if (OB_SUCC(ret) && (rowkey_vid_task_submitted_ || is_rebuild_index_)) {
    state_finished = true;
  }
  DEBUG_SYNC(BUILD_VECTOR_INDEX_PREPARE_ROWKEY_VID);
  #ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_POST_VEC_INDEX_PREPARE_ROWKEY_VID_TBL_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] build vec index fail to prepare_rowkey_vid_table", K(ret));
    }
  }
  #endif
  if (state_finished && OB_SUCC(ret)) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("generate schema finished", K(ret), K(parent_task_id_), K(task_id_),
          K(*this));
    }
  } else if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
    (void)switch_status(ObDDLTaskStatus::FAIL, false, ret);  // allow clean up
    LOG_INFO("prepare failed", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexBuildTask::prepare_aux_index_tables()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  const ObIndexType aux_delta_buffer_type = ObIndexType::INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL;
  const ObIndexType aux_index_id_type = ObIndexType::INDEX_TYPE_VEC_INDEX_ID_LOCAL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::GENERATE_VEC_AUX_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(prepare_aux_table(aux_delta_buffer_type,
                                       delta_buffer_task_submitted_,
                                       delta_buffer_table_id_,
                                       delta_buffer_task_id_))) {
    LOG_WARN("failed to prepare delta buffer aux table", K(ret),
        K(delta_buffer_task_submitted_), K(delta_buffer_table_id_));
  } else if (OB_FAIL(prepare_aux_table(aux_index_id_type,
                                       index_id_task_submitted_,
                                       index_id_table_id_,
                                       index_id_task_id_))) {
    LOG_WARN("failed to prepare index id aux table", K(ret),
        K(index_id_task_submitted_), K(index_id_table_id_));
  }
  if (OB_SUCC(ret) && delta_buffer_task_submitted_ && index_id_task_submitted_) {
    state_finished = true;
  }
  DEBUG_SYNC(BUILD_VECTOR_INDEX_PREPARE_AUX_INDEX);
  #ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_POST_VEC_INDEX_PREPARE_DELTA_OR_INDEX_ID_TBL_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] build vec index fail to prepare_delta_or_index_id_table", K(ret));
    }
  }
  #endif
  if (state_finished && OB_SUCC(ret)) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("generate schema finished", K(ret), K(parent_task_id_), K(task_id_),
          K(*this));
    }
  } else if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
    (void)switch_status(ObDDLTaskStatus::FAIL, false, ret);  // allow clean up
    LOG_INFO("prepare failed", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexBuildTask::construct_create_index_arg(
    const ObIndexType index_type,
    obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (share::schema::is_vec_rowkey_vid_type(index_type)) {
    if (OB_FAIL(construct_rowkey_vid_arg(arg))) {
      LOG_WARN("failed to construct rowkey vid arg", K(ret));
    }
  } else if (share::schema::is_vec_vid_rowkey_type(index_type)) {
    if (OB_FAIL(construct_vid_rowkey_arg(arg))) {
      LOG_WARN("failed to construct vid rowkey arg", K(ret));
    }
  } else if (share::schema::is_vec_delta_buffer_type(index_type)) {
    if (OB_FAIL(construct_delta_buffer_arg(arg))) {
      LOG_WARN("failed to construct delta buf index aux arg", K(ret));
    }
  } else if (share::schema::is_vec_index_id_type(index_type)) {
    if (OB_FAIL(construct_index_id_arg(arg))) {
      LOG_WARN("failed to construct index id aux table arg", K(ret));
    }
  } else if (share::schema::is_vec_index_snapshot_data_type(index_type)) {
    if (OB_FAIL(construct_index_snapshot_data_arg(arg))) {
      LOG_WARN("failed to construct snapshot aux table arg", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("undexpected index type", K(ret), K(index_type));
  }
  return ret;
}

int ObVecIndexBuildTask::prepare_vid_rowkey_table()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  const ObIndexType aux_vid_rowkey_type = ObIndexType::INDEX_TYPE_VEC_VID_ROWKEY_LOCAL;
  const ObIndexType aux_index_snapshot_type = ObIndexType::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::GENERATE_VID_ROWKEY_SCHEMA != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (!is_rebuild_index_ &&
             OB_FAIL(prepare_aux_table(aux_vid_rowkey_type,
                                       vid_rowkey_task_submitted_,
                                       vid_rowkey_aux_table_id_,
                                       vid_rowkey_task_id_))) {
    LOG_WARN("failed to prepare aux vid rowkey table", K(ret),
        K(vid_rowkey_task_submitted_), K(vid_rowkey_aux_table_id_));
  } else if (OB_FAIL(prepare_aux_table(aux_index_snapshot_type,
                                       index_snapshot_data_task_submitted_,
                                       index_snapshot_data_table_id_,
                                       index_snapshot_task_id_))) {
    LOG_WARN("failed to prepare index snapshot aux table", K(ret),
        K(index_snapshot_data_task_submitted_), K(index_snapshot_data_table_id_));
  }
  if (OB_SUCC(ret) &&
    (vid_rowkey_task_submitted_ || is_rebuild_index_) && index_snapshot_data_task_submitted_) {
    state_finished = true;
  }
  DEBUG_SYNC(BUILD_VECTOR_INDEX_PREPARE_VID_ROWKEY);
  #ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_POST_VEC_INDEX_PREPARE_VID_ROWKEY_OR_SNAP_TBL_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] build vec index fail to prepare_vid_rowkey_or_snapshot_table", K(ret));
    }
  }
  #endif
  if (state_finished && OB_SUCC(ret)) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("generate schema finished", K(ret), K(parent_task_id_), K(task_id_),
          K(*this));
    }
  } else if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
    (void)switch_status(ObDDLTaskStatus::FAIL, false, ret);  // allow clean up
    LOG_INFO("prepare failed", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexBuildTask::construct_rowkey_vid_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_VEC_ROWKEY_VID_LOCAL)) {
  } else if (FALSE_IT(arg.index_option_.parser_name_.reset())) {
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator_, arg.index_type_, arg.index_name_, arg.index_name_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObVecIndexBuildTask::construct_vid_rowkey_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_VEC_VID_ROWKEY_LOCAL)) {
  } else if (FALSE_IT(arg.index_option_.parser_name_.reset())) {
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator_, arg.index_type_, arg.index_name_, arg.index_name_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObVecIndexBuildTask::construct_delta_buffer_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL)) {
  } else if (FALSE_IT(arg.index_option_.parser_name_.reset())) {
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator_, arg.index_type_, arg.index_name_, arg.index_name_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObVecIndexBuildTask::construct_index_id_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_VEC_INDEX_ID_LOCAL)) {
  } else if (FALSE_IT(arg.index_option_.parser_name_.reset())) {
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator_, arg.index_type_, arg.index_name_, arg.index_name_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObVecIndexBuildTask::construct_index_snapshot_data_arg(obrpc::ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_index_arg(allocator_, create_index_arg_, arg))) {
    LOG_WARN("failed to deep copy index arg", K(ret));
  } else if (FALSE_IT(arg.index_type_ = INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL)) {
  } else if (FALSE_IT(arg.index_option_.parser_name_.reset())) {
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator_, arg.index_type_, arg.index_name_, arg.index_name_))) {
    LOG_WARN("failed to generate index name", K(ret));
  }
  return ret;
}

int ObVecIndexBuildTask::record_index_table_id(
    const obrpc::ObCreateIndexArg *create_index_arg,
    uint64_t &aux_table_id)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = create_index_arg->index_type_;
  if (share::schema::is_vec_rowkey_vid_type(index_type)) {
    rowkey_vid_aux_table_id_ = aux_table_id;
  } else if (share::schema::is_vec_vid_rowkey_type(index_type)) {
    vid_rowkey_aux_table_id_ = aux_table_id;
  } else if (share::schema::is_vec_delta_buffer_type(index_type)) {
    delta_buffer_table_id_ = aux_table_id;
  } else if (share::schema::is_vec_index_id_type(index_type)) {
    index_id_table_id_ = aux_table_id;
  } else if (share::schema::is_vec_index_snapshot_data_type(index_type)) {
    index_snapshot_data_table_id_ = aux_table_id;
  }
  return ret;
}

int ObVecIndexBuildTask::get_index_table_id(
    const obrpc::ObCreateIndexArg *create_index_arg,
    uint64_t &index_table_id)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = create_index_arg->index_type_;
  if (share::schema::is_vec_rowkey_vid_type(index_type)) {
    index_table_id = rowkey_vid_aux_table_id_;
  } else if (share::schema::is_vec_vid_rowkey_type(index_type)) {
    index_table_id = vid_rowkey_aux_table_id_;
  } else if (share::schema::is_vec_delta_buffer_type(index_type)) {
    index_table_id = delta_buffer_table_id_;
  } else if (share::schema::is_vec_index_id_type(index_type)) {
    index_table_id = index_id_table_id_;
  } else if (share::schema::is_vec_index_snapshot_data_type(index_type)) {
    index_table_id = index_snapshot_data_table_id_;
  }
  return ret;
}

int ObVecIndexBuildTask::CheckTaskStatusFn::operator()(common::hash::HashMapPair<uint64_t, share::ObDomainDependTaskStatus> &entry)
{
  int ret = OB_SUCCESS;
  if (child_task_failed_ || state_finished_) {
    // do nothing
  } else {
    const uint64_t task_key = entry.first;
    const int64_t target_object_id = -1;
    const int64_t child_task_id = entry.second.task_id_;
    if (entry.second.ret_code_ == INT64_MAX) {
      // maybe ddl already finish when switching rs
      HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
        int64_t unused_user_msg_len = 0;
        ObAddr unused_addr;
        if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                    dest_tenant_id_,
                                                    child_task_id,
                                                    target_object_id,
                                                    unused_addr,
                                                    false, //is_ddl_retry_task
                                                    *GCTX.sql_proxy_,
                                                    error_message,
                                                    unused_user_msg_len))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("ddl task not finish", K(dest_tenant_id_), K(task_key),
                K(child_task_id), K(target_object_id));
          } else {
            LOG_WARN("fail to get ddl error message", K(ret), K(task_key),
                K(child_task_id), K(target_object_id));
          }
        } else {
          finished_task_cnt_++;
          if (error_message.ret_code_ != OB_SUCCESS) {
            ret = error_message.ret_code_;
            child_task_failed_ = true;
            state_finished_ = true;
          }
        }
      }
    } else {
      finished_task_cnt_++;
      if (entry.second.ret_code_ != OB_SUCCESS) {
        ret = entry.second.ret_code_;
        child_task_failed_ = true;
        state_finished_ = true;
      }
    }
  }
  return ret;
}

// wait data complement of aux index tables
int ObVecIndexBuildTask::wait_aux_table_complement()
{
  int ret = OB_SUCCESS;
  bool child_task_failed = false;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::WAIT_ROWKEY_VID_TABLE_COMPLEMENT != task_status_ &&
             ObDDLTaskStatus::WAIT_VEC_AUX_TABLE_COMPLEMENT != task_status_ &&
             ObDDLTaskStatus::WAIT_VID_ROWKEY_TABLE_COMPLEMENT != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (is_rebuild_index_ &&
            (ObDDLTaskStatus::WAIT_ROWKEY_VID_TABLE_COMPLEMENT == task_status_)) {
    state_finished = true;
    LOG_DEBUG("rebuild index, no share table rebuild, no need to wait", K(task_status_));
  } else {
    int64_t finished_task_cnt = 0;
    CheckTaskStatusFn check_task_status_fn(dependent_task_result_map_, finished_task_cnt, child_task_failed, state_finished, dst_tenant_id_);
    if (OB_FAIL(dependent_task_result_map_.foreach_refactored(check_task_status_fn))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("foreach refactored failed", K(ret), K(dst_tenant_id_), K(child_task_failed));
        if (!child_task_failed) {
          LOG_WARN("check status failed, but child_task_failed is false, check reason!", K(ret), K(dst_tenant_id_), K(child_task_failed));
        }
      } else {
        ret = OB_SUCCESS; // reach max dump count
      }
    }
    if (finished_task_cnt == dependent_task_result_map_.size() || OB_FAIL(ret)) {
      // 1. all child tasks finish.
      // 2. the parent task exits if any child task fails.
      state_finished = true;
    }
  }
  #ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_POST_VEC_INDEX_WAIT_AUX_TBL_COMPLEMENT_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] build vec index fail to complement aux table data", K(ret));
    }
  }
  #endif
  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status;
    if (child_task_failed || OB_FAIL(ret)) {
      if (!ObIDDLTask::in_ddl_retry_white_list(ret)) {
        const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
        const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
        (void)switch_status(new_status, false, ret);
        ret = OB_SUCCESS; // allow clean up
      }
    } else if (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_status(next_status))) {
        LOG_WARN("failed to get next status", K(ret));
      } else {
        (void)switch_status(next_status, true, ret);
        LOG_INFO("wait aux table complement finished", K(ret), K(parent_task_id_),
            K(task_id_), K(*this));
      }
    }
  }
  return ret;
}

int ObVecIndexBuildTask::on_child_task_finish(
    const uint64_t child_task_key,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIndexBuildTask has not been inited", K(ret));
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

int ObVecIndexBuildTask::update_index_status_in_schema(
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

int ObVecIndexBuildTask::serialize_params_to_message(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int8_t rowkey_vid_submitted = static_cast<int8_t>(rowkey_vid_task_submitted_);
  int8_t vid_rowkey_submitted = static_cast<int8_t>(vid_rowkey_task_submitted_);
  int8_t delta_buffer_task_submitted = static_cast<int8_t>(delta_buffer_task_submitted_);
  int8_t index_id_task_submitted = static_cast<int8_t>(index_id_task_submitted_);
  int8_t index_snapshot_data_task_submitted = static_cast<int8_t>(index_snapshot_data_task_submitted_);
  int8_t drop_index_submitted = static_cast<int8_t>(drop_index_task_submitted_);
  int8_t is_rebuild_index = static_cast<int8_t>(is_rebuild_index_);

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
                                           rowkey_vid_aux_table_id_))) {
    LOG_WARN("serialize rowkey vec table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           vid_rowkey_aux_table_id_))) {
    LOG_WARN("serialize vid rowkey table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           delta_buffer_table_id_))) {
    LOG_WARN("serialize delta buffer index table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           index_id_table_id_))) {
    LOG_WARN("serialize index id table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode(buf,
                                           buf_len,
                                           pos,
                                           index_snapshot_data_table_id_))) {
    LOG_WARN("serialize snapshot table id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              rowkey_vid_submitted))) {
    LOG_WARN("serialize rowkey vid task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              vid_rowkey_submitted))) {
    LOG_WARN("serialize vid rowkey task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              delta_buffer_task_submitted))) {
    LOG_WARN("serialize delta buf task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              index_id_task_submitted))) {
    LOG_WARN("serialize index id task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              index_snapshot_data_task_submitted))) {
    LOG_WARN("serialize snapshot task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               rowkey_vid_task_id_))) {
    LOG_WARN("serialize rowkey vid task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               vid_rowkey_task_id_))) {
    LOG_WARN("serialize vid rowkey task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               delta_buffer_task_id_))) {
    LOG_WARN("serialize delta buf task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               index_id_task_id_))) {
    LOG_WARN("serialize index id task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               index_snapshot_task_id_))) {
    LOG_WARN("serialize index snapshot task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              drop_index_submitted))) {
    LOG_WARN("serialize drop vec index task submitted failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               drop_index_task_id_))) {
    LOG_WARN("serialize drop index task id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_len,
                                              pos,
                                              is_rebuild_index))) {
    LOG_WARN("serialize drop index task id failed", K(ret));
  }
  return ret;
}

int ObVecIndexBuildTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t rowkey_vid_submitted = 0;
  int8_t vid_rowkey_submitted = 0;
  int8_t delta_buffer_task_submitted = 0;
  int8_t index_id_task_submitted = 0;
  int8_t index_snapshot_data_task_submitted = 0;
  int8_t drop_index_submitted = 0;
  int8_t is_rebuild_index = 0;
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
                                           data_len, pos, rowkey_vid_aux_table_id_))) {
    LOG_WARN("fail to deserialize rowkey vid table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           vid_rowkey_aux_table_id_))) {
    LOG_WARN("fail to deserialize vid rowkey table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           delta_buffer_table_id_))) {
    LOG_WARN("fail to deserialize delta buf index aux table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           index_id_table_id_))) {
    LOG_WARN("fail to deserialize index id table id", K(ret));
  } else if (OB_FAIL(serialization::decode(buf,
                                           data_len,
                                           pos,
                                           index_snapshot_data_table_id_))) {
    LOG_WARN("fail to deserialize snapthot table id", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &rowkey_vid_submitted))) {
    LOG_WARN("fail to deserialize rowkey vid task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &vid_rowkey_submitted))) {
    LOG_WARN("fail to deserialize vid rowkey task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &delta_buffer_task_submitted))) {
    LOG_WARN("fail to deserialize vid index aux task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &index_id_task_submitted))) {
    LOG_WARN("fail to deserialize index id task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &index_snapshot_data_task_submitted))) {
    LOG_WARN("fail to deserialize snapshot task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &rowkey_vid_task_id_))) {
    LOG_WARN("fail to deserialize rowkey vid task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &vid_rowkey_task_id_))) {
    LOG_WARN("fail to deserialize vid rowkey task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &delta_buffer_task_id_))) {
    LOG_WARN("fail to deserialize delta buffer index aux task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &index_id_task_id_))) {
    LOG_WARN("fail to deserialize index id task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &index_snapshot_task_id_))) {
    LOG_WARN("fail to deserialize index sanpshot id task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &drop_index_submitted))) {
    LOG_WARN("fail to deserialize drop vec index task submmitted", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &drop_index_task_id_))) {
    LOG_WARN("fail to deserialize drop vec index task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              data_len,
                                              pos,
                                              &is_rebuild_index))) {
    LOG_WARN("fail to deserialize is_rebuild_index", K(ret));
  } else if (!dependent_task_result_map_.created() &&
             OB_FAIL(dependent_task_result_map_.create(OB_VEC_INDEX_BUILD_CHILD_TASK_NUM,
                                                       lib::ObLabel("DepTasMap")))) {
    LOG_WARN("create dependent task map failed", K(ret));
  } else {
    rowkey_vid_task_submitted_ = rowkey_vid_submitted;
    vid_rowkey_task_submitted_ = vid_rowkey_submitted;
    delta_buffer_task_submitted_ = delta_buffer_task_submitted;
    index_id_task_submitted_ = index_id_task_submitted;
    index_snapshot_data_task_submitted_ = index_snapshot_data_task_submitted;
    drop_index_task_submitted_ = drop_index_submitted;
    is_rebuild_index_ = is_rebuild_index;
    if (rowkey_vid_task_id_ > 0) {
      share::ObDomainDependTaskStatus rowkey_vid_status;
      rowkey_vid_status.task_id_ = rowkey_vid_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(rowkey_vid_aux_table_id_,
                                                            rowkey_vid_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(rowkey_vid_aux_table_id_),
            K(rowkey_vid_status));
      }
    }
    if (OB_SUCC(ret) && vid_rowkey_task_id_ > 0) {
      share::ObDomainDependTaskStatus vid_rowkey_status;
      vid_rowkey_status.task_id_ = vid_rowkey_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(vid_rowkey_aux_table_id_,
                                                            vid_rowkey_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(vid_rowkey_aux_table_id_),
            K(vid_rowkey_status));
      }
    }
    if (OB_SUCC(ret) && delta_buffer_task_id_ > 0) {
      share::ObDomainDependTaskStatus delta_buf_aux_status;
      delta_buf_aux_status.task_id_ = delta_buffer_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(delta_buffer_table_id_,
                                                            delta_buf_aux_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(delta_buffer_table_id_),
            K(delta_buf_aux_status));
      }
    }
    if (OB_SUCC(ret) && index_id_task_id_ > 0) {
      share::ObDomainDependTaskStatus index_id_aux_status;
      index_id_aux_status.task_id_ = index_id_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(index_id_table_id_,
                                                            index_id_aux_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(index_id_table_id_),
            K(index_id_aux_status));
      }
    }
    if (OB_SUCC(ret) && index_snapshot_task_id_ > 0) {
      share::ObDomainDependTaskStatus index_snapshot_aux_status;
      index_snapshot_aux_status.task_id_ = index_snapshot_task_id_;
      if (OB_FAIL(dependent_task_result_map_.set_refactored(index_snapshot_data_table_id_,
                                                            index_snapshot_aux_status))) {
        LOG_WARN("set dependent task map failed", K(ret), K(index_snapshot_data_table_id_),
            K(index_snapshot_aux_status));
      }
    }
  }
  return ret;
}

int64_t ObVecIndexBuildTask::get_serialize_param_size() const
{
  int8_t rowkey_vid_submitted = static_cast<int8_t>(rowkey_vid_task_submitted_);
  int8_t vid_rowkey_submitted = static_cast<int8_t>(vid_rowkey_task_submitted_);
  int8_t delta_buffer_task_submitted = static_cast<int8_t>(delta_buffer_task_submitted_);
  int8_t index_id_task_submitted = static_cast<int8_t>(index_id_task_submitted_);
  int8_t index_snapshot_data_task_submitted = static_cast<int8_t>(index_snapshot_data_task_submitted_);
  int8_t drop_index_submitted = static_cast<int8_t>(drop_index_task_submitted_);
  int8_t is_rebuild_index = static_cast<int8_t>(is_rebuild_index_);
  return create_index_arg_.get_serialize_size()
      + ObDDLTask::get_serialize_param_size()
      + serialization::encoded_length(rowkey_vid_aux_table_id_)
      + serialization::encoded_length(vid_rowkey_aux_table_id_)
      + serialization::encoded_length(delta_buffer_table_id_)
      + serialization::encoded_length(index_id_table_id_)
      + serialization::encoded_length(index_snapshot_data_table_id_)
      + serialization::encoded_length_i8(rowkey_vid_submitted)
      + serialization::encoded_length_i8(vid_rowkey_submitted)
      + serialization::encoded_length_i8(delta_buffer_task_submitted)
      + serialization::encoded_length_i8(index_id_task_submitted)
      + serialization::encoded_length_i8(index_snapshot_data_task_submitted)
      + serialization::encoded_length_i64(rowkey_vid_task_id_)
      + serialization::encoded_length_i64(vid_rowkey_task_id_)
      + serialization::encoded_length_i64(delta_buffer_task_id_)
      + serialization::encoded_length_i64(index_id_task_id_)
      + serialization::encoded_length_i64(index_snapshot_task_id_)
      + serialization::encoded_length_i8(drop_index_submitted)
      + serialization::encoded_length_i64(drop_index_task_id_)
      + serialization::encoded_length_i8(is_rebuild_index);
}

int ObVecIndexBuildTask::print_child_task_ids(char *buf, int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  } else {
    int64_t pos = 0;
    MEMSET(buf, 0, len);
    TCRLockGuard guard(lock_);
    common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus> ::const_iterator iter =
      dependent_task_result_map_.begin();
    if (OB_FAIL(databuff_printf(buf, len, pos, "[ "))) {
      LOG_WARN("failed to print", K(ret));
    } else {
      while (OB_SUCC(ret) && iter != dependent_task_result_map_.end()) {
        const int64_t child_task_id = iter->second.task_id_;
        if (OB_FAIL(databuff_printf(buf,
                                    len,
                                    pos,
                                    "%ld ",
                                    child_task_id))) {
          LOG_WARN("failed to print", K(ret));
        }
        ++iter;
      }
      if (OB_SUCC(ret)) {
        databuff_printf(buf, len, pos, "]");
      }
    }
  }
  return ret;
}

int ObVecIndexBuildTask::collect_longops_stat(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
  databuff_printf(stat_info_.message_, MAX_LONG_OPS_MESSAGE_LENGTH, pos, "TENANT_ID: %ld, TASK_ID: %ld, ", tenant_id_, task_id_);
  switch(status) {
    case ObDDLTaskStatus::PREPARE: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: PREPARE"))) {
        LOG_WARN("failed to print", K(ret), K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_ROWKEY_VID_SCHEMA: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: GENERATE_ROWKEY_VID_SCHEMA"))) {
        LOG_WARN("failed to print", K(ret), K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_ROWKEY_VID_TABLE_COMPLEMENT: {
      char child_task_ids[MAX_LONG_OPS_MESSAGE_LENGTH];
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: WAIT_ROWKEY_VID_TABLE_COMPLEMENT"))) {
        LOG_WARN("failed to print", K(ret), K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      } else if (OB_FAIL(print_child_task_ids(child_task_ids, MAX_LONG_OPS_MESSAGE_LENGTH))) {
        if (ret == OB_SIZE_OVERFLOW) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get all child task ids", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_VEC_AUX_SCHEMA: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: GENERATE_VEC_AUX_SCHEMA"))) {
        LOG_WARN("failed to print", K(ret),  K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_VEC_AUX_TABLE_COMPLEMENT: {
      char child_task_ids[MAX_LONG_OPS_MESSAGE_LENGTH];
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: WAIT_VEC_AUX_TABLE_COMPLEMENT"))) {
        LOG_WARN("failed to print", K(ret),  K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      } else if (OB_FAIL(print_child_task_ids(child_task_ids, MAX_LONG_OPS_MESSAGE_LENGTH))) {
        if (ret == OB_SIZE_OVERFLOW) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get all child task ids", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::GENERATE_VID_ROWKEY_SCHEMA: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: GENERATE_VID_ROWKEY_SCHEMA"))) {
        LOG_WARN("failed to print", K(ret),  K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_VID_ROWKEY_TABLE_COMPLEMENT: {
      char child_task_ids[MAX_LONG_OPS_MESSAGE_LENGTH];
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: WAIT_VID_ROWKEY_TABLE_COMPLEMENT"))) {
        LOG_WARN("failed to print", K(ret), K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      } else if (OB_FAIL(print_child_task_ids(child_task_ids, MAX_LONG_OPS_MESSAGE_LENGTH))) {
        if (ret == OB_SIZE_OVERFLOW) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get all child task ids", K(ret));
        }
      }
      break;
    }
    case ObDDLTaskStatus::VALIDATE_CHECKSUM: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: VALIDATE_CHECKSUM"))) {
        LOG_WARN("failed to print", K(ret), K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      }
      break;
    }
    case ObDDLTaskStatus::FAIL: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: FAIL"))) {
        LOG_WARN("failed to print", K(ret), K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      }
      break;
    }
    case ObDDLTaskStatus::SUCCESS: {
      if (OB_FAIL(databuff_printf(stat_info_.message_,
                                  MAX_LONG_OPS_MESSAGE_LENGTH,
                                  pos,
                                  "STATUS: SUCCESS"))) {
        LOG_WARN("failed to print", K(ret),  K(rowkey_vid_aux_table_id_), K(rowkey_vid_task_submitted_),
                                            K(vid_rowkey_aux_table_id_), K(vid_rowkey_task_submitted_),
                                            K(delta_buffer_table_id_), K(delta_buffer_task_submitted_),
                                            K(index_id_table_id_), K(index_id_task_submitted_),
                                            K(index_snapshot_data_table_id_), K(index_id_task_submitted_));
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K(status), K(*this));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_COLLECT_LONGOPS_STAT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(copy_longops_stat(value))) {
    LOG_WARN("failed to collect common longops stat", K(ret));
  }

  return ret;
}

int ObVecIndexBuildTask::ChangeTaskStatusFn::operator()(common::hash::HashMapPair<uint64_t, share::ObDomainDependTaskStatus> &entry)
{
  int ret = OB_SUCCESS;
  const uint64_t task_key = entry.first;
  const int64_t target_object_id = -1;
  const int64_t child_task_id = entry.second.task_id_;
  if (entry.second.ret_code_ == INT64_MAX) {
    // maybe ddl already finish when switching rs
    HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
      int64_t unused_user_msg_len = 0;
      ObAddr unused_addr;
      if (OB_FAIL(ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                  dest_tenant_id_,
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
          not_finished_cnt_++;
          ObMySQLTransaction trans;
          if (OB_FAIL(trans.start(&rt_service_->get_sql_proxy(),
                                  dest_tenant_id_))) {
            LOG_WARN("start transaction failed", K(ret));
          } else if (OB_FAIL(ObDDLTaskRecordOperator::update_task_status(
                              trans, dest_tenant_id_, child_task_id, ObDDLTaskStatus::FAIL))) {
            LOG_WARN("update child task status failed", K(ret), K(child_task_id));
          } else {
            int tmp_ret = trans.end(true/*commit*/);
            if (OB_SUCCESS != tmp_ret) {
              ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
            }
            LOG_INFO("cancel not finished ddl task", K(dest_tenant_id_),
                K(task_key), K(child_task_id), K(target_object_id));
          }
        } else {
          LOG_WARN("fail to get ddl error message", K(ret), K(task_key),
              K(child_task_id), K(target_object_id));
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuildTask::clean_on_failed()
{
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
    int64_t not_finished_cnt = 0;
    ChangeTaskStatusFn change_statu_fn(dependent_task_result_map_, dst_tenant_id_, root_service_, not_finished_cnt);
    if (OB_FAIL(dependent_task_result_map_.foreach_refactored(change_statu_fn))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("foreach refactored failed", K(ret), K(dst_tenant_id_));
      } else {
        ret = OB_SUCCESS; // reach max dump count
      }
    }
    // 2. drop already built index
    if (OB_FAIL(ret)) {
    } else if (not_finished_cnt > 0) {
      LOG_INFO("child task not finished, not submit drop vec index task.", K(not_finished_cnt));
    } else if (!drop_index_task_submitted_) {
      if (OB_FAIL(submit_drop_vec_index_task())) {
        LOG_WARN("failed to drop vec index", K(ret));
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

int ObVecIndexBuildTask::submit_drop_vec_index_task()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;

  obrpc::ObDropIndexArg drop_index_arg;
  obrpc::ObDropIndexRes drop_index_res;
  ObString index_name;
  ObSqlString drop_index_sql;
  bool is_index_exist = true;
  ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
  bool has_aux_table = (delta_buffer_table_id_ != OB_INVALID_ID);
  uint64_t index_table_id = has_aux_table ? delta_buffer_table_id_ : index_table_id_;
  if (OB_ISNULL(root_service_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
  } else if (OB_INVALID_ID != rowkey_vid_aux_table_id_ &&
             OB_FAIL(drop_index_arg.index_ids_.push_back(rowkey_vid_aux_table_id_))) {
    LOG_WARN("fail to push back rowkey_vid_aux_table_id_", K(ret));
  } else if (OB_INVALID_ID != vid_rowkey_aux_table_id_ &&
             OB_FAIL(drop_index_arg.index_ids_.push_back(vid_rowkey_aux_table_id_))) {
    LOG_WARN("fail to push back vid_rowkey_aux_table_id_", K(ret));
  } else if (OB_INVALID_ID != delta_buffer_table_id_ &&
             OB_FAIL(drop_index_arg.index_ids_.push_back(delta_buffer_table_id_))) {
    LOG_WARN("fail to push back delta_buffer_table_id_", K(ret), K(delta_buffer_table_id_));
  } else if (OB_INVALID_ID != index_id_table_id_ &&
             OB_FAIL(drop_index_arg.index_ids_.push_back(index_id_table_id_))) {
    LOG_WARN("fail to push back index_id_table_id_", K(ret), K(index_id_table_id_));
  } else if (OB_INVALID_ID != index_snapshot_data_table_id_ &&
             OB_FAIL(drop_index_arg.index_ids_.push_back(index_snapshot_data_table_id_))) {
    LOG_WARN("fail to push back index_snapshot_data_table_id_", K(ret));
  } else if (drop_index_arg.index_ids_.count() <= 0) {
    LOG_INFO("no table need to be drop, skip", K(ret)); // no table exist, skip drop
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(object_id_));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_table_schema is null", K(ret), KP(data_table_schema));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, data_table_schema->get_database_id(), database_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(data_table_schema->get_database_id()));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database_schema is null", KR(ret), KP(database_schema));
  } else {
    int64_t ddl_rpc_timeout = 0;
    drop_index_arg.is_inner_          = true;
    drop_index_arg.tenant_id_         = tenant_id_;
    drop_index_arg.exec_tenant_id_    = tenant_id_;
    drop_index_arg.index_table_id_    = index_table_id;
    drop_index_arg.index_name_        = data_table_schema->get_table_name();  // not in used
    drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
    drop_index_arg.is_add_to_scheduler_ = true;
    drop_index_arg.task_id_           = task_id_; // parent task
    drop_index_arg.session_id_        = data_table_schema->get_session_id();
    drop_index_arg.table_name_        = data_table_schema->get_table_name();
    drop_index_arg.database_name_     = database_schema->get_database_name_str();
    drop_index_arg.is_vec_inner_drop_ = true;  // if want to drop only one index, is_vec_inner_drop_ should be false, else should be true.
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(data_table_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout))) {
      LOG_WARN("failed to get ddl rpc timeout", KR(ret));
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DROP_INDEX_RPC_FAILED))) {
      LOG_WARN("ddl sim failure", KR(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout).drop_index_on_failed(drop_index_arg, drop_index_res))) {
      LOG_WARN("drop index failed", KR(ret), K(ddl_rpc_timeout));
    } else {
      drop_index_task_submitted_ = true;
      drop_index_task_id_ = drop_index_res.task_id_;
      LOG_INFO("success submit drop vec index task", K(ret), K(drop_index_task_id_));
    }
  }
  return ret;
}

int ObVecIndexBuildTask::wait_drop_index_finish(bool &is_finish)
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

int ObVecIndexBuildTask::succ()
{
  return cleanup();
}

int ObVecIndexBuildTask::validate_checksum()
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
    // TODO @wuxingying: validate checksum, set next status to FAIL if validation failed
    if (OB_SUCC(ret)) {
      state_finished = true;
    }
  }
  #ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_POST_VEC_INDEX_CHECKSUM_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] build vec index fail to checksum", K(ret));
    }
  }
  #endif
  if (state_finished && OB_SUCC(ret)) {
    ObDDLTaskStatus next_status;
    if (OB_FAIL(get_next_status(next_status))) {
      LOG_WARN("failed to get next status", K(ret));
    } else {
      (void)switch_status(next_status, true, ret);
      LOG_INFO("validate checksum finished", K(ret), K(parent_task_id_),
          K(task_id_), K(*this));
    }
  } else if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
    (void)switch_status(ObDDLTaskStatus::FAIL, false, ret);  // allow clean up
    LOG_INFO("prepare failed", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexBuildTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else {
    const uint64_t data_table_id = object_id_;
    const uint64_t index_table_id = index_table_id_;
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
      LOG_WARN("fail to get table schema", K(ret), KP(data_schema));
    } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), dst_tenant_id_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                   task_id_))) {
      LOG_WARN("failed to get owner id", K(ret), K(task_id_));
    } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(*data_schema,
                                                            index_table_id,
                                                            false,
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

  if (OB_FAIL(ret)) {
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

int ObVecIndexBuildTask::update_task_message(common::ObISQLClient &proxy)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString msg;
  common::ObArenaAllocator allocator("ObVecIndexBuild");
  const int64_t serialize_param_size = get_serialize_param_size();

  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_param_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(serialize_param_size));
  } else if (OB_FAIL(serialize_params_to_message(buf, serialize_param_size, pos))) {
    LOG_WARN("failed to serialize params to message", KR(ret));
  } else {
    msg.assign(buf, serialize_param_size);
    if (OB_FAIL(ObDDLTaskRecordOperator::update_message(proxy, tenant_id_, task_id_, msg))) {
      LOG_WARN("failed to update message", KR(ret));
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
