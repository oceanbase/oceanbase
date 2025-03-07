/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "rootserver/ddl_task/ob_drop_vec_ivf_index_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "rootserver/ob_root_service.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/ob_ddl_sim_point.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{

ObDropVecIVFIndexTask::ObDropVecIVFIndexTask()
  : ObDDLTask(DDL_DROP_VEC_IVFFLAT_INDEX),
    root_service_(nullptr),
    centroid_(),
    cid_vector_(),
    rowkey_cid_(), // delta_buffer_table
    sq_meta_(),
    pq_centroid_(),
    pq_code_(),
    drop_index_arg_()
{
}

ObDropVecIVFIndexTask::~ObDropVecIVFIndexTask()
{
}

int ObDropVecIVFIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t data_table_id,
    const ObDDLType task_type,
    const ObVecIndexDDLChildTaskInfo &centroid,       // domain table
    const ObVecIndexDDLChildTaskInfo &cid_vector,
    const ObVecIndexDDLChildTaskInfo &rowkey_cid,
    const ObVecIndexDDLChildTaskInfo &sq_meta,
    const ObVecIndexDDLChildTaskInfo &pq_centroid,
    const ObVecIndexDDLChildTaskInfo &pq_code,
    const int64_t schema_version,
    const int64_t consumer_group_id,
    const uint64_t tenant_data_version,
    const obrpc::ObDropIndexArg &drop_index_arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_format_version = tenant_data_version;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
               || task_id <= 0
               || OB_INVALID_ID == data_table_id
               || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id), K(centroid),
        K(cid_vector), K(rowkey_cid), K(sq_meta), K(pq_centroid), K(pq_code), K(schema_version));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, drop_index_arg, drop_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", K(ret));
  } else if (OB_FAIL(centroid_.deep_copy_from_other(centroid, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(centroid));
  } else if (OB_FAIL(cid_vector_.deep_copy_from_other(cid_vector, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(cid_vector));
  } else if (OB_FAIL(rowkey_cid_.deep_copy_from_other(rowkey_cid, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(rowkey_cid));
  } else if (OB_FAIL(sq_meta_.deep_copy_from_other(sq_meta, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(sq_meta));
  } else if (OB_FAIL(pq_centroid_.deep_copy_from_other(pq_centroid, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(pq_centroid));
  } else if (OB_FAIL(pq_code_.deep_copy_from_other(pq_code, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(pq_code));
  } else if (tenant_data_format_version <= 0 && OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_format_version))) {
    LOG_WARN("get min data version failed", K(ret), K(tenant_id));
  } else {
    // get valid object id, target_object_id_ // not use this id
    if (centroid_.is_valid()) {
      target_object_id_ = centroid_.table_id_;
    } else if (cid_vector_.is_valid()) {
      target_object_id_ = cid_vector_.table_id_;
    } else if (rowkey_cid_.is_valid()) {
      target_object_id_ = rowkey_cid_.table_id_;
    } else if (sq_meta_.is_valid()) {
      target_object_id_ = sq_meta_.table_id_;
    } else if (pq_centroid_.is_valid()) {
      target_object_id_ = pq_centroid_.table_id_;
    } else if (pq_code_.is_valid()) {
      target_object_id_ = pq_code_.table_id_;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid object id", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      task_type_ = task_type;
      set_gmt_create(ObTimeUtility::current_time());
      tenant_id_ = tenant_id;
      object_id_ = data_table_id;
      schema_version_ = schema_version;
      task_id_ = task_id;
      parent_task_id_ = 0; // no parent task
      consumer_group_id_ = consumer_group_id;
      task_version_ = OB_DROP_VEC_IVF_INDEX_TASK_VERSION;
      dst_tenant_id_ = tenant_id;
      dst_schema_version_ = schema_version;
      is_inited_ = true;
      data_format_version_ = tenant_data_format_version;
      execution_id_ = 1L;
    }
  }
  return ret;
}

int ObDropVecIVFIndexTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret));
  } else {
    task_type_ = task_record.ddl_type_;
    tenant_id_ = task_record.tenant_id_;
    object_id_ = task_record.object_id_;
    target_object_id_ = task_record.target_object_id_;
    schema_version_ = task_record.schema_version_;
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    task_version_ = task_record.task_version_;
    ret_code_ = task_record.ret_code_;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    execution_id_ = task_record.execution_id_;
    snapshot_version_ = task_record.snapshot_version_;
    pos = 0;
    if (OB_ISNULL(task_record.message_.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, task record message is nullptr", K(ret), K(task_record));
    } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(),
            task_record.message_.length(), pos))) {
      LOG_WARN("deserialize params from message failed", K(ret));
    } else {
      is_inited_ = true;
      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObDropVecIVFIndexTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIVFIndexTask has not been inited", K(ret));
  } else if (!need_retry()) {
    // task is done
  } else if (OB_FAIL(check_switch_succ())) {
    LOG_WARN("check need retry failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(ObDDLTaskStatus::DROP_AUX_INDEX_TABLE))) {
          LOG_WARN("fail to prepare", K(ret));
        }
        break;
      case ObDDLTaskStatus::DROP_AUX_INDEX_TABLE:
        if (OB_FAIL(drop_aux_index_table(WAIT_CHILD_TASK_FINISH))) {
          LOG_WARN("fail to prepare", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH:
        if (OB_FAIL(wait_drop_task_finish(SUCCESS))) {
          LOG_WARN("fail to check and wait task", K(ret));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(succ())) {
          LOG_WARN("do succ procedure failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(fail())) {
          LOG_WARN("do fail procedure failed", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, task status is not valid", K(ret), K(task_status_));
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

int ObDropVecIVFIndexTask::deep_copy_index_arg(common::ObIAllocator &allocator,
                                            const obrpc::ObDropIndexArg &src_index_arg,
                                            obrpc::ObDropIndexArg &dst_index_arg)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  const int64_t serialize_size = src_index_arg.get_serialize_size();
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(serialize_size));
  } else if (OB_FAIL(src_index_arg.serialize(buf, serialize_size, pos))) {
    LOG_WARN("serialize source index arg failed", K(ret));
  } else if (OB_FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(dst_index_arg.deserialize(buf, serialize_size, pos))) {
    LOG_WARN("deserialize failed", K(ret));
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    allocator.free(buf);
  }

  return ret;
}

int ObDropVecIVFIndexTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::serialize", K(ret));
  } else if (OB_FAIL(centroid_.serialize(buf, buf_size, pos))) {    // domain index
    LOG_WARN("fail to serialize centroid_ table info", K(ret), K(centroid_));
  } else if (OB_FAIL(cid_vector_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize cid_vector_ table info", K(ret), K(cid_vector_));
  } else if (OB_FAIL(rowkey_cid_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize rowkey_cid table info", K(ret), K(rowkey_cid_));
  } else if (OB_FAIL(sq_meta_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize sq_meta table info", K(ret), K(sq_meta_));
  } else if (OB_FAIL(pq_centroid_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize pq_centroid_ table info", K(ret), K(pq_centroid_));
  } else if (OB_FAIL(pq_code_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize pq_code_ table info", K(ret), K(pq_code_));
  } else if (OB_FAIL(drop_index_arg_.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize failed", K(ret));
  }
  return ret;
}

int ObDropVecIVFIndexTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObDropIndexArg tmp_drop_index_arg;
  ObVecIndexDDLChildTaskInfo tmp_info;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::deserialize", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize rowkey vid table info", K(ret));
  } else if (OB_FAIL(centroid_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize vid rowkey table info", K(ret));
  } else if (OB_FAIL(cid_vector_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize index id table info", K(ret));
  } else if (OB_FAIL(rowkey_cid_.deep_copy_from_other(tmp_info, allocator_))) { // delta_buffer_table
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize delta buffer table info", K(ret));
  } else if (OB_FAIL(sq_meta_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize index snapshot data table info", K(ret));
  } else if (OB_FAIL(pq_centroid_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize index snapshot data table info", K(ret));
  } else if (OB_FAIL(pq_code_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_drop_index_arg.deserialize(buf, buf_size, pos))) {
    LOG_WARN("deserialize failed", K(ret));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, tmp_drop_index_arg, drop_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", K(ret));
  }
  return ret;
}

int64_t ObDropVecIVFIndexTask::get_serialize_param_size() const
{
  return ObDDLTask::get_serialize_param_size()
       + centroid_.get_serialize_size()       // domain table
       + cid_vector_.get_serialize_size()
       + rowkey_cid_.get_serialize_size()
       + sq_meta_.get_serialize_size()
       + pq_centroid_.get_serialize_size()
       + pq_code_.get_serialize_size()
       + drop_index_arg_.get_serialize_size();
}

// TODO@xiajin : update task_id_ in RS drop_index func
int ObDropVecIVFIndexTask::update_task_message()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString msg;
  common::ObArenaAllocator allocator("ObVecReBuild");
  const int64_t serialize_param_size = get_serialize_param_size();

  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_param_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(serialize_param_size));
  } else if (OB_FAIL(serialize_params_to_message(buf, serialize_param_size, pos))) {
    LOG_WARN("failed to serialize params to message", KR(ret));
  } else {
    msg.assign(buf, serialize_param_size);
    if (OB_FAIL(ObDDLTaskRecordOperator::update_message(root_service_->get_sql_proxy(), tenant_id_, task_id_, msg))) {
      LOG_WARN("failed to update message", KR(ret));
    }
  }
  return ret;
}

int ObDropVecIVFIndexTask::check_switch_succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_centroid_exist = false; //
  bool is_cid_vector_exist = false;
  bool is_rowkey_cid_exist = false;
  bool is_sq_meta_exist = false;
  bool is_pq_centroid_exist = false;
  bool is_pq_code_exist = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hasn't initialized", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id_));
  } else if (centroid_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, centroid_.table_id_, is_centroid_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(centroid_));
  } else if (cid_vector_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, cid_vector_.table_id_, is_cid_vector_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(cid_vector_));
  } else if (rowkey_cid_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, rowkey_cid_.table_id_, is_rowkey_cid_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(rowkey_cid_));
  } else if (sq_meta_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, sq_meta_.table_id_, is_sq_meta_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(sq_meta_));
  } else if (pq_centroid_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, pq_centroid_.table_id_, is_pq_centroid_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(pq_centroid_));
  } else if (pq_code_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, pq_code_.table_id_, is_pq_code_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(pq_code_));
  } else {
    is_centroid_exist = centroid_.is_valid() ? is_centroid_exist : false;
    is_cid_vector_exist = cid_vector_.is_valid() ? is_cid_vector_exist : false;
    is_rowkey_cid_exist = rowkey_cid_.is_valid() ? is_rowkey_cid_exist : false;
    is_sq_meta_exist = sq_meta_.is_valid() ? is_sq_meta_exist : false;
    is_pq_centroid_exist = pq_centroid_.is_valid() ? is_pq_centroid_exist : false;
    is_pq_code_exist = pq_code_.is_valid() ? is_pq_code_exist : false;

    if (!is_centroid_exist &&
        !is_cid_vector_exist &&
        !is_rowkey_cid_exist &&
        !is_sq_meta_exist &&
        !is_pq_centroid_exist &&
        !is_pq_code_exist) {
      task_status_ = ObDDLTaskStatus::SUCCESS;
    }
  }
  return ret;
}

/*
  create drop share vector index table task and wait task
*/
int ObDropVecIVFIndexTask::prepare(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  DEBUG_SYNC(DROP_VECTOR_INDEX_PREPARE_STATUS);
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
    if (OB_FAIL(switch_status(new_status, true, ret))) {
      LOG_WARN("switch status failed", K(ret), K(new_status), K(task_status_));
    } else {
      LOG_INFO("prepare success", K(ret), K(parent_task_id_), K(task_id_), K(*this));
    }
  }
  return ret;
}

int ObDropVecIVFIndexTask::drop_aux_index_table(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIVFIndexTask has not been inited", K(ret));
  } else if (ObDDLType::DDL_DROP_VEC_IVFFLAT_INDEX == task_type_) {
    if (OB_FAIL(drop_aux_ivfflat_index_table(new_status))) {
      LOG_WARN("fail to drop ivfflat index table", K(ret));
    }
  } else if (ObDDLType::DDL_DROP_VEC_IVFSQ8_INDEX == task_type_) {
    if (OB_FAIL(drop_aux_ivfsq8_index_table(new_status))) {
      LOG_WARN("fail to drop ivfsq8 index table", K(ret));
    }
  } else if (ObDDLType::DDL_DROP_VEC_IVFPQ_INDEX == task_type_) {
    if (OB_FAIL(drop_aux_ivfpq_index_table(new_status))) {
      LOG_WARN("fail to drop ivfpq index table", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl task type", K(ret), K(task_type_));
  }
  return ret;
}

int ObDropVecIVFIndexTask::drop_aux_ivfflat_index_table(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIVFIndexTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (0 == centroid_.task_id_ && centroid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, centroid_.table_id_, centroid_.index_name_, centroid_.task_id_, true/* is_domain_index */))) {
      LOG_WARN("fail to create drop index task", K(ret), K(centroid_));
  } else if (0 == cid_vector_.task_id_  && cid_vector_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, cid_vector_.table_id_, cid_vector_.index_name_, cid_vector_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(cid_vector_));
  } else if (0 == rowkey_cid_.task_id_ && rowkey_cid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, rowkey_cid_.table_id_, rowkey_cid_.index_name_, rowkey_cid_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(rowkey_cid_));
  } else if (OB_FAIL(update_task_message())) {
    LOG_WARN("fail to update task message to __all_ddl_task_status", K(ret), K(centroid_), K(cid_vector_), K(rowkey_cid_));
  }
  return ret;
}

int ObDropVecIVFIndexTask::drop_aux_ivfsq8_index_table(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIVFIndexTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (0 == centroid_.task_id_ && centroid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, centroid_.table_id_, centroid_.index_name_, centroid_.task_id_, true/* is_domain_index */))) {
      LOG_WARN("fail to create drop index task", K(ret), K(centroid_));
  } else if (0 == cid_vector_.task_id_  && cid_vector_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, cid_vector_.table_id_, cid_vector_.index_name_, cid_vector_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(cid_vector_));
  } else if (0 == rowkey_cid_.task_id_ && rowkey_cid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, rowkey_cid_.table_id_, rowkey_cid_.index_name_, rowkey_cid_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(rowkey_cid_));
  } else if (0 == sq_meta_.task_id_ && sq_meta_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, sq_meta_.table_id_, sq_meta_.index_name_, sq_meta_.task_id_))) {
     LOG_WARN("fail to create drop index task", K(ret), K(sq_meta_));
  } else if (OB_FAIL(update_task_message())) {
    LOG_WARN("fail to update task message to __all_ddl_task_status", K(ret), K(centroid_), K(cid_vector_), K(rowkey_cid_), K(sq_meta_));
  }
  return ret;
}

int ObDropVecIVFIndexTask::drop_aux_ivfpq_index_table(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIVFIndexTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (0 == centroid_.task_id_ && centroid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, centroid_.table_id_, centroid_.index_name_, centroid_.task_id_, true/* is_domain_index */))) {
      LOG_WARN("fail to create drop index task", K(ret), K(centroid_));
  } else if (0 == pq_centroid_.task_id_  && pq_centroid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, pq_centroid_.table_id_, pq_centroid_.index_name_, pq_centroid_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(pq_centroid_));
  } else if (0 == pq_code_.task_id_ && pq_code_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, pq_code_.table_id_, pq_code_.index_name_, pq_code_.task_id_))) {
     LOG_WARN("fail to create drop index task", K(ret), K(pq_code_));
  } else if (0 == rowkey_cid_.task_id_ && rowkey_cid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, rowkey_cid_.table_id_, rowkey_cid_.index_name_, rowkey_cid_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(rowkey_cid_));
  } else if (OB_FAIL(update_task_message())) {
    LOG_WARN("fail to update task message to __all_ddl_task_status", K(ret), K(centroid_), K(pq_centroid_), K(rowkey_cid_), K(pq_code_));
  }
  return ret;
}


int ObDropVecIVFIndexTask::wait_drop_task_finish(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  ObSEArray<ObVecIndexDDLChildTaskInfo, 2> vec_child_tasks;
  if (task_type_ == ObDDLType::DDL_DROP_VEC_IVFFLAT_INDEX) {
    if (centroid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(centroid_))) {
      LOG_WARN("fail to push back ivfflat centroid task", K(ret));
    } else if (cid_vector_.is_valid() && OB_FAIL(vec_child_tasks.push_back(cid_vector_))) {
      LOG_WARN("fail to push back ivfflat cid vector task", K(ret));
    } else if (rowkey_cid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(rowkey_cid_))) {
      LOG_WARN("fail to push back ivfflat centroid task", K(ret));
    }
  } else if (task_type_ == ObDDLType::DDL_DROP_VEC_IVFSQ8_INDEX) {
    if (centroid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(centroid_))) {
      LOG_WARN("fail to push back ivfsq8 centroid task", K(ret));
    } else if (cid_vector_.is_valid() && OB_FAIL(vec_child_tasks.push_back(cid_vector_))) {
      LOG_WARN("fail to push back ivfsq8 cid vector task", K(ret));
    } else if (rowkey_cid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(rowkey_cid_))) {
      LOG_WARN("fail to push back ivfsq8 rowkey_cid task", K(ret));
    } else if (sq_meta_.is_valid() && OB_FAIL(vec_child_tasks.push_back(sq_meta_))) {
      LOG_WARN("fail to push back ivfsq8 sq_meta task", K(ret));
    }
  } else if (task_type_ == ObDDLType::DDL_DROP_VEC_IVFPQ_INDEX) {
    if (centroid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(centroid_))) {
      LOG_WARN("fail to push back ivfpq centroid task", K(ret));
    } else if (pq_centroid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(pq_centroid_))) {
      LOG_WARN("fail to push back ivfpq pq_centroid task", K(ret));
    } else if (pq_code_.is_valid() && OB_FAIL(vec_child_tasks.push_back(pq_code_))) {
      LOG_WARN("fail to push back ivfpq pq_code task", K(ret));
    } else if (rowkey_cid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(rowkey_cid_))) {
      LOG_WARN("fail to push back ivfpq rowkey_cid task", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(wait_child_task_finish(vec_child_tasks, has_finished))) {
    LOG_WARN("fail to wait child task finish", K(ret), K(vec_child_tasks));
  } else if (has_finished) {
    // overwrite return code
    if (OB_FAIL(switch_status(new_status, true/*enable_flt*/, ret))) {
      LOG_WARN("fail to switch status", K(ret), K(new_status));
    } else {
      LOG_INFO("wait_drop_task_finish success", K(ret), K(task_type_));
    }
  }
  return ret;
}

int ObDropVecIVFIndexTask::check_drop_index_finish(
    const uint64_t tenant_id,
    const int64_t task_id,
    const int64_t table_id,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  const ObAddr unused_addr;
  int64_t unused_user_msg_len = 0;
  share::ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  has_finished = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(table_id));
  } else if (OB_FAIL(share::ObDDLErrorMessageTableOperator::get_ddl_error_message(
                                                       tenant_id,
                                                       task_id,
                                                       -1/*target_object_id*/,
                                                       table_id,
                                                       *GCTX.sql_proxy_,
                                                       error_message,
                                                       unused_user_msg_len))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("ddl task not finish", K(ret), K(tenant_id),  K(task_id), K(table_id));
    } else {
      LOG_WARN("fail to get ddl error message", K(ret), K(tenant_id), K(task_id), K(table_id));
    }
  } else {
    ret = error_message.ret_code_;
    has_finished = true;
  }
  LOG_INFO("wait build index finish", K(ret), K(tenant_id), K(task_id), K(table_id), K(has_finished));
  return ret;
}

int ObDropVecIVFIndexTask::wait_child_task_finish(
    const common::ObIArray<ObVecIndexDDLChildTaskInfo> &child_task_ids,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  if (0 == child_task_ids.count()) {
    has_finished = true;
  } else {
    bool finished = true;
    for (int64_t i = 0; OB_SUCC(ret) && finished && i < child_task_ids.count(); ++i) {
      const ObVecIndexDDLChildTaskInfo &task_info = child_task_ids.at(i);
      finished = false;
      if (-1 == task_info.task_id_) {
        finished = true;
      } else if (OB_FAIL(check_drop_index_finish(tenant_id_, task_info.task_id_, task_info.table_id_, finished))) {
        LOG_WARN("fail to check vec index child task finish", K(ret));
      } else if (!finished) { // nothing to do
        LOG_INFO("child task hasn't been finished", K(tenant_id_), K(task_info));
      }
    }
    if (OB_SUCC(ret) && finished) {
      has_finished = true;
    }
  }
  return ret;
}

int ObDropVecIVFIndexTask::create_drop_index_task(
    share::schema::ObSchemaGetterGuard &guard,
    const uint64_t index_tid,
    const common::ObString &index_name,
    int64_t &task_id,
    const bool is_domain_index)
{
  int ret = OB_SUCCESS;
  ObSqlString drop_index_sql;
  const ObTableSchema *index_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  bool is_index_exist = false;
  if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == index_tid || index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_tid), K(index_name));
  } else if (OB_FAIL(guard.check_table_exist(tenant_id_, index_tid, is_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(index_tid));
  } else if (!is_index_exist) {
    // nothing to do, just by pass.
    task_id = -1;
  } else if (OB_FAIL(guard.get_table_schema(tenant_id_, index_tid, index_schema))) {
    LOG_WARN("fail to get index table schema", K(ret), K(tenant_id_), K(index_tid));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, index schema is nullptr", K(ret), KP(index_schema));
  } else if (OB_FAIL(guard.get_database_schema(tenant_id_, index_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(index_schema->get_database_id()));
  } else if (OB_FAIL(guard.get_table_schema(tenant_id_, index_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(index_schema->get_data_table_id()));
  } else if (OB_UNLIKELY(nullptr == database_schema || nullptr == data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema is nullptr", K(ret), KP(database_schema), KP(data_table_schema));
  } else if (is_domain_index && OB_FAIL(drop_index_sql.assign(drop_index_arg_.ddl_stmt_str_))) {
    LOG_WARN("assign user drop index sql failed", K(ret));
  } else {
    int64_t ddl_rpc_timeout_us = 0;
    obrpc::ObDropIndexArg arg;
    obrpc::ObDropIndexRes res;
    arg.is_inner_            = true;
    arg.tenant_id_           = tenant_id_;
    arg.exec_tenant_id_      = tenant_id_;
    arg.index_table_id_      = index_tid;
    arg.session_id_          = data_table_schema->get_session_id();
    arg.index_name_          = index_name;
    arg.table_name_          = data_table_schema->get_table_name();
    arg.database_name_       = database_schema->get_database_name_str();
    arg.index_action_type_   = obrpc::ObIndexArg::DROP_INDEX;
    arg.ddl_stmt_str_        = drop_index_sql.string();
    arg.is_add_to_scheduler_ = true;
    arg.task_id_             = task_id_;
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
            index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout_us))) {
      LOG_WARN("fail to get ddl rpc timeout", K(ret));
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout_us).drop_index(arg, res))) {
      LOG_WARN("fail to drop index", K(ret), K(ddl_rpc_timeout_us), K(arg), K(res.task_id_));
    } else {
      task_id = res.task_id_;
    }
    LOG_INFO("drop index", K(ret), K(index_tid), K(index_name), K(task_id),
        "data table name", data_table_schema->get_table_name_str(),
        "database name", database_schema->get_database_name_str());
  }
  return ret;
}

int ObDropVecIVFIndexTask::succ()
{
  return cleanup();
}

int ObDropVecIVFIndexTask::fail()
{
  return cleanup();
}

int ObDropVecIVFIndexTask::cleanup_impl()
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
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", K(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
