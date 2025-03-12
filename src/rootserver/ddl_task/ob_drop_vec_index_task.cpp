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

#include "rootserver/ddl_task/ob_drop_vec_index_task.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{

ObDropVecIndexTask::ObDropVecIndexTask()
  : ObDDLTask(DDL_DROP_VEC_INDEX),
    root_service_(nullptr),
    rowkey_vid_(),
    vid_rowkey_(),
    domain_index_(), // delta_buffer_table
    vec_index_id_(),
    vec_index_snapshot_data_(),
    drop_index_arg_(),
    replica_builder_(),
    check_dag_exit_tablets_map_(),
    wait_trans_ctx_(),
    delte_lob_meta_request_time_(0),
    delte_lob_meta_job_ret_code_(INT64_MAX),
    check_dag_exit_retry_cnt_(0),
    del_lob_meta_row_task_submitted_(false)
{
}

ObDropVecIndexTask::~ObDropVecIndexTask()
{
}

int ObDropVecIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t data_table_id,
    const ObDDLType ddl_type,
    const ObVecIndexDDLChildTaskInfo &rowkey_vid,
    const ObVecIndexDDLChildTaskInfo &vid_rowkey,
    const ObVecIndexDDLChildTaskInfo &domain_index,  // delta_buffer_table
    const ObVecIndexDDLChildTaskInfo &vec_delta_buffer,
    const ObVecIndexDDLChildTaskInfo &vec_index_snapshot_data,
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
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id), K(rowkey_vid),
        K(vid_rowkey), K(domain_index), K(vec_delta_buffer), K(vec_index_snapshot_data), K(schema_version));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, drop_index_arg, drop_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", K(ret));
  } else if (OB_FAIL(rowkey_vid_.deep_copy_from_other(rowkey_vid, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(rowkey_vid));
  } else if (OB_FAIL(vid_rowkey_.deep_copy_from_other(vid_rowkey, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(vid_rowkey));
  } else if (OB_FAIL(domain_index_.deep_copy_from_other(domain_index, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(domain_index));
  } else if (OB_FAIL(vec_index_id_.deep_copy_from_other(vec_delta_buffer, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(vec_delta_buffer));
  } else if (OB_FAIL(vec_index_snapshot_data_.deep_copy_from_other(vec_index_snapshot_data, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(vec_index_snapshot_data));
  } else if (tenant_data_format_version <= 0 && OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_format_version))) {
    LOG_WARN("get min data version failed", K(ret), K(tenant_id));
  } else {
    // get valid object id, target_object_id_ // not use this id
    if (domain_index_.is_valid()) {
      target_object_id_ = domain_index_.table_id_;
    } else if (rowkey_vid_.is_valid()) {
      target_object_id_ = rowkey_vid_.table_id_;
    } else if (vec_index_id_.is_valid()) {
      target_object_id_ = vec_index_id_.table_id_;
    } else if (vid_rowkey_.is_valid()) {
      target_object_id_ = vid_rowkey_.table_id_;
    } else if (vec_index_snapshot_data_.is_valid()) {
      target_object_id_ = vec_index_snapshot_data_.table_id_;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid object id", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      task_type_ = DDL_DROP_VEC_INDEX;
      set_gmt_create(ObTimeUtility::current_time());
      tenant_id_ = tenant_id;
      object_id_ = data_table_id;
      schema_version_ = schema_version;
      task_id_ = task_id;
      parent_task_id_ = 0; // no parent task
      consumer_group_id_ = consumer_group_id;
      task_version_ = OB_DROP_VEC_INDEX_TASK_VERSION;
      dst_tenant_id_ = tenant_id;
      dst_schema_version_ = schema_version;
      is_inited_ = true;
      data_format_version_ = tenant_data_format_version;
      execution_id_ = 1L;
    }
  }
  return ret;
}

int ObDropVecIndexTask::init(const ObDDLTaskRecord &task_record)
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

// to hold snapshot, containing data in old table with new schema version.
int ObDropVecIndexTask::obtain_snapshot(const share::ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  ObDDLTaskStatus old_status = task_status_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  }  else if (!vec_index_snapshot_data_.is_valid()) {
    // do not need snapshot and delete lob meta row(when snapshot table is not built)
    state_finished = true;
    if (OB_FAIL(switch_status(ObDDLTaskStatus::DROP_AUX_INDEX_TABLE, true, ret))) {
      LOG_WARN("fail to switch task status to ObDDLTaskStatus::DROP_AUX_INDEX_TABLE", K(ret));
    }
  } else if (snapshot_version_ > 0) {
    // already hold snapshot, switch to next status
    state_finished = true;
    if (OB_FAIL(switch_status(next_task_status, true, ret))) {
      LOG_WARN("fail to switch task status", K(ret), K(next_task_status));
    }
  } else if (OB_FAIL(ObDDLUtil::obtain_snapshot(next_task_status, vec_index_snapshot_data_.table_id_,
                                                vec_index_snapshot_data_.table_id_, snapshot_version_,
                                                this))) {
    LOG_WARN("fail to obtain_snapshot", K(ret), K(snapshot_version_));
  } else {
    state_finished = true;
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_VEC_INDEX_OBTAIN_SNAPSHOT_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] fail to obtain snapshot", K(ret));
    }
  }
#endif
  if (state_finished && OB_SUCC(ret)) {
    LOG_INFO("success to obtain_snapshot", K(ret));
  } else if (next_task_status == task_status_) {  // resume old task status and retry
    if (OB_FAIL(switch_status(old_status, true, ret))) {
      LOG_WARN("fail to switch status", K(ret), K(old_status), K(task_status_));
    } else {
      LOG_INFO("resume obtain_snapshot success", K(ret), K(old_status), K(task_status_), K(task_id_));
    }
  }
  return ret;
}

int ObDropVecIndexTask::drop_lob_meta_row(const ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool is_build_replica_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIndexTask is not inited", K(ret));
  } else if (ObDDLTaskStatus::DROP_LOB_META_ROW != task_status_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_UNLIKELY(snapshot_version_ <= 0)) {
    is_build_replica_end = true; // switch to fail.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected snapshot", K(ret), KPC(this));
  } else if (vec_index_snapshot_data_.is_valid() && !del_lob_meta_row_task_submitted_ && OB_FAIL(send_build_single_replica_request())) {
    LOG_WARN("fail to send build single replica request", K(ret));
  } else if (vec_index_snapshot_data_.is_valid() && del_lob_meta_row_task_submitted_ && OB_FAIL(check_build_single_replica(is_build_replica_end))) {
    LOG_WARN("fail to check build single replica", K(ret), K(is_build_replica_end));
  } else if (!vec_index_snapshot_data_.is_valid()) {
    is_build_replica_end = true;
  }
  if (is_build_replica_end) {
    ret = OB_SUCC(ret) ? delte_lob_meta_job_ret_code_ : ret;
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_VEC_INDEX_DROP_LOB_META_ROW_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] fail to drop lob meta row", K(ret));
    }
  }
#endif
    if (OB_FAIL(ret)) {
      LOG_WARN("fail in delete lob meta row", K(ret));
    } else if (OB_FAIL(finish())) {
      LOG_WARN("fail in release snapshot", K(ret));
    } else if (OB_FAIL(switch_status(next_task_status, true/*enable_flt*/, ret))) {
      LOG_WARN("fail to switch task status", K(ret), K(next_task_status));
    } else {
      LOG_INFO("drop_lob_meta_row success", K(ret));
    }
  }
  return ret;
}

int ObDropVecIndexTask::wait_trans_end(ObDDLTaskStatus next_task_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  ObDDLTaskStatus old_status = task_status_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::WAIT_TRANS_END != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(ObDDLTask::wait_trans_end(wait_trans_ctx_, next_task_status))) {
    LOG_WARN("fail to wait trans end", K(ret));
  } else {
    state_finished = true;
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_VEC_INDEX_WAIT_TRANS_END_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[ERRSIM] fail to wait trans end", K(ret));
    }
  }
#endif
  if (state_finished && OB_SUCC(ret)) {
    LOG_INFO("success to wait trans end", K(ret));
  } else if (next_task_status == task_status_) {  // resume old task status and retry
    if (OB_FAIL(switch_status(old_status, true, ret))) {
      LOG_WARN("fail to switch status", K(ret), K(old_status), K(task_status_));
    } else {
      LOG_INFO("resume wait_trans_end old status success", K(ret), K(old_status), K(task_status_), K(task_id_));
    }
  }
  return ret;
}

int ObDropVecIndexTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropIndexTask has not been inited", K(ret));
  } else if (!need_retry()) {
    // task is done
  } else if (OB_FAIL(check_switch_succ())) {
    LOG_WARN("check need retry failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(ObDDLTaskStatus::WAIT_TRANS_END))) {
          LOG_WARN("fail to prepare", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END:
        if (OB_FAIL(wait_trans_end(ObDDLTaskStatus::OBTAIN_SNAPSHOT))) {
          LOG_WARN("fail to wait trans end", K(ret));
        }
        break;
      case ObDDLTaskStatus::OBTAIN_SNAPSHOT:
        if (OB_FAIL(obtain_snapshot(ObDDLTaskStatus::DROP_LOB_META_ROW))) {
          LOG_WARN("fail to wait trans end", K(ret));
        }
        break;
      case ObDDLTaskStatus::DROP_LOB_META_ROW:
        if (OB_FAIL(drop_lob_meta_row(ObDDLTaskStatus::DROP_AUX_INDEX_TABLE))) {
          LOG_WARN("fail to do drop lob meta row of aux table", K(ret));
        }
        break;
      case ObDDLTaskStatus::DROP_AUX_INDEX_TABLE:
        if (OB_FAIL(drop_aux_index_table(WAIT_CHILD_TASK_FINISH))) {
          LOG_WARN("fail to prepare", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH:
        if (OB_FAIL(check_and_wait_finish(SUCCESS))) {
          LOG_WARN("fail to check and wait task", K(ret));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(succ())) {
          LOG_WARN("do succ procedure failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(exit_all_dags_and_clean())) {
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

int ObDropVecIndexTask::deep_copy_index_arg(common::ObIAllocator &allocator,
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

int ObDropVecIndexTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::serialize", K(ret));
  } else if (OB_FAIL(rowkey_vid_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize rowkey vid table info", K(ret), K(rowkey_vid_));
  } else if (OB_FAIL(vid_rowkey_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize vid rowkey table info", K(ret), K(vid_rowkey_));
  } else if (OB_FAIL(domain_index_.serialize(buf, buf_size, pos))) { // delta_buffer_table
    LOG_WARN("fail to serialize index id table info", K(ret), K(domain_index_));
  } else if (OB_FAIL(vec_index_id_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize index delta buffer table info", K(ret), K(vec_index_id_));
  } else if (OB_FAIL(vec_index_snapshot_data_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize index snapshot data table info", K(ret), K(vec_index_snapshot_data_));
  } else if (OB_FAIL(drop_index_arg_.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_size,
                                               pos,
                                               delte_lob_meta_job_ret_code_))) {
    LOG_WARN("serialize delte_lob_meta_job_ret_code failed", K(ret));
  }
  return ret;
}

int ObDropVecIndexTask::deserialize_params_from_message(
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
  } else if (OB_FAIL(rowkey_vid_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize vid rowkey table info", K(ret));
  } else if (OB_FAIL(vid_rowkey_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize index id table info", K(ret));
  } else if (OB_FAIL(domain_index_.deep_copy_from_other(tmp_info, allocator_))) { // delta_buffer_table
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize delta buffer table info", K(ret));
  } else if (OB_FAIL(vec_index_id_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize index snapshot data table info", K(ret));
  } else if (OB_FAIL(vec_index_snapshot_data_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_drop_index_arg.deserialize(buf, buf_size, pos))) {
    LOG_WARN("deserialize failed", K(ret));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, tmp_drop_index_arg, drop_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               buf_size,
                                               pos,
                                               &delte_lob_meta_job_ret_code_))) {
    LOG_WARN("fail to deserialize delte_lob_meta_job_ret_code_", K(ret));
  }
  return ret;
}

int64_t ObDropVecIndexTask::get_serialize_param_size() const
{
  return ObDDLTask::get_serialize_param_size()
       + rowkey_vid_.get_serialize_size()
       + vid_rowkey_.get_serialize_size()
       + domain_index_.get_serialize_size()  // delta_buffer_table
       + vec_index_id_.get_serialize_size()
       + vec_index_snapshot_data_.get_serialize_size()
       + drop_index_arg_.get_serialize_size()
       + serialization::encoded_length_i64(delte_lob_meta_job_ret_code_);
}

int ObDropVecIndexTask::update_task_message()
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

int ObDropVecIndexTask::check_switch_succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_domain_index_exist = false; // delta_buffer_table
  bool is_vid_rowkey_exist = false;
  bool is_rowkey_vid_exist = false;
  bool is_index_id_exist = false;
  bool is_snapshot_data_exist = false;
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
  } else if (domain_index_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, domain_index_.table_id_, is_domain_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(domain_index_));
  } else if (vid_rowkey_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, vid_rowkey_.table_id_, is_vid_rowkey_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(vid_rowkey_));
  } else if (rowkey_vid_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, rowkey_vid_.table_id_, is_rowkey_vid_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(rowkey_vid_));
  } else if (vec_index_id_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, vec_index_id_.table_id_, is_index_id_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(vec_index_id_));
  } else if (vec_index_snapshot_data_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, vec_index_snapshot_data_.table_id_, is_snapshot_data_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(vec_index_snapshot_data_));
  } else {
    is_domain_index_exist = domain_index_.is_valid() ? is_domain_index_exist : false;
    is_rowkey_vid_exist = rowkey_vid_.is_valid() ? is_rowkey_vid_exist : false;
    is_vid_rowkey_exist = vid_rowkey_.is_valid() ? is_vid_rowkey_exist : false;
    is_index_id_exist = vec_index_id_.is_valid() ? is_index_id_exist : false;
    is_snapshot_data_exist = vec_index_snapshot_data_.is_valid() ? is_snapshot_data_exist : false;

    if (!is_domain_index_exist &&
        !is_rowkey_vid_exist &&
        !is_vid_rowkey_exist &&
        !is_index_id_exist &&
        !is_snapshot_data_exist) {
      task_status_ = ObDDLTaskStatus::SUCCESS;
    }
  }
  return ret;
}

/*
  create drop none share vector index table task and wait task
*/
int ObDropVecIndexTask::prepare(const share::ObDDLTaskStatus &new_status)
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

int ObDropVecIndexTask::drop_aux_index_table(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIndexTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (0 == domain_index_.task_id_ && domain_index_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, domain_index_.table_id_, domain_index_.index_name_, domain_index_.task_id_, true/* is_domain_index */))) {
      LOG_WARN("fail to create drop index task", K(ret), K(domain_index_));
  } else if (0 == vec_index_id_.task_id_  && vec_index_id_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, vec_index_id_.table_id_, vec_index_id_.index_name_, vec_index_id_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(vec_index_id_));
  } else if (0 == vec_index_snapshot_data_.task_id_ && vec_index_snapshot_data_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, vec_index_snapshot_data_.table_id_, vec_index_snapshot_data_.index_name_, vec_index_snapshot_data_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(vec_index_snapshot_data_));
  } else if (OB_FAIL(update_task_message())) {
    LOG_WARN("fail to update domain_index_, vec_index_id_, vec_index_snapshot_data_ to __all_ddl_task_status", K(ret));
  } else if (OB_FAIL(wait_none_share_index_child_task_finish(has_finished))) {
    LOG_WARN("fail to wait vec none share child task finish", K(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_VEC_INDEX_DROP_AUX_TABLE_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      has_finished = false;
      LOG_WARN("[ERRSIM] fail to drop aux index table", K(ret));
    }
  }
#endif
  if (has_finished) {
    // overwrite return code
    if (OB_FAIL(switch_status(new_status, true/*enable_flt*/, ret))) {
      LOG_WARN("fail to switch status", K(ret), K(new_status));
    } else {
      vec_index_snapshot_data_.table_id_ = OB_INVALID_ID;
      LOG_INFO("drop_aux_index_table success", K(ret));
    }
  }
  return ret;
}

int ObDropVecIndexTask::check_and_wait_finish(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool has_finished = false;
  if (OB_FAIL(create_drop_share_index_task())) {
    LOG_WARN("fail to create drop share index child task", K(ret));
  } else if (0 == rowkey_vid_.task_id_ && 0 == vid_rowkey_.task_id_) {
    // If there are other vector indexes, there is no need to drop the rowkey vid auxiliary table. And the task
    // status is set to success and skipped.
    has_finished = true;
  } else if (OB_FAIL(wait_share_index_child_task_finish(has_finished))) {
    LOG_WARN("fail to wait share index child task finish", K(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(common::EventTable::EN_VEC_INDEX_DROP_SHARE_TABLE_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      has_finished = false;
      LOG_WARN("[ERRSIM] fail to drop share index table", K(ret));
    }
  }
#endif
  if (has_finished) {
    // overwrite return code
    if (OB_FAIL(switch_status(new_status, true/*enable_flt*/, ret))) {
      LOG_WARN("fail to switch status", K(ret), K(new_status));
    } else {
      LOG_INFO("check_and_wait_finish success", K(ret));
    }
  }
  return ret;
}

int ObDropVecIndexTask::check_drop_index_finish(
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

int ObDropVecIndexTask::wait_child_task_finish(
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

int ObDropVecIndexTask::wait_none_share_index_child_task_finish(bool &has_finished)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObVecIndexDDLChildTaskInfo, 2> vec_child_tasks;
  if (domain_index_.is_valid() &&  OB_FAIL(vec_child_tasks.push_back(domain_index_))) {  // delta_buffer_table
    LOG_WARN("fail to push back index id table child task", K(ret));
  } else if (vec_index_id_.is_valid() && OB_FAIL(vec_child_tasks.push_back(vec_index_id_))) {
    LOG_WARN("fail to push back delta buffer table child task", K(ret));
  } else if (vec_index_snapshot_data_.is_valid() && OB_FAIL(vec_child_tasks.push_back(vec_index_snapshot_data_))) {
    LOG_WARN("fail to push back index snapshot data table child task", K(ret));
  } else if (OB_FAIL(wait_child_task_finish(vec_child_tasks, has_finished))) {
    LOG_WARN("fail to wait child task finish", K(ret), K(vec_child_tasks));
  }
  return ret;
}

int ObDropVecIndexTask::wait_share_index_child_task_finish(bool &has_finished)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObVecIndexDDLChildTaskInfo, 2> vec_child_tasks;
  if (vid_rowkey_.is_valid() && OB_FAIL(vec_child_tasks.push_back(vid_rowkey_))) {
    LOG_WARN("fail to push back vid rowkey table child task", K(ret));
  } else if (rowkey_vid_.is_valid() && OB_FAIL(vec_child_tasks.push_back(rowkey_vid_))) {
    LOG_WARN("fail to push back rowkey vid table child task", K(ret));
  } else if (OB_FAIL(wait_child_task_finish(vec_child_tasks, has_finished))) {
    LOG_WARN("fail to wait child task finish", K(ret), K(vec_child_tasks));
  }
  return ret;
}

int ObDropVecIndexTask::create_drop_index_task(
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

int ObDropVecIndexTask::create_drop_share_index_task()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (0 == rowkey_vid_.task_id_ && rowkey_vid_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, rowkey_vid_.table_id_, rowkey_vid_.index_name_, rowkey_vid_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(rowkey_vid_));
  } else if (0 == vid_rowkey_.task_id_ && vid_rowkey_.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, vid_rowkey_.table_id_, vid_rowkey_.index_name_, vid_rowkey_.task_id_))) {
    LOG_WARN("fail to create drop index task", K(ret), K(vid_rowkey_));
  } else if (OB_FAIL(update_task_message())) {
    LOG_WARN("fail to update vid_rowkey_ and rowkey_vid_ to __all_ddl_task_status", K(ret));
  }
  return ret;
}

int ObDropVecIndexTask::succ()
{
  return cleanup();
}

int ObDropVecIndexTask::fail()
{
  return cleanup();
}

int ObDropVecIndexTask::check_and_cancel_del_dag(bool &all_dag_exit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!vec_index_snapshot_data_.is_valid() || !del_lob_meta_row_task_submitted_) {
    all_dag_exit = true;
  } else if (OB_FAIL(ObDDLUtil::check_and_cancel_single_replica_dag(this, vec_index_snapshot_data_.table_id_,
            vec_index_snapshot_data_.table_id_, check_dag_exit_tablets_map_, check_dag_exit_retry_cnt_, false/*is_complement_data_dag*/, all_dag_exit))) {
    LOG_WARN("fail to check and cancel delete lob mete row dag", K(ret), K(vec_index_snapshot_data_));
  }
  return ret;
}

int ObDropVecIndexTask::release_snapshot(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!vec_index_snapshot_data_.is_valid()) {
    // do nothing
  } else if (OB_FAIL(ObDDLUtil::release_snapshot(this, vec_index_snapshot_data_.table_id_, vec_index_snapshot_data_.table_id_, snapshot_version))) {
    LOG_WARN("release snapshot failed", K(ret));
  }
  return ret;
}

int ObDropVecIndexTask::finish()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (snapshot_version_ > 0 && OB_FAIL(release_snapshot(snapshot_version_))) {
    LOG_WARN("release snapshot failed", K(ret));
  }
  return ret;
}

int ObDropVecIndexTask::exit_all_dags_and_clean()
{
  int ret = OB_SUCCESS;
  bool all_delete_lob_meta_row_dag_exit = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(check_and_cancel_del_dag(all_delete_lob_meta_row_dag_exit))) {
    LOG_WARN("check and cancel delete lob meta row data dag failed", K(ret));
  } else if (!all_delete_lob_meta_row_dag_exit) {
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("wait all delete lob meta row data dag exit", K(dst_tenant_id_), K(task_id_));
    }
  } else if (OB_FAIL(finish())) {
    LOG_WARN("finish tans failed", K(ret));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("cleanup failed", K(ret));
  }
  return ret;
}

int ObDropVecIndexTask::cleanup_impl()
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

int ObDropVecIndexTask::send_build_single_replica_request()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_format_version = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnRedefinitionTask has not been inited", K(ret));
  } else {
    ObDDLReplicaBuildExecutorParam param;
    param.tenant_id_ = tenant_id_;
    param.dest_tenant_id_ = dst_tenant_id_;
    param.ddl_type_ = task_type_;
    param.snapshot_version_ = snapshot_version_; // should > 0, but = 0
    param.task_id_ = task_id_;
    param.parallelism_ = std::max(parallelism_, 1L);
    param.execution_id_ = execution_id_; // should >= 0
    param.data_format_version_ = data_format_version_; // should > 0
    param.consumer_group_id_ = consumer_group_id_;
    param.is_no_logging_ = is_no_logging_;

    if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, vec_index_snapshot_data_.table_id_, param.source_tablet_ids_))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_FAIL(ObDDLUtil::get_tablets(dst_tenant_id_, vec_index_snapshot_data_.table_id_, param.dest_tablet_ids_))) {
      LOG_WARN("fail to get tablets", K(ret), K(tenant_id_), K(target_object_id_));
    }

    const int64_t src_tablet_cnt = param.source_tablet_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < src_tablet_cnt; ++i) {
      if (OB_FAIL(param.source_table_ids_.push_back(vec_index_snapshot_data_.table_id_))) {
        LOG_WARN("failed to push back src table id", K(ret));
      } else if (OB_FAIL(param.source_schema_versions_.push_back(schema_version_))) {
        LOG_WARN("failed to push back src schema version", K(ret));
      }
    }
    const int64_t dest_tablet_cnt = param.dest_tablet_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablet_cnt; ++i) {
      if (OB_FAIL(param.dest_table_ids_.push_back(target_object_id_))) {
        LOG_WARN("failed to push back dest table id", K(ret));
      } else if (OB_FAIL(param.dest_schema_versions_.push_back(dst_schema_version_))) {
        LOG_WARN("failed to push back dest schema version", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replica_builder_.build(param))) {
      LOG_WARN("fail to send build single replica", K(ret), K(param));
    } else {
      del_lob_meta_row_task_submitted_ = true;
      delte_lob_meta_request_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

// check whether all leaders have completed the task
int ObDropVecIndexTask::check_build_single_replica(bool &is_end)
{
  int ret = OB_SUCCESS;
  is_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedefinitionTask has not been inited", K(ret));
  } else if (OB_FAIL(replica_builder_.check_build_end(false/*do not need check sum*/,is_end, delte_lob_meta_job_ret_code_))) {
    LOG_WARN("fail to check build end", K(ret));
  } else if (!is_end) {
    if (delte_lob_meta_request_time_ + ObDDLUtil::calc_inner_sql_execute_timeout() < ObTimeUtility::current_time()) {   // timeout, retry
      del_lob_meta_row_task_submitted_ = false;
      delte_lob_meta_request_time_ = 0;
    }
  }
  return ret;
}

// update sstable complement status for all leaders
int ObDropVecIndexTask::update_drop_lob_meta_row_job_status(const common::ObTabletID &tablet_id,
                                                            const ObAddr &addr,
                                                            const int64_t snapshot_version,
                                                            const int64_t execution_id,
                                                            const int ret_code,
                                                            const ObDDLTaskInfo &addition_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropVecIndexTask has not been inited", K(ret));
  } else if (ObDDLTaskStatus::DROP_LOB_META_ROW != task_status_) {
    // by pass, may be network delay
  } else if (snapshot_version != snapshot_version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot version not match", K(ret), K(snapshot_version), K(snapshot_version_));
  } else if (execution_id < execution_id_) {
    LOG_INFO("receive a mismatch execution result, ignore", K(ret_code), K(execution_id), K(execution_id_));
  } else if (OB_FAIL(replica_builder_.update_build_progress(tablet_id,
                                                            addr,
                                                            ret_code,
                                                            addition_info.row_scanned_,
                                                            addition_info.row_inserted_,
                                                            addition_info.physical_row_count_))) {
    LOG_WARN("fail to set partition task status", K(ret));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
