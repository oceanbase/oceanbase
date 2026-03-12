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

#define USING_LOG_PREFIX STORAGE_FTS

#include "rootserver/ddl_task/ob_drop_search_index_task.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{

ObDropSearchIndexTask::ObDropSearchIndexTask()
  : ObDDLTask(DDL_DROP_FTS_INDEX),
    root_service_(nullptr),
    search_def_index_(),
    search_data_index_(),
    drop_def_index_finish_(false),
    drop_data_index_finish_(false)
{
}

ObDropSearchIndexTask::~ObDropSearchIndexTask()
{
}

int ObDropSearchIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t data_table_id,
    const ObDDLType ddl_type,
    const ObFTSDDLChildTaskInfo &search_def_index,
    const ObFTSDDLChildTaskInfo &search_data_index,
    const ObString &ddl_stmt_str,
    const int64_t schema_version,
    const int64_t consumer_group_id,
    const int64_t target_object_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
               || task_id <= 0
               || OB_INVALID_ID == data_table_id
               || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id),
        K(search_def_index), K(schema_version));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else if (OB_FAIL(search_def_index_.deep_copy_from_other(search_def_index, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(search_def_index));
  } else if (OB_FAIL(search_data_index_.deep_copy_from_other(search_data_index, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(search_data_index));
  } else if (OB_FAIL(set_ddl_stmt_str(ddl_stmt_str))) {
    LOG_WARN("fail to deep copy drop index arg", K(ret));
  } else {
    task_type_ = ddl_type;
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = tenant_id;
    object_id_ = data_table_id;
    target_object_id_ = target_object_id; // not use this id
    schema_version_ = schema_version;
    task_id_ = task_id;
    parent_task_id_ = 0; // no parent task
    consumer_group_id_ = consumer_group_id;
    task_version_ = OB_DROP_SEARCH_INDEX_TASK_VERSION;
    dst_tenant_id_ = tenant_id;
    dst_schema_version_ = schema_version;
    is_inited_ = true;
  }
  return ret;
}

int ObDropSearchIndexTask::init(const ObDDLTaskRecord &task_record)
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

int ObDropSearchIndexTask::process()
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
        if (OB_FAIL(prepare(WAIT_CHILD_TASK_FINISH))) {
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

int ObDropSearchIndexTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int8_t drop_def_index_finish = static_cast<int8_t>(drop_def_index_finish_);
  int8_t drop_data_index_finish = static_cast<int8_t>(drop_data_index_finish_);
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::serialize", K(ret));
  } else if (OB_FAIL(search_def_index_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize search def table info", K(ret), K(search_def_index_));
  } else if (OB_FAIL(search_data_index_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize search data table info", K(ret), K(search_data_index_));
  } else if (OB_FAIL(ddl_stmt_str_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize ddl stmt string", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_size,
                                              pos,
                                              drop_def_index_finish))) {
    LOG_WARN("fail to serialize drop def index finish", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_size,
                                              pos,
                                              drop_data_index_finish))) {
    LOG_WARN("fail to serialize drop data index finish", K(ret));
  }
  return ret;
}

int ObDropSearchIndexTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObFTSDDLChildTaskInfo tmp_info;
  ObString tmp_ddl_stmt_str;
  int8_t drop_def_index_finish = 0;
  int8_t drop_data_index_finish = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::deserialize", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize search def table info", K(ret));
  } else if (OB_FAIL(search_def_index_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize search data table info", K(ret));
  } else if (OB_FAIL(search_data_index_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_UNLIKELY(pos >= buf_size)) {
    // The end of the message has been reached. It is an old version message without drop index arg.
    // just skip.
  } else if (OB_FAIL(tmp_ddl_stmt_str.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize drop index arg", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, tmp_ddl_stmt_str, ddl_stmt_str_))) {
    LOG_WARN("fail to copy ddl stmt string", K(ret), K(tmp_ddl_stmt_str));
  } else if (OB_UNLIKELY(pos >= buf_size)) {
    // The end of the message has been reached. It is an old version message without drop index arg.
    // just skip.
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              buf_size,
                                              pos,
                                              &drop_def_index_finish))) {
      LOG_WARN("fail to deserialize drop def index finish", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              buf_size,
                                              pos,
                                              &drop_data_index_finish))) {
      LOG_WARN("fail to deserialize drop data index finish", K(ret));
  } else {
    drop_def_index_finish_ = static_cast<bool>(drop_def_index_finish);
    drop_data_index_finish_ = static_cast<bool>(drop_data_index_finish);
  }
  return ret;
}

int64_t ObDropSearchIndexTask::get_serialize_param_size() const
{
  int8_t drop_def_index_finish = static_cast<int8_t>(drop_def_index_finish_);
  int8_t drop_data_index_finish = static_cast<int8_t>(drop_data_index_finish_);
  return ObDDLTask::get_serialize_param_size()
       + search_def_index_.get_serialize_size()
       + search_data_index_.get_serialize_size()
       + ddl_stmt_str_.get_serialize_size()
       + serialization::encoded_length_i8(drop_def_index_finish)
       + serialization::encoded_length_i8(drop_data_index_finish);
}

int ObDropSearchIndexTask::update_task_message(common::ObISQLClient &proxy)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString msg;
  common::ObArenaAllocator allocator("ObDropSearchIdx");
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

int ObDropSearchIndexTask::check_switch_succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_def_index_exist = false;
  bool is_data_index_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hasn't initialized", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObDDLUtil::check_tenant_status_normal(GCTX.sql_proxy_, tenant_id_))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret || OB_STANDBY_READ_ONLY == ret) {
      need_retry_ = false;
      LOG_INFO("tenant status is abnormal, exit anyway", K(ret), K(tenant_id_));
    } else {
      LOG_WARN("check tenant status failed", K(ret), K(tenant_id_));
    }
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id_));
  } else if (search_def_index_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, search_def_index_.table_id_, is_def_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(search_def_index_));
  } else if (search_data_index_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, search_data_index_.table_id_, is_data_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(search_data_index_));
  } else if (!is_def_index_exist && !is_data_index_exist) {
    task_status_ = ObDDLTaskStatus::SUCCESS;
  }
  return ret;
}

int ObDropSearchIndexTask::prepare(const share::ObDDLTaskStatus &new_status)
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

  DEBUG_SYNC(DROP_FTS_INDEX_PREPARE_STATUS);

  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status = new_status;
    if (OB_FAIL(ret)) {
      next_status = ObIDDLTask::in_ddl_retry_white_list(ret) ? next_status : ObDDLTaskStatus::FAIL;
    }
    (void)switch_status(next_status, true, ret);
  }
  return ret;
}

int ObDropSearchIndexTask::check_and_wait_finish(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  }
  if (OB_SUCC(ret)) {
    if (!drop_data_index_finish_) {
      if (OB_FAIL(send_and_wait_drop_index_task(search_data_index_, schema_guard, drop_data_index_finish_))) {
          LOG_WARN("Failed to send and wait drop index task for search data index", K(ret));
      }
    } else if (!drop_def_index_finish_) {
      if (OB_FAIL(send_and_wait_drop_index_task(search_def_index_, schema_guard, drop_def_index_finish_))) {
          LOG_WARN("Failed to send and wait drop index task for search def index", K(ret));
      }
    } else {
      state_finished = true;
    }
  }
  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status = new_status;
    if (OB_FAIL(ret)) {
      next_status = ObIDDLTask::in_ddl_retry_white_list(ret) ? next_status : ObDDLTaskStatus::FAIL;
    }
    (void)switch_status(next_status, true, ret);
  }
  return ret;
}

int ObDropSearchIndexTask::check_drop_index_finish(
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
    LOG_WARN("there are invalid arguments", K(ret), K(tenant_id), K(task_id), K(table_id));
  } else if (OB_FAIL(share::ObDDLErrorMessageTableOperator::get_ddl_error_message(tenant_id,
                                                                                  task_id,
                                                                                  -1/*target_object_id*/,
                                                                                  table_id,
                                                                                  *GCTX.sql_proxy_,
                                                                                  error_message,
                                                                                  unused_user_msg_len))) {

    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("the drop index task not completed", K(ret), K(tenant_id), K(task_id), K(table_id), K(has_finished));
    } else {
      LOG_WARN("fail to get ddl error message", K(ret), K(tenant_id), K(task_id), K(table_id));
    }
  } else {
    ret = error_message.ret_code_;
    has_finished = true;
    if (OB_SUCCESS == ret && OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
      LOG_WARN("fail to update drop fulltext index task message", K(ret));
    }
    LOG_INFO("wait drop index finish", K(ret), K(tenant_id), K(task_id), K(table_id), K(has_finished));
  }
  return ret;
}

int ObDropSearchIndexTask::wait_drop_child_task_finish(
    const ObFTSDDLChildTaskInfo &child_task_info,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  has_finished = false;
  if (-1 == child_task_info.task_id_ || OB_INVALID_ID == child_task_info.table_id_) {
    has_finished = true;
  } else if (OB_FAIL(check_drop_index_finish(tenant_id_, child_task_info.task_id_, child_task_info.table_id_, has_finished))) {
    LOG_WARN("fail to check fts index child task finish", K(ret));
  } else if (!has_finished) {
    LOG_INFO("the child task hasn't been finished", K(ret), K(tenant_id_), K(child_task_info));
  }
  return ret;
}

int ObDropSearchIndexTask::send_and_wait_drop_index_task(
    ObFTSDDLChildTaskInfo &child_task_info,
    ObSchemaGetterGuard &schema_guard,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  if (0 == child_task_info.task_id_ &&
      child_task_info.is_valid() &&
      OB_FAIL(create_drop_index_task(schema_guard,
                                     child_task_info.table_id_,
                                     child_task_info.index_name_,
                                     child_task_info.task_id_))) {
      LOG_WARN("fail to create drop index task", K(ret), K(child_task_info));
  } else if (0 == child_task_info.task_id_) {
    // by pass
    has_finished = true;
    LOG_INFO("the rowkey doc/doc rowkey index table do not need to be deleted, \
        there are still other full-text indexes", K(ret), K(child_task_info));
  } else if (OB_FAIL(wait_drop_child_task_finish(child_task_info, has_finished))) {
    LOG_WARN("fail to wait drop domain child task finish", K(ret), K(child_task_info));
  }
  return ret;
}

int ObDropSearchIndexTask::create_drop_index_task(
    share::schema::ObSchemaGetterGuard &guard,
    const uint64_t index_tid,
    const common::ObString &index_name,
    int64_t &task_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObSqlString drop_index_sql;
  bool is_index_exist = false;
  if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.rs_rpc_proxy_));
  } else if (OB_INVALID_ID == index_tid) {
    // nothing to do, just by pass.
    task_id = -1;
  } else if (OB_UNLIKELY(index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_name));
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
  } else if (data_table_schema->is_search_def_index()
    && OB_FAIL(guard.get_table_schema(tenant_id_, data_table_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(data_table_schema->get_data_table_id()));
  } else if (nullptr == data_table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema is nullptr", K(ret), KP(database_schema), KP(data_table_schema));
  } else if (OB_FAIL(drop_index_sql.assign(ddl_stmt_str_))) {
    LOG_WARN("fail to assign drop index sql", K(ret));
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
    arg.ddl_stmt_str_        = nullptr;
    arg.is_add_to_scheduler_ = true;
    arg.task_id_             = task_id_;
    arg.is_hidden_           = data_table_schema->is_user_hidden_table();
    arg.ddl_stmt_str_ = drop_index_sql.string();
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
            index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout_us))) {
      LOG_WARN("fail to get ddl rpc timeout", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(ddl_rpc_timeout_us).drop_index(arg, res))) {
      LOG_WARN("fail to drop index", K(ret), K(ddl_rpc_timeout_us), K(arg), K(res.task_id_));
    } else {
      task_id = res.task_id_;
    }
    LOG_INFO("drop index", K(ret), K(index_tid), K(index_name), K(task_id),
        "data table name", data_table_schema->get_table_name_str(),
        "database name", database_schema->get_database_name_str(),
        K(drop_index_sql.ptr()));
  }

  DEBUG_SYNC(AFTER_DROP_FTS_SUBMIT_SUBTASK);

  return ret;
}

int ObDropSearchIndexTask::succ()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_WARN("cleanup task failed", K(ret));
  }
  return ret;
}

int ObDropSearchIndexTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_WARN("cleanup task failed", K(ret));
  }
  return ret;
}

int ObDropSearchIndexTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    ObTableLockOwnerID owner_id;
    ObMySQLTransaction trans;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_,
                                                              schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     object_id_,
                                                     data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(object_id_));
    } else if (OB_UNLIKELY(nullptr == data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data table schema is nullptr", K(ret), KP(data_table_schema));
    } else if (data_table_schema->is_search_def_index()
      && OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                               data_table_schema->get_data_table_id(),
                                               data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(data_table_schema->get_data_table_id()));
    } else if (nullptr == data_table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, schema is nullptr", K(ret), KP(data_table_schema));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, task_id_))) {
      LOG_WARN("fail to get owner id", K(ret), K(task_id_));
    } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(*data_table_schema,
                                                            0 /* index_table_id */, // not support global fulltext index
                                                            false /* is_global_index = false */,
                                                            owner_id,
                                                            trans))) {
      LOG_WARN("failed to unlock online ddl lock", K(ret));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  if (FAILEDx(ObDDLTaskRecordOperator::delete_record(*GCTX.sql_proxy_, tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", K(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
