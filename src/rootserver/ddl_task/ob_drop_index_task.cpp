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

#include "ob_drop_index_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "rootserver/ob_root_service.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

ObDropIndexTask::ObDropIndexTask()
  : ObDDLTask(DDL_DROP_INDEX), wait_trans_ctx_(), drop_index_arg_()
{
}

ObDropIndexTask::~ObDropIndexTask()
{
}

int ObDropIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t data_table_id,
    const uint64_t index_table_id,
    const int64_t schema_version,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const obrpc::ObDropIndexArg &drop_index_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == data_table_id
      || OB_INVALID_ID == index_table_id || schema_version <= 0 || parent_task_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id),
        K(index_table_id), K(schema_version), K(parent_task_id));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, drop_index_arg, drop_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", K(ret));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = tenant_id;
    object_id_ = data_table_id;
    target_object_id_ = index_table_id;
    schema_version_ = schema_version;
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
    consumer_group_id_ = consumer_group_id;
    task_version_ = OB_DROP_INDEX_TASK_VERSION;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    is_inited_ = true;
    ddl_tracing_.open();
  }
  return ret;
}

int ObDropIndexTask::init(
    const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else {
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
    if (nullptr != task_record.message_.ptr()) {
      int64_t pos = 0;
      if (OB_FAIL(deserlize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
        LOG_WARN("deserialize params from message failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      is_inited_ = true;
      // set up span during recover task
    ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

bool ObDropIndexTask::is_valid() const
{
  return is_inited_ && !trace_id_.is_invalid();
}

int ObDropIndexTask::update_index_status(const ObIndexStatus new_status)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root_service is nullptr", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id_, schema_guard, schema_version_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(target_object_id_), K(schema_version_));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(target_object_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", K(ret));
  } else {
    obrpc::ObUpdateIndexStatusArg arg;
    arg.index_table_id_ = index_schema->get_table_id();
    arg.status_ = new_status;
    arg.exec_tenant_id_ = tenant_id_;
    arg.in_offline_ddl_white_list_ = index_schema->get_table_state_flag() != TABLE_STATE_NORMAL;
    int64_t ddl_rpc_timeout = 0;
    int64_t table_id = index_schema->get_table_id();
    DEBUG_SYNC(BEFORE_UPDATE_GLOBAL_INDEX_STATUS);
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, table_id, ddl_rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout fail", K(ret));
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().to(GCTX.self_addr()).timeout(ddl_rpc_timeout).update_index_status(arg))) {
      LOG_WARN("update index status failed", K(ret), K(arg));
    } else {
      LOG_INFO("notify index status changed finish", K(new_status), K(target_object_id_));
    }
  }
  return ret;
}

int ObDropIndexTask::prepare(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropIndexTask has not been inited", K(ret));
  } else if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", K(ret));
  }
  return ret;
}

// Disused stage, just for compatibility.
int ObDropIndexTask::set_write_only(const share::ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropIndexTask has not been inited", K(ret));
  } else if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", K(ret));
  }
  return ret;
}

int ObDropIndexTask::set_unusable(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropIndexTask has not been inited", K(ret));
  } else if (OB_FAIL(update_index_status(INDEX_STATUS_UNUSABLE))) {
    LOG_WARN("update index status failed", K(ret));
  } else if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", K(ret));
  }
  return ret;
}

int ObDropIndexTask::drop_index_impl()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObSqlString drop_index_sql;
  bool is_oracle_mode = false;
  bool is_index_exist = false;
  ObString index_name;
  const ObTableSchema *index_schema = nullptr;
  if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root_service is nullptr", K(ret));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, target_object_id_, is_index_exist))) {
    LOG_WARN("check table exist failed", K(ret), K(target_object_id_));
  } else if (!is_index_exist) {
    // by pass
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_schema))) {
    LOG_WARN("get index schema failed", K(ret), K(target_object_id_));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("index schema is null", K(ret), K(target_object_id_));
  } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
    LOG_WARN("get index name failed", K(ret), K(index_schema->get_table_type()), KPC(index_schema));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, index_schema->get_database_id(), database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K(index_schema->get_database_id()));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(index_schema->get_data_table_id()));
  } else if (OB_UNLIKELY(nullptr == database_schema || nullptr == data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null schema", K(ret), KP(database_schema), KP(data_table_schema));
  } else if (OB_FAIL(drop_index_sql.assign(drop_index_arg_.ddl_stmt_str_))) {
    LOG_WARN("assign user drop index sql failed", K(ret));
  } else {
    int64_t ddl_rpc_timeout = 0;
    obrpc::ObDropIndexArg drop_index_arg;
    obrpc::ObDropIndexRes drop_index_res;
    drop_index_arg.is_inner_          = true;
    drop_index_arg.tenant_id_         = tenant_id_;
    drop_index_arg.exec_tenant_id_    = tenant_id_;
    drop_index_arg.index_table_id_    = target_object_id_;
    drop_index_arg.session_id_        = data_table_schema->get_session_id();
    drop_index_arg.index_name_        = index_name;
    drop_index_arg.table_name_        = data_table_schema->get_table_name();
    drop_index_arg.database_name_     = database_schema->get_database_name_str();
    drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
    drop_index_arg.ddl_stmt_str_      = drop_index_sql.string();
    drop_index_arg.is_add_to_scheduler_ = false;
    drop_index_arg.task_id_           = task_id_;
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout))) {
      LOG_WARN("failed to get ddl rpc timeout", K(ret));
    } else if (OB_FAIL(root_service_->get_common_rpc_proxy().timeout(ddl_rpc_timeout).drop_index(drop_index_arg, drop_index_res))) {
      LOG_WARN("drop index failed", K(ret), K(ddl_rpc_timeout));
    }
    LOG_INFO("drop index", K(ret), K(drop_index_sql.ptr()), K(drop_index_arg));
  }
  return ret;
}

int ObDropIndexTask::drop_index(const ObDDLTaskStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(drop_index_impl())) {
    LOG_WARN("send drop index rpc failed", K(ret));
  } else if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", K(ret));
  }
  return ret;
}

int ObDropIndexTask::succ()
{
  return cleanup();
}

int ObDropIndexTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(drop_index_impl())) {
    LOG_WARN("drop index impl failed", K(ret));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("cleanup failed", K(ret));
  }
  return ret;
}

int ObDropIndexTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", K(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }

  if (OB_SUCC(ret) && parent_task_id_ > 0) {
    const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
    root_service_->get_ddl_task_scheduler().on_ddl_task_finish(parent_task_id, get_task_key(), ret_code_, trace_id_);
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}

int ObDropIndexTask::process()
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
        if (OB_FAIL(prepare(SET_WRITE_ONLY))) {
          LOG_WARN("prepare failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::SET_WRITE_ONLY:
        if (OB_FAIL(set_write_only(WAIT_TRANS_END_FOR_WRITE_ONLY))) {
          LOG_WARN("set write only failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END_FOR_WRITE_ONLY:
        if (OB_FAIL(wait_trans_end(wait_trans_ctx_, SET_UNUSABLE))) {
          LOG_WARN("wait trans end failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::SET_UNUSABLE:
        if (OB_FAIL(set_unusable(WAIT_TRANS_END_FOR_UNUSABLE))) {
          LOG_WARN("set unusable failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_TRANS_END_FOR_UNUSABLE:
        if (OB_FAIL(wait_trans_end(wait_trans_ctx_, DROP_SCHEMA))) {
          LOG_WARN("wait trans end failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::DROP_SCHEMA:
        if (OB_FAIL(drop_index(SUCCESS))) {
          LOG_WARN("drop index failed", K(ret));
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

// switch to SUCCESS if index table does not exist.
int ObDropIndexTask::check_switch_succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_index_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else if (OB_FAIL(root_service_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, target_object_id_, is_index_exist))) {
    LOG_WARN("check table exist failed", K(ret), K(target_object_id_));
  } else if (!is_index_exist) {
    task_status_ = ObDDLTaskStatus::SUCCESS;
  } 
  return ret;
}

int ObDropIndexTask::deep_copy_index_arg(common::ObIAllocator &allocator,
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

int ObDropIndexTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("ObDDLTask serialize failed", K(ret));
  } else if (OB_FAIL(drop_index_arg_.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize failed", K(ret));
  }
  return ret;
}

int ObDropIndexTask::deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObDropIndexArg tmp_drop_index_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserlize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", K(ret));
  } else if (OB_FAIL(tmp_drop_index_arg.deserialize(buf, buf_size, pos))) {
    LOG_WARN("deserialize failed", K(ret));
  } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_drop_index_arg))) {
    LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_drop_index_arg));
  } else if (OB_FAIL(deep_copy_index_arg(allocator_, tmp_drop_index_arg, drop_index_arg_))) {
    LOG_WARN("deep copy drop index arg failed", K(ret));
  }
  return ret;
}

int64_t ObDropIndexTask::get_serialize_param_size() const
{
  return drop_index_arg_.get_serialize_size() + ObDDLTask::get_serialize_param_size();
}

void ObDropIndexTask::flt_set_task_span_tag() const
{
  FLT_SET_TAG(ddl_task_id, task_id_, ddl_parent_task_id, parent_task_id_,
              ddl_data_table_id, object_id_, ddl_index_table_id, target_object_id_,
              ddl_schema_version, schema_version_);
}

void ObDropIndexTask::flt_set_status_span_tag() const
{
  switch (task_status_) {
  case ObDDLTaskStatus::PREPARE: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::SET_WRITE_ONLY: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::WAIT_TRANS_END_FOR_WRITE_ONLY: {
    FLT_SET_TAG(ddl_data_table_id, object_id_, ddl_snapshot_version, snapshot_version_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::SET_UNUSABLE: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::WAIT_TRANS_END_FOR_UNUSABLE: {
    FLT_SET_TAG(ddl_data_table_id, object_id_, ddl_snapshot_version, snapshot_version_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::DROP_SCHEMA: {
    FLT_SET_TAG(ddl_index_table_id, target_object_id_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::FAIL: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::SUCCESS: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  default: {
    break;
  }
  }
}
