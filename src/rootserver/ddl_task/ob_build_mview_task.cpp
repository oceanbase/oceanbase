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

#include "rootserver/ddl_task/ob_build_mview_task.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_ddl_common.h"
#include "share/ob_ddl_error_message_table_operator.h"

namespace oceanbase
{
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver
{
ObBuildMViewTask::ObBuildMViewTask()
    : ObDDLTask(ObDDLType::DDL_CREATE_MVIEW),
      mview_table_id_(target_object_id_),
      root_service_(nullptr),
      mview_complete_refresh_task_id_(0)
{
}
ObBuildMViewTask::~ObBuildMViewTask()
{
}

int ObBuildMViewTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const share::schema::ObTableSchema *mview_schema,
    const int64_t schema_version,
    const int64_t parallelism,
    const int64_t consumer_group_id,
    const obrpc::ObMViewCompleteRefreshArg &mview_complete_refresh_arg,
    const int64_t parent_task_id,
    const int64_t task_status,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_format_version = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service is null", KR(ret), KP(root_service_));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || task_id <= 0
             || OB_ISNULL(mview_schema)
             || schema_version <= 0
             || task_status < ObDDLTaskStatus::PREPARE
             || task_status > ObDDLTaskStatus::SUCCESS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_id), KP(mview_schema), K(schema_version), K(task_status));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, mview_complete_refresh_arg, arg_))) {
    LOG_WARN("failed to copy mview complete refresh arg", KR(ret), K(mview_complete_refresh_arg));
  } else if (OB_FAIL(ObShareUtil::fetch_current_data_version(
      *GCTX.sql_proxy_, tenant_id, tenant_data_format_version))) {
    LOG_WARN("get min data version failed", KR(ret), K(tenant_id));
  } else {
    int64_t now = ObTimeUtility::current_time();
    set_gmt_create(now);
    tenant_id_ = tenant_id;
    object_id_ = mview_schema->get_table_id();
    mview_table_id_ = mview_schema->get_table_id();
    schema_version_ = schema_version;
    parallelism_ = parallelism;
    arg_.exec_tenant_id_ = tenant_id_;
    arg_.parent_task_id_ = task_id;
    if (snapshot_version > 0) {
      snapshot_version_ = snapshot_version;
    }
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
    task_version_ = OB_BUILD_MVIEW_TASK_VERSION;
    consumer_group_id_ = consumer_group_id;
    start_time_ = now;
    data_format_version_ = tenant_data_format_version;
    task_status_ = static_cast<ObDDLTaskStatus>(task_status);
    if (OB_FAIL(init_ddl_task_monitor_info(mview_schema->get_table_id()))) {
      LOG_WARN("failed to init ddl task monitor info", KR(ret));
    } else {
      dst_tenant_id_ = tenant_id_;
      dst_schema_version_ = schema_version_;

      is_inited_ = true;
      ddl_tracing_.open();
    }
  }

  return ret;
}

int ObBuildMViewTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;

  const uint64_t mview_table_id = task_record.object_id_;
  const int64_t schema_version = task_record.schema_version_;
  int64_t pos = 0;
  const ObTableSchema *mview_schema = nullptr;
  const char *ddl_type_str = nullptr;
  const char *target_name = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service is null", KR(ret), KP(root_service_));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service", KR(ret));
  } else if (!task_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(task_record));
  } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
    LOG_WARN("deserialize params from message failed", KR(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          task_record.tenant_id_, schema_guard, schema_version))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(mview_table_id), K(schema_version));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", KR(ret), K(mview_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(task_record.tenant_id_, mview_table_id, mview_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(mview_table_id));
  } else if (OB_ISNULL(mview_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", KR(ret), K(mview_schema));
  } else {
    int64_t now = ObTimeUtility::current_time();
    set_gmt_create(now);
    tenant_id_ = task_record.tenant_id_;
    object_id_ = mview_table_id;
    mview_table_id_ = mview_table_id;
    schema_version_ = schema_version;
    snapshot_version_ = task_record.snapshot_version_;
    execution_id_ = task_record.execution_id_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    ret_code_ = task_record.ret_code_;
    start_time_ = now;
    if (OB_FAIL(init_ddl_task_monitor_info(mview_schema->get_table_id()))) {
      LOG_WARN("failed to init ddl task monitor info", KR(ret));
    } else {
      dst_tenant_id_ = tenant_id_;
      dst_schema_version_ = schema_version_;
      is_inited_ = true;
      ddl_tracing_.open();
    }
  }
  return ret;
}

int ObBuildMViewTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("failed to check health", KR(ret));
  } else if (!need_retry()) {
    // by pass
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch (task_status_) {
      case ObDDLTaskStatus::START_REFRESH_MVIEW_TASK:
        if (OB_FAIL(start_refresh_mview_task())) {
          LOG_WARN("start refresh mview task failed", KR(ret), K(*this));
        }
        break;
      case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH:
        if (OB_FAIL(wait_child_task_finish())) {
          LOG_WARN("wait trans end failed", KR(ret), K(*this));
        }
        break;
      case ObDDLTaskStatus::TAKE_EFFECT:
        if (OB_FAIL(enable_mview())) {
          LOG_WARN("enable_mview failed", KR(ret), K(*this));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(clean_on_fail())) {
          LOG_WARN("failed to do cleanup", KR(ret), K(*this));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(succ())) {
          LOG_WARN("clean failed_task failed", KR(ret), K(*this));
        }
        break;
      default:
        LOG_INFO("not expected status", KR(ret), K(task_status_));
        break;
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

int ObBuildMViewTask::clean_on_fail()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *mview_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  bool is_mv_exist = false;
  if (IS_NOT_INIT || root_service_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("root_service_ is null", KR(ret), KP(root_service_));
  } else if (OB_FAIL(root_service_->get_ddl_service().get_tenant_schema_guard_with_version_in_inner_table(
      tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, mview_table_id_, is_mv_exist))) {
    LOG_WARN("check table exist failed", KR(ret), K_(tenant_id), K(mview_table_id_));
  } else if (!is_mv_exist) {
    // by pass
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
      mview_table_id_, mview_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K(tenant_id_), K(mview_table_id_));
  } else if (OB_ISNULL(mview_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mview schema is null", KR(ret), K(mview_table_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_,
      mview_schema->get_database_id(), database_schema))) {
    LOG_WARN("failed to get database_schema", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", KR(ret), K(mview_schema->get_database_id()));
  } else {
    obrpc::ObDropTableArg drop_table_arg;
    obrpc::ObTableItem table_item;
    drop_table_arg.if_exist_ = true;
    drop_table_arg.tenant_id_ = tenant_id_;
    drop_table_arg.to_recyclebin_ = false;
    drop_table_arg.table_type_ = MATERIALIZED_VIEW;
    drop_table_arg.session_id_ = 100;
    drop_table_arg.exec_tenant_id_ = tenant_id_;
    table_item.database_name_ = database_schema->get_database_name();
    table_item.table_name_ = mview_schema->get_table_name();
    if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
      LOG_WARN("failed to add table item!", K(table_item), KR(ret));
    } else {
      drop_table_arg.table_type_ = MATERIALIZED_VIEW;
      obrpc::ObDDLRes drop_table_res;
      int64_t ddl_rpc_timeout = 0;
      if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_,
          mview_table_id_, ddl_rpc_timeout))) {
        LOG_WARN("failed to get ddl rpc timeout", KR(ret));
      } else if (OB_FAIL(root_service_->get_common_rpc_proxy()
          .to(GCTX.self_addr())
          .timeout(ddl_rpc_timeout)
          .drop_table(drop_table_arg, drop_table_res))) {
        LOG_WARN("failed to drop materialized view", KR(tmp_ret), K(drop_table_arg));
      } else {
        LOG_INFO("materialized view is successfully dropped",
            K(drop_table_arg), K(drop_table_res));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cleanup())) {
      LOG_WARN("cleanup failed", KR(ret));
    }
  }
  return ret;
}

int ObBuildMViewTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (IS_NOT_INIT || root_service_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(root_service_));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("failed to report error code", KR(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(
      root_service_->get_sql_proxy(), tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", KR(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }
  return ret;
}

void ObBuildMViewTask::flt_set_task_span_tag() const
{
  LOG_INFO("flt_set_task_span_tag begin");
}

void ObBuildMViewTask::flt_set_status_span_tag() const
{
  LOG_INFO("flt_set_status_span_tag begin");
}


int ObBuildMViewTask::mview_complete_refresh(obrpc::ObMViewCompleteRefreshRes &res)
{
  int ret = OB_SUCCESS;
  int64_t ddl_rpc_timeout = 0;
  if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_,
      arg_.table_id_, ddl_rpc_timeout))) {
    LOG_WARN("failed to get ddl rpc timeout", KR(ret));
  } else if (OB_FAIL(root_service_->get_common_rpc_proxy()
      .to(GCTX.self_addr())
      .timeout(ddl_rpc_timeout)
      .mview_complete_refresh(arg_, res))) {
    LOG_WARN("failed to update mview status", KR(ret), K(arg_));
  }
  return ret;
}


int ObBuildMViewTask::start_refresh_mview_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || root_service_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(root_service_));
  } else if (ObDDLTaskStatus::START_REFRESH_MVIEW_TASK != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", KR(ret), K(task_status_));
  } else if (ATOMIC_LOAD(&mview_complete_refresh_task_id_) == 0) {
    ObMViewCompleteRefreshRes res;
    if (OB_FAIL(mview_complete_refresh(res))) {
      LOG_WARN("failed to do mview complete refresh", KR(ret));
    } else {
      LOG_INFO("start mview complete refresh", K(mview_complete_refresh_task_id_));
      if (OB_FAIL(set_mview_complete_refresh_task_id(res.task_id_))) {
        LOG_WARN("fail to set mview_complete_refresh_task_id", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_task_message())) {
      LOG_WARN("fail to update task message", KR(ret));
    }
  }

  (void)switch_status(ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH, true, ret);
  LOG_INFO("start refresh mview task finished", KR(ret), K(*this));
  return ret;
}

int ObBuildMViewTask::update_task_message()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString msg;
  common::ObArenaAllocator allocator("ObBuildMVTask");
  const int64_t serialize_param_size = get_serialize_param_size();

  if (ATOMIC_LOAD(&mview_complete_refresh_task_id_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mview_complete_refresh_task_id_ should not be 0", KR(ret), K(mview_complete_refresh_task_id_));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_param_size)))) {
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


int ObBuildMViewTask::set_mview_complete_refresh_task_id(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id should not be 0", KR(ret), K(task_id));
  } else if (!ATOMIC_CAS(&mview_complete_refresh_task_id_, 0, task_id)) {
    if (mview_complete_refresh_task_id_ != task_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mview_complete_refresh_task_id_ must = task_id", KR(ret), K(mview_complete_refresh_task_id_), K(task_id));
    }
  }
  return ret;
}

int ObBuildMViewTask::on_child_task_prepare(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id should not be 0", KR(ret), K(task_id));
  } else if (ATOMIC_LOAD(&mview_complete_refresh_task_id_) == 0) {
    LOG_INFO("mview refresh task prepare", K(task_id));
    if (OB_FAIL(set_mview_complete_refresh_task_id(task_id))) {
      LOG_WARN("fail to set mview refresh task id", KR(ret));
    }
  } else if (task_id != mview_complete_refresh_task_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mview_complete_refresh_task_id_ must = task_id", KR(ret), K(mview_complete_refresh_task_id_), K(task_id));
  }

  if (OB_SUCC(ret)) {
    if (ATOMIC_LOAD(&task_status_) == ObDDLTaskStatus::START_REFRESH_MVIEW_TASK) {
      ret = OB_EAGAIN;
      LOG_INFO("wait build mview status to promote", K(mview_complete_refresh_task_id_));
    }
  }
  return ret;
}

int ObBuildMViewTask::wait_child_task_finish()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", KR(ret), K(task_status_));
  }

  if (OB_SUCC(ret)) {
    ObAddr unused_addr;
    int64_t unused_user_msg_len = 0;
    ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
    if (OB_SUCCESS == ObDDLErrorMessageTableOperator::get_ddl_error_message(
        tenant_id_,
        mview_complete_refresh_task_id_,
        -1 /* target_object_id */,
        unused_addr,
        false /* is_ddl_retry_task */,
        *GCTX.sql_proxy_,
        error_message,
        unused_user_msg_len)) {
      ret = error_message.ret_code_;
      if (OB_SUCCESS != ret) {
        FORWARD_USER_ERROR(ret, error_message.user_message_);
      }
      state_finished = true;
    }
  }

  if (state_finished || OB_FAIL(ret)) {
    (void)switch_status(ObDDLTaskStatus::TAKE_EFFECT, true, ret);
    LOG_INFO("build_mview_task wait_child_task_finish finished", KR(ret), K(*this));
  }

  return ret;
}

int ObBuildMViewTask::enable_mview()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  ObDDLTaskStatus next_status = ObDDLTaskStatus::SUCCESS;
  if (IS_NOT_INIT || root_service_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(root_service_));
  } else if (ObDDLTaskStatus::TAKE_EFFECT != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", KR(ret), K(task_status_));
  } else {
    ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *mview_schema = nullptr;
    bool mview_table_exist = false;
    if (GCTX.is_standby_cluster()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create mview in slave cluster is not allowed", KR(ret), K(mview_table_id_));
    } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, mview_table_id_, mview_table_exist))) {
      LOG_WARN("failed to check table exist", KR(ret), K_(tenant_id), K(mview_table_id_));
    } else if (!mview_table_exist) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("mview table does not exist", KR(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
        mview_table_id_, mview_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(tenant_id_), K(mview_table_id_));
    } else if (OB_ISNULL(mview_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mview schema is null", KR(ret), K(mview_table_id_));
    } else if (mview_schema->mv_available()) {
      state_finished = true;
      LOG_INFO("build_mview_task enable_mview mview status is already valid",
          K(mview_schema->get_table_mode()));
    } else {
      ObUpdateMViewStatusArg arg;
      arg.mview_table_id_ = mview_schema->get_table_id();
      arg.mv_available_flag_ = ObMVAvailableFlag::IS_MV_AVAILABLE;
      arg.exec_tenant_id_ = tenant_id_;
      arg.in_offline_ddl_white_list_ = mview_schema->get_table_state_flag() != TABLE_STATE_NORMAL;
      int64_t ddl_rpc_timeout = 0;
      if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_,
          mview_schema->get_table_id(), ddl_rpc_timeout))) {
        LOG_WARN("failed to get ddl rpc timeout", KR(ret));
      } else if (OB_FAIL(root_service_->get_common_rpc_proxy()
          .to(GCTX.self_addr())
          .timeout(ddl_rpc_timeout)
          .update_mview_status(arg))) {
        LOG_WARN("failed to update mview status", KR(ret), K(arg));
      }
    }
  }

  if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
    state_finished = true;
    next_status = ObDDLTaskStatus::TAKE_EFFECT;
  }
  if (state_finished) {
    (void)switch_status(next_status, true, ret);
    LOG_INFO("build_mview_task enable_mview finished", KR(ret), K(*this));
  }
  return ret;
}

int ObBuildMViewTask::succ()
{
  return cleanup();
}

int ObBuildMViewTask::check_health()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || root_service_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(root_service_));
  } else if (OB_FAIL(refresh_status())) {
    LOG_WARN("failed to refresh status", KR(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("failed to refresh schema version", KR(ret));
  } else {
    ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *mview_schema = nullptr;
    bool is_mview_table_exist = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, mview_table_id_, is_mview_table_exist))) {
      LOG_WARN("failed to check mview table exist", KR(ret), K(tenant_id_), K(mview_table_id_));
    } else if (!is_mview_table_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table or mview table not exist", KR(ret), K(is_mview_table_exist));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, mview_table_id_, mview_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(tenant_id_), K(mview_table_id_));
    } else if (OB_ISNULL(mview_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("mview schema is null, but mview table exist", KR(ret), K(mview_table_id_));
    }

    if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      switch_status(new_status, false, ret);
      LOG_WARN("failed to switch status", KR(ret), K(old_status), K(new_status));
    }
  }
  if (ObDDLTaskStatus::FAIL == static_cast<ObDDLTaskStatus>(task_status_)
      || ObDDLTaskStatus::SUCCESS == static_cast<ObDDLTaskStatus>(task_status_)) {
    ret = OB_SUCCESS; // allow clean up
  }
  check_ddl_task_execute_too_long();
  return ret;
}

int ObBuildMViewTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("ObDDLTask serialize failed", KR(ret));
  } else if (OB_FAIL(arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize create index arg failed", KR(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, mview_complete_refresh_task_id_);
  }
  return ret;
}

int ObBuildMViewTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObMViewCompleteRefreshArg tmp_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", K(ret));
  } else if (OB_FAIL(tmp_arg.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize table failed", K(ret));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, tmp_arg, arg_))) {
    LOG_WARN("deep copy build mv arg failed", K(ret));
  } else {
    int64_t mview_complete_refresh_task_id = 0;
    LST_DO_CODE(OB_UNIS_DECODE, mview_complete_refresh_task_id);
    if (mview_complete_refresh_task_id > 0) {
      if (OB_FAIL(set_mview_complete_refresh_task_id(mview_complete_refresh_task_id))) {
        LOG_WARN("fail to set mview_complete_refresh_task_id", KR(ret));
      }
    }
  }
  return ret;
}

int64_t ObBuildMViewTask::get_serialize_param_size() const
{
  return arg_.get_serialize_size()
      + serialization::encoded_length_i64(mview_complete_refresh_task_id_)
      + ObDDLTask::get_serialize_param_size();
}

} // namespace rootserver
} // namespace oceanbase
