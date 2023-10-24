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
#include "ob_modify_autoinc_task.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_rpc_client.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/ob_rpc_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
using namespace oceanbase::transaction::tablelock;

ObUpdateAutoincSequenceTask::ObUpdateAutoincSequenceTask(
    const uint64_t tenant_id,
    const int64_t data_table_id,
    const int64_t dest_table_id,
    const int64_t schema_version,
    const uint64_t column_id,
    const ObObjType &orig_column_type,
    const ObSQLMode &sql_mode,
    const common::ObCurTraceId::TraceId &trace_id,
    const int64_t task_id)
    : tenant_id_(tenant_id), data_table_id_(data_table_id), dest_table_id_(dest_table_id),
      schema_version_(schema_version), column_id_(column_id), orig_column_type_(orig_column_type),
      sql_mode_(sql_mode), trace_id_(trace_id), task_id_(task_id)
{
  set_retry_times(0);
}

int ObUpdateAutoincSequenceTask::process()
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_id_guard(trace_id_);
  ObRootService *root_service = GCTX.root_service_;
  int64_t max_value = 0;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_UNLIKELY(tenant_id_ == OB_INVALID_ID || column_id_ == OB_INVALID_ID || data_table_id_ == OB_INVALID_ID
             || orig_column_type_ >= ObMaxType || dest_table_id_ == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), K(data_table_id_), K(column_id_),
                                 K(orig_column_type_), K(dest_table_id_));
  } else {
    ObDDLService &ddl_service = root_service->get_ddl_service();
    ObMultiVersionSchemaService &schema_service = ddl_service.get_schema_service();
    ObMySQLProxy &sql_proxy = ddl_service.get_sql_proxy();
    const ObTableSchema *table_schema = nullptr;
    const ObDatabaseSchema *db_schema = nullptr;
    const ObColumnSchemaV2 *column_schema = nullptr;
    ObSchemaGetterGuard schema_guard;
    ObDDLTaskKey task_key(tenant_id_, dest_table_id_, schema_version_);
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dest_table_id_, table_schema))) {
      LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(dest_table_id_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schemas should not be null", K(ret), K(table_schema));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, table_schema->get_database_id(), db_schema))) {
      LOG_WARN("failed to get database schema", K(ret), K(table_schema->get_data_table_id()), K(dest_table_id_));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, database schema must not be nullptr", K(ret));
    } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get alter column schema", K(ret), K(column_id_), KPC(table_schema));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObTimeoutCtx timeout_ctx;
        ObSqlString sql;
        sqlclient::ObMySQLResult *result = NULL;
        common::ObCommonSqlProxy *user_sql_proxy = GCTX.ddl_sql_proxy_;
        ObSessionParam session_param;
        session_param.sql_mode_ = reinterpret_cast<int64_t *>(&sql_mode_);
        session_param.ddl_info_.set_is_ddl(true);
        // if data_table_id != dest_table_id, meaning this is happening in ddl double write
        session_param.ddl_info_.set_source_table_hidden(data_table_id_ != dest_table_id_);
        ObObj obj;
        const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
        if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
          LOG_WARN("set trx timeout failed", K(ret));
        } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
          LOG_WARN("set timeout failed", K(ret));
        } else if (OB_FAIL(sql.assign_fmt("SELECT /*+no_rewrite*/ CAST(MAX(`%s`) AS SIGNED) AS MAX_VALUE FROM `%s`.`%s`",
                                    column_schema->get_column_name(),
                                    db_schema->get_database_name(),
                                    table_schema->get_table_name()))) {
          LOG_WARN("failed to assign fmt", K(ret), K(column_schema->get_column_name_str()), K(db_schema->get_database_name_str()), K(table_schema->get_table_name_str()));
        } else if (OB_FAIL(user_sql_proxy->read(res, tenant_id_, sql.ptr(), &session_param))) {
          LOG_WARN("fail to read", KR(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result failed", K(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get next", KR(ret));
        } else if (OB_FAIL(result->get_obj("MAX_VALUE", obj))) {
          LOG_WARN("fail to get result obj", K(ret));
        } else {
          max_value = MAX(0, obj.get_int());
        }
      }
    }
    if (OB_SUCCESS != (tmp_ret = root_service->get_ddl_scheduler().notify_update_autoinc_end(task_key, max_value + 1, ret))) {
      LOG_WARN("fail to finish update autoinc task", K(ret), K(max_value));
    }
    LOG_INFO("execute finish update autoinc task finish", K(ret), K(task_key), K(data_table_id_), K(column_id_), K(max_value));
  }
  return ret;
}

ObAsyncTask *ObUpdateAutoincSequenceTask::deep_copy(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObUpdateAutoincSequenceTask *new_task = nullptr;
  if (OB_ISNULL(buf) || buf_size < get_deep_copy_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), "deep_copy_size", get_deep_copy_size(), K(buf_size));
  } else {
    new_task = new (buf) ObUpdateAutoincSequenceTask(tenant_id_,
                                                     data_table_id_,
                                                     dest_table_id_,
                                                     schema_version_,
                                                     column_id_,
                                                     orig_column_type_,
                                                     sql_mode_,
                                                     trace_id_,
                                                     task_id_);
  }
  return new_task;
}

ObModifyAutoincTask::ObModifyAutoincTask()
  : ObDDLTask(ObDDLType::DDL_INVALID), lock_(), wait_trans_ctx_(), alter_table_arg_(),
    update_autoinc_job_ret_code_(INT64_MAX), update_autoinc_job_time_(0)
{
}

int ObModifyAutoincTask::init(const uint64_t tenant_id,
                              const int64_t task_id,
                              const int64_t table_id,
                              const int64_t schema_version,
                              const int64_t consumer_group_id,
                              const obrpc::ObAlterTableArg &alter_table_arg,
                              const int64_t task_status,
                              const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObModifyAutoincTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || !alter_table_arg.is_valid())
             || task_status < ObDDLTaskStatus::PREPARE || task_status > ObDDLTaskStatus::SUCCESS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), K(table_id), K(alter_table_arg), K(task_status));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, alter_table_arg, alter_table_arg_))) {
    LOG_WARN("deep copy alter table arg failed", K(ret));
  } else if (OB_FAIL(set_ddl_stmt_str(alter_table_arg_.ddl_stmt_str_))) {
    LOG_WARN("set ddl stmt str failed", K(ret));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    task_type_ = ObDDLType::DDL_MODIFY_AUTO_INCREMENT;
    object_id_ = table_id;
    target_object_id_ = table_id;
    schema_version_ = schema_version;
    consumer_group_id_ = consumer_group_id;
    task_status_ = static_cast<ObDDLTaskStatus>(task_status);
    snapshot_version_ = snapshot_version;
    tenant_id_ = tenant_id;
    task_version_ = OB_MODIFY_AUTOINC_TASK_VERSION;
    task_id_ = task_id;
    dst_tenant_id_ = tenant_id;
    dst_schema_version_ = schema_version;
    is_inited_ = true;
    ddl_tracing_.open();
  }
  return ret;
}

int ObModifyAutoincTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = task_record.object_id_;
  const uint64_t target_schema_id = task_record.target_object_id_;
  const int64_t schema_version = task_record.schema_version_;
  task_type_ = ObDDLType::DDL_MODIFY_AUTO_INCREMENT;
  int64_t pos = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObModifyAutoincTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_FAIL(deserlize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
    LOG_WARN("deserialize params from message failed", K(ret));
  } else if (OB_FAIL(set_ddl_stmt_str(task_record.ddl_stmt_str_))) {
    LOG_WARN("set ddl stmt str failed", K(ret));
  } else {
    object_id_ = data_table_id;
    target_object_id_ = target_schema_id;
    schema_version_ = schema_version;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    tenant_id_ = task_record.tenant_id_;
    task_id_ = task_record.task_id_;
    ret_code_ = task_record.ret_code_;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    is_inited_ = true;

    // set up span during recover task
    ddl_tracing_.open_for_recovery();
  }
  return ret;
}

int ObModifyAutoincTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObModifyAutoincTask has not been inited", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check health failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch(task_status_) {
      case ObDDLTaskStatus::WAIT_TRANS_END: {
        if (OB_FAIL(wait_trans_end())) {
          LOG_WARN("wait trans end failed", K(ret), K(*this));
        }
        break;
      }
      case ObDDLTaskStatus::MODIFY_AUTOINC: {
        if (OB_FAIL(modify_autoinc())) {
          LOG_WARN("update schema failed", K(ret), K(*this));
        }
        break;
      }
      case ObDDLTaskStatus::FAIL: {
        if (OB_FAIL(fail())) {
          LOG_WARN("process fail failed", K(ret), K(*this));
        }
        break;
      }
      case ObDDLTaskStatus::SUCCESS: {
        if (OB_FAIL(success())) {
          LOG_WARN("process success failed", K(ret), K(*this));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ObModifyAutoincTask status", K(ret), K(task_status_), K(*this));
        break;
      }
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

int ObModifyAutoincTask::unlock_table()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObModifyAutoincTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(trans.start(&root_service->get_sql_proxy(), tenant_id_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(ObDDLLock::unlock_for_offline_ddl(tenant_id_, object_id_, ObTableLockOwnerID(task_id_), trans))) {
    LOG_WARN("failed to unlock table", K(ret));
  }

  bool commit = (OB_SUCCESS == ret);
  int tmp_ret = trans.end(commit);
  if (OB_SUCCESS != tmp_ret) {
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  return ret;
}

int ObModifyAutoincTask::modify_autoinc()
{
  int ret = OB_SUCCESS;
  bool is_update_autoinc_end = false;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObModifyAutoincTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(check_update_autoinc_end(is_update_autoinc_end))) {
    LOG_WARN("fail to check update autoinc end", K(ret));
  } else if (!is_update_autoinc_end && update_autoinc_job_time_ == 0) {
    int64_t refreshed_schema_version = 0;
    const ObTableSchema *orig_table_schema = nullptr;
    ObTableSchema new_table_schema;
    ObSchemaGetterGuard schema_guard;
    ObDDLService &ddl_service = root_service->get_ddl_service();
    ObMultiVersionSchemaService &schema_service = ddl_service.get_schema_service();
    const AlterTableSchema &alter_table_schema = alter_table_arg_.alter_table_schema_;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, orig_table_schema))) {
      LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_ISNULL(orig_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("cannot find orig table", K(ret), K(alter_table_arg_));
    } else {
      int64_t alter_column_id = 0;

      ObTableSchema::const_column_iterator iter = alter_table_schema.column_begin();
      ObTableSchema::const_column_iterator iter_end = alter_table_schema.column_end();
      AlterColumnSchema *alter_column_schema = nullptr;
      for(; OB_SUCC(ret) && iter != iter_end; iter++) {
        if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*iter))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter is NULL", K(ret));
        } else {
          const ObString &orig_column_name = alter_column_schema->get_origin_column_name();
          const ObColumnSchemaV2 *orig_column_schema = orig_table_schema->get_column_schema(orig_column_name);
          if (alter_column_schema->is_autoincrement() && !orig_column_schema->is_autoincrement()) {
            alter_column_id = orig_column_schema->get_column_id();
            break; // there can only be one autoinc column
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(alter_column_id == OB_INVALID_ID)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid alter column info", K(alter_column_id));
        } else {
          const AlterTableSchema &alter_table_schema = alter_table_arg_.alter_table_schema_;
          ObObjType column_type = orig_table_schema->get_column_schema(alter_column_id)->get_data_type();
          ObUpdateAutoincSequenceTask task(tenant_id_, object_id_, target_object_id_, schema_version_,
                                          alter_column_id, column_type, alter_table_arg_.sql_mode_,
                                          trace_id_, task_id_);
          if (OB_FAIL(root_service->submit_ddl_single_replica_build_task(task))) {
            LOG_WARN("fail to submit ObUpdateAutoincSequenceTask", K(ret));
          } else {
            update_autoinc_job_time_ = ObTimeUtility::current_time();
            LOG_INFO("submit ObUpdateAutoincSequenceTask success", K(object_id_), K(alter_column_id));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) || is_update_autoinc_end) {
    if (OB_FAIL(switch_status(ObDDLTaskStatus::WAIT_TRANS_END, true, ret))) {
      LOG_WARN("fail to switch status", K(ret));
    }
  }
  return ret;
}

// TODO(shuangcan): abstract this function
int ObModifyAutoincTask::wait_trans_end()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDDLTaskStatus new_status = WAIT_TRANS_END;
  const ObDDLTaskStatus next_task_status = SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObModifyAutoincTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (snapshot_version_ > 0) {
    new_status = next_task_status;
  }

  if (OB_SUCC(ret) && new_status != next_task_status && !wait_trans_ctx_.is_inited()) {
    const ObTableSchema *updated_table_schema = nullptr;
    ObSchemaGetterGuard schema_guard;
    ObDDLService &ddl_service = root_service->get_ddl_service();
    ObMultiVersionSchemaService &schema_service = ddl_service.get_schema_service();
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, updated_table_schema))) {
      LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(object_id_));
    } else if (OB_ISNULL(updated_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("cannot find orig table", K(ret), K(alter_table_arg_));
    } else if (OB_FAIL(wait_trans_ctx_.init(tenant_id_,
                                            object_id_,
                                            ObDDLWaitTransEndCtx::WAIT_SCHEMA_TRANS,
                                            updated_table_schema->get_schema_version()))) {
      LOG_WARN("fail to init wait trans ctx", K(ret));
    }
  }
  // try wait transaction end
  if (OB_SUCC(ret) && new_status != next_task_status && snapshot_version_ <= 0) {
    bool is_trans_end = false;
    if (OB_FAIL(wait_trans_ctx_.try_wait(is_trans_end, snapshot_version_))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail to try wait transaction", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      new_status = next_task_status;
    }
  }

  if (new_status != ObDDLTaskStatus::WAIT_TRANS_END || OB_FAIL(ret)) {
    if (OB_FAIL(switch_status(new_status, true, ret))) {
      LOG_WARN("fail to switch task status", K(ret));
    }
  }
  return ret;
}

int ObModifyAutoincTask::set_schema_available()
{
  int ret = OB_SUCCESS;
  int64_t tablet_count = 0;
  int64_t rpc_timeout = 0;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObModifyAutoincTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else {
    ObSArray<uint64_t> unused_ids;
    alter_table_arg_.ddl_task_type_ = share::UPDATE_AUTOINC_SCHEMA;
    alter_table_arg_.alter_table_schema_.set_tenant_id(tenant_id_);
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
      LOG_WARN("get rpc timeout failed", K(ret));
    } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
        execute_ddl_task(alter_table_arg_, unused_ids))) {
      LOG_WARN("alter table failed", K(ret));
    }
  }
  return ret;
}

int ObModifyAutoincTask::rollback_schema()
{
  int ret = OB_SUCCESS;
  int64_t tablet_count = 0;
  int64_t rpc_timeout = 0;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObModifyAutoincTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else {
    ObArenaAllocator allocator;
    SMART_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
      if (OB_FAIL(deep_copy_table_arg(allocator, alter_table_arg_, alter_table_arg))) {
        LOG_WARN("deep copy table arg failed", K(ret));
      } else {
        ObSArray<uint64_t> unused_ids;
        alter_table_arg.ddl_task_type_ = share::UPDATE_AUTOINC_SCHEMA;
        alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id_);
        alter_table_arg.alter_table_schema_.reset_column_info();
        if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
          LOG_WARN("get rpc timeout failed", K(ret));
        } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
            execute_ddl_task(alter_table_arg, unused_ids))) {
          LOG_WARN("alter table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObModifyAutoincTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rollback_schema())) {
    LOG_WARN("fail to set schema available", K(ret));
  } else if (OB_FAIL(unlock_table())) {
    LOG_WARN("fail to unlock table", K(ret));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("clean up failed", K(ret));
  } else {
    need_retry_ = false;
  }
  return ret;
}

int ObModifyAutoincTask::success()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_schema_available())) {
    LOG_WARN("fail to set schema available", K(ret));
  } else if (OB_FAIL(unlock_table())) {
    LOG_WARN("fail to unlock table", K(ret));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("clean up failed", K(ret));
  } else {
    need_retry_ = false;
  }
  return ret;
}

int ObModifyAutoincTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObModifyAutoincTask has not been inited", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else if (OB_FAIL(remove_task_record())) {
    LOG_WARN("remove task record failed", K(ret));
  }
  return ret;
}

int ObModifyAutoincTask::check_update_autoinc_end(bool &is_end)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == update_autoinc_job_ret_code_) {
    // not finish
  } else {
    is_end = true;
    ret_code_ = update_autoinc_job_ret_code_;
    ret = ret_code_;
  }
  return ret;
}

int ObModifyAutoincTask::notify_update_autoinc_finish(const uint64_t autoinc_val, const int ret_code)
{
  int ret = OB_SUCCESS;
  update_autoinc_job_ret_code_ = ret_code;
  alter_table_arg_.alter_table_schema_.set_auto_increment(autoinc_val);
  return ret;
}

int ObModifyAutoincTask::check_health()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret));
  } else if (!root_service->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service, do not need retry", K(ret), K(object_id_), K(target_object_id_));
    need_retry_ = false;
  } else if (OB_FAIL(refresh_status())) { // refresh task status
    LOG_WARN("refresh status failed", K(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else {
    ObMultiVersionSchemaService &schema_service = root_service->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;
    bool is_source_table_exist = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("get tanant schema guard failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, object_id_, is_source_table_exist))) {
      LOG_WARN("check data table exist failed", K(ret), K_(tenant_id), K(object_id_));
    } else if (!is_source_table_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table not exist", K(ret), K(is_source_table_exist));
    }
    if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)) {
      const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
      const ObDDLTaskStatus new_status = ObDDLTaskStatus::FAIL;
      switch_status(new_status, false, ret);
      LOG_WARN("switch status to build_failed", K(ret), K(old_status), K(new_status));
    }
    if (ObDDLTaskStatus::FAIL == static_cast<ObDDLTaskStatus>(task_status_)
        || ObDDLTaskStatus::SUCCESS == static_cast<ObDDLTaskStatus>(task_status_)) {
      ret = OB_SUCCESS; // allow clean up
    }
  }
  return ret;
}

int ObModifyAutoincTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize ObDDLTask", K(ret));
  } else if (OB_FAIL(alter_table_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize table arg failed", K(ret));
  }
  return ret;
}

int ObModifyAutoincTask::deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  obrpc::ObAlterTableArg tmp_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserlize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("ObDDLTask deserlize failed", K(ret));
  } else if (OB_FAIL(tmp_arg.deserialize(buf, data_len, pos))) {
    LOG_WARN("serialize table failed", K(ret));
  } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(task_type_, tenant_id, tmp_arg))) {
    LOG_WARN("replace user tenant id failed", K(ret), K(tenant_id), K(tmp_arg));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, tmp_arg, alter_table_arg_))) {
    LOG_WARN("deep copy table arg failed", K(ret));
  }
  return ret;
}

int64_t ObModifyAutoincTask::get_serialize_param_size() const
{
  return alter_table_arg_.get_serialize_size() + ObDDLTask::get_serialize_param_size();
}

void ObModifyAutoincTask::flt_set_task_span_tag() const
{
  FLT_SET_TAG(ddl_task_id, task_id_, ddl_parent_task_id, parent_task_id_,
              ddl_data_table_id, object_id_, ddl_schema_version, schema_version_,
              ddl_snapshot_version, snapshot_version_, ddl_ret_code, ret_code_);
}

void ObModifyAutoincTask::flt_set_status_span_tag() const
{
  switch (task_status_) {
  case ObDDLTaskStatus::WAIT_TRANS_END: {
    FLT_SET_TAG(ddl_data_table_id, object_id_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::OBTAIN_SNAPSHOT: {
    FLT_SET_TAG(ddl_data_table_id, object_id_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::MODIFY_AUTOINC: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
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
