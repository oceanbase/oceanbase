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
#include "ob_constraint_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_ddl_common.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "share/scn.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

ObCheckConstraintValidationTask::ObCheckConstraintValidationTask(
    const uint64_t tenant_id,
    const int64_t data_table_id,
    const int64_t constraint_id,
    const int64_t target_object_id,
    const int64_t schema_version,
    const common::ObCurTraceId::TraceId &trace_id,
    const int64_t task_id,
    const bool check_table_empty,
    const obrpc::ObAlterTableArg::AlterConstraintType alter_constraint_type)
    : tenant_id_(tenant_id), data_table_id_(data_table_id), constraint_id_(constraint_id),
      target_object_id_(target_object_id), schema_version_(schema_version), trace_id_(trace_id), task_id_(task_id),
      check_table_empty_(check_table_empty), alter_constraint_type_(alter_constraint_type)
{
  set_retry_times(0); // do not retry
}

int ObCheckConstraintValidationTask::process()
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_id_guard(trace_id_);
  ObRootService *root_service = GCTX.root_service_;
  const ObConstraint *constraint = nullptr;
  bool is_oracle_mode = false;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  int tmp_ret = OB_SUCCESS;
  ObTabletID unused_tablet_id;
  ObDDLTaskKey task_key(tenant_id_, target_object_id_, schema_version_);
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(data_table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, VALIDATE_CONSTRAINT_OR_FOREIGN_KEY_TASK_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (!check_table_empty_ && OB_ISNULL(constraint = table_schema->get_constraint(constraint_id_))) {
    ret = OB_ERR_CONTRAINT_NOT_FOUND;
    LOG_WARN("error unexpected, can not get constraint", K(ret));
  } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check tenant is oracle mode failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, table_schema->get_database_id(), database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K_(tenant_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_SYS;
    LOG_WARN("get database schema failed", K(ret));
  } else {
    const ObString &check_expr_str = check_table_empty_ ? "1 != 1"
                                     : constraint->get_check_expr_str();
    const ObString &database_name = database_schema->get_database_name_str();
    const ObString &table_name = table_schema->get_table_name_str();
    ObSqlString sql_string;
    ObSessionParam session_param;
    session_param.sql_mode_ = nullptr;
    session_param.tz_info_wrap_ = nullptr;
    session_param.ddl_info_.set_is_ddl(true);
    session_param.ddl_info_.set_source_table_hidden(table_schema->is_user_hidden_table());
    session_param.ddl_info_.set_dest_table_hidden(false);
    ObTimeoutCtx timeout_ctx;
    const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString ddl_schema_hint_str;
      if (check_expr_str.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check_expr_str is empty", K(ret));
      } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set trx timeout failed", K(ret));
      } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set timeout failed", K(ret));
      } else if (OB_FAIL(ObDDLUtil::generate_ddl_schema_hint_str(table_name, table_schema->get_schema_version(), is_oracle_mode, ddl_schema_hint_str))) {
        LOG_WARN("failed to generate ddl schema hint str", K(ret));
      } else if (OB_FAIL(ddl_schema_hint_str.append_fmt(is_oracle_mode ?
                  " INDEX(\"%.*s\".\"%.*s\" PRIMARY)" : " INDEX(`%.*s`.`%.*s` PRIMARY)",
            static_cast<int>(database_name.length()),
            database_name.ptr(),
            static_cast<int>(table_name.length()),
            table_name.ptr()))) {
        LOG_WARN("fail to assign index hint", K(ret));
      } else if (OB_FAIL(sql_string.assign_fmt(
          is_oracle_mode ?
              "SELECT /*+ %.*s */ 1 FROM \"%.*s\".\"%.*s\" WHERE NOT (%.*s) AND ROWNUM = 1" // for oracle mode
              : "SELECT /*+ %.*s */ 1 FROM `%.*s`.`%.*s` WHERE NOT (%.*s) LIMIT 1", // for mysql mode
                 static_cast<int>(ddl_schema_hint_str.length()), ddl_schema_hint_str.ptr(),
                 static_cast<int>(database_name.length()), database_name.ptr(),
                 static_cast<int>(table_name.length()), table_name.ptr(),
                 static_cast<int>(check_expr_str.length()), check_expr_str.ptr()))) {
        LOG_WARN("fail to assign format", K(ret));
      }

      DEBUG_SYNC(BEFORE_CHECK_CONSTRAINT_VALID_SEND_SQL);

      if (OB_FAIL(ret)) {
      } else if (is_oracle_mode && OB_FAIL(GCTX.ddl_oracle_sql_proxy_->read(res, table_schema->get_tenant_id(), sql_string.ptr(), &session_param))) {
        LOG_WARN("execute sql failed", K(ret), K(sql_string.ptr()));
      } else if (!is_oracle_mode && OB_FAIL(GCTX.ddl_sql_proxy_->read(res, table_schema->get_tenant_id(), sql_string.ptr(), &session_param))) {
        LOG_WARN("execute sql failed", K(ret), K(sql_string.ptr()));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(ret), K(table_schema->get_tenant_id()), K(sql_string));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterate next result fail", K(ret), K(sql_string));
        }
      } else {
        // target table has at least one record that violates the constraint
        if (check_table_empty_) {
          ret = OB_ERR_TABLE_ADD_NOT_NULL_COLUMN_NOT_EMPTY;
        } else if (CONSTRAINT_TYPE_CHECK == constraint->get_constraint_type()) {
          ret = OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED;
        } else {
          ret = obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_constraint_type_ ?
                OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED :
                (is_oracle_mode ? OB_ERR_NOT_NULL_CONSTRAINT_VIOLATED : OB_ER_INVALID_USE_OF_NULL);
        }
        if (!is_oracle_mode && OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED == ret) {
          // in mysql mode, change errcode from OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED to OB_ERR_CHECK_CONSTRAINT_VIOLATED
          ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
        }
        LOG_WARN("old data is not valid for this new check constraint", K(ret), K(sql_string));
      }
    }
  }
  ObDDLTaskInfo info;
  if (OB_SUCCESS != (tmp_ret = root_service->get_ddl_scheduler().on_sstable_complement_job_reply(unused_tablet_id, task_key, 1L/*unused snapshot version*/, 1L/*unused execution id*/, ret, info))) {
    LOG_WARN("fail to finish check constraint task", K(ret), K(tmp_ret));
  }
  char table_id_buffer[256];
  snprintf(table_id_buffer, sizeof(table_id_buffer), "data_table_id:%ld, target_object_id:%ld",
            data_table_id_, target_object_id_);
  ROOTSERVICE_EVENT_ADD("ddl scheduler", "check constraint validation task process finish",
    K_(tenant_id),
    "ret", ret,
    K_(trace_id),
    K_(task_id),
    K_(constraint_id),
    K_(schema_version),
    table_id_buffer);
  LOG_INFO("process check constraint validation task", "ddl_event_info", ObDDLEventInfo(), K(task_id_), K(constraint_id_));
  return ret;
}

ObAsyncTask *ObCheckConstraintValidationTask::deep_copy(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObCheckConstraintValidationTask *new_task = nullptr;
  if (OB_ISNULL(buf) || buf_size < get_deep_copy_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), "deep_copy_size", get_deep_copy_size(), K(buf_size));
  } else {
    new_task = new (buf) ObCheckConstraintValidationTask(tenant_id_, data_table_id_, constraint_id_, target_object_id_,
      schema_version_, trace_id_, task_id_, check_table_empty_, alter_constraint_type_);
  }
  return new_task;
}

ObForeignKeyConstraintValidationTask::ObForeignKeyConstraintValidationTask(
    const uint64_t tenant_id,
    const int64_t data_table_id,
    const int64_t foregin_key_id,
    const int64_t schema_version,
    const common::ObCurTraceId::TraceId &trace_id,
    const int64_t task_id)
    : tenant_id_(tenant_id), data_table_id_(data_table_id), foregin_key_id_(foregin_key_id), schema_version_(schema_version), trace_id_(trace_id), task_id_(task_id)
{
  set_retry_times(0); // do not retry
}

int ObForeignKeyConstraintValidationTask::process()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, VALIDATE_CONSTRAINT_OR_FOREIGN_KEY_TASK_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else {
    ObTabletID unused_tablet_id;
    ObDDLTaskKey task_key(tenant_id_, foregin_key_id_, schema_version_);
    ObDDLTaskInfo info;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(check_fk_by_send_sql())) {
      LOG_WARN("failed to check fk", K(ret));
    }
    if (OB_SUCCESS != (tmp_ret = root_service->get_ddl_scheduler().on_sstable_complement_job_reply(unused_tablet_id, task_key, 1L/*unused snapshot version*/, 1L/*unused execution id*/, ret, info))) {
      LOG_WARN("fail to finish check constraint task", K(ret));
    }
    LOG_INFO("execute check foreign key task finish", K(ret), "ddl_event_info", ObDDLEventInfo(), K(task_key), K(data_table_id_), K(foregin_key_id_));
  }
  ROOTSERVICE_EVENT_ADD("ddl scheduler", "foreign key constraint validation task process finish",
    K_(tenant_id),
    "ret", ret,
    K_(trace_id),
    K_(task_id),
    K_(foregin_key_id),
    K_(schema_version),
    data_table_id_);
  return ret;
}

int ObForeignKeyConstraintValidationTask::check_fk_by_send_sql() const
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_id_guard(trace_id_);
  const ObConstraint *constraint = nullptr;
  bool is_oracle_mode = false;
  ObSchemaGetterGuard schema_guard;
  // notice that data_table_id_ may be parent_table_id or child_table_id,
  // for example: data_table_id will be parent_table_id when altering non-ref column type of parent table.
  //
  const ObTableSchema *data_table_schema = nullptr;
  const ObDatabaseSchema *data_database_schema = nullptr;
  const ObTableSchema *child_table_schema = nullptr;
  const ObDatabaseSchema *child_database_schema = nullptr;
  const ObTableSchema *parent_table_schema = nullptr;
  const ObDatabaseSchema *parent_database_schema = nullptr;
  ObForeignKeyInfo fk_info;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id_, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(data_table_id_));
  } else if (OB_ISNULL(data_table_schema) || data_table_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, data_table_schema->get_database_id(), data_database_schema))) {
    LOG_WARN("failed to get database schema", K(ret));
  } else if (OB_ISNULL(data_database_schema) || data_database_schema->is_in_recyclebin()) {
    // ob drop database to recyclebin won't drop its tables to recyclebin, but will drop fk of its tables directly.
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("database schema not exist", K(ret));
  } else if (OB_FAIL(get_foreign_key_info(data_table_schema, foregin_key_id_, fk_info))) {
    LOG_WARN("get foreign key info failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, fk_info.parent_table_id_, parent_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(fk_info.parent_table_id_));
  } else if (OB_ISNULL(parent_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, fk_info.child_table_id_, child_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(fk_info.child_table_id_));
  } else if (OB_ISNULL(child_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret));
  } else if (OB_FAIL(child_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check tenant is oracle mode failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, child_table_schema->get_database_id(), child_database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K_(tenant_id));
  } else if (OB_ISNULL(child_database_schema)) {
    ret = OB_ERR_SYS;
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, parent_table_schema->get_database_id(), parent_database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K_(tenant_id));
  } else if (OB_ISNULL(parent_database_schema)) {
    ret = OB_ERR_SYS;
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_FAIL(check_fk_constraint_data_valid(*child_table_schema, *child_database_schema, *parent_table_schema, *parent_database_schema, fk_info, is_oracle_mode))) {
    LOG_WARN("check fk constraint data valid failed", K(ret));
  }
  return ret;
}

int ObForeignKeyConstraintValidationTask::get_foreign_key_info(const ObTableSchema *table_schema, const int64_t foreign_key_id, ObForeignKeyInfo &fk_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(table_schema));
  } else {
    const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema->get_foreign_key_infos();
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos.count() && !found; ++i) {
      if (foreign_key_id == fk_infos.at(i).foreign_key_id_) {
        fk_info = fk_infos.at(i);
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObForeignKeyConstraintValidationTask::get_column_names(
    const ObTableSchema &table_schema,
    const ObIArray<uint64_t> &column_ids,
    ObIArray<ObString> &column_name_str) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_schema.is_valid() || column_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_schema), K(column_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const uint64_t column_id = column_ids.at(i);
      const ObColumnSchemaV2 *column_schema = nullptr;
      if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_SYS;
        LOG_WARN("get column schema failed", K(ret), K(column_id));
      } else if (OB_FAIL(column_name_str.push_back(column_schema->get_column_name_str()))) {
        LOG_WARN("push back column name failed", K(ret));
      }
    }
  }
  return ret;
}

int ObForeignKeyConstraintValidationTask::check_fk_constraint_data_valid(
    const ObTableSchema &child_table_schema,
    const ObDatabaseSchema &child_database_schema,
    const ObTableSchema &parent_table_schema,
    const ObDatabaseSchema &parent_database_schema,
    const ObForeignKeyInfo &fk_info,
    const bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> child_column_names;
  ObArray<ObString> parent_column_names;
  ObSqlString sql_string;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!child_table_schema.is_valid() || !child_database_schema.is_valid() || !parent_table_schema.is_valid() || !parent_database_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(child_table_schema), K(child_database_schema), K(parent_table_schema), K(parent_database_schema));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(get_column_names(child_table_schema, fk_info.child_column_ids_, child_column_names))) {
    LOG_WARN("get column names failed", K(ret));
  } else if (OB_FAIL(get_column_names(parent_table_schema, fk_info.parent_column_ids_, parent_column_names))) {
    LOG_WARN("get column names failed", K(ret));
  } else {
    ObTabletID unused_tablet_id;
    int tmp_ret = OB_SUCCESS;
    ObSessionParam session_param;
    ObTimeoutCtx timeout_ctx;
    session_param.sql_mode_ = nullptr;
    session_param.tz_info_wrap_ = nullptr;
    session_param.ddl_info_.set_is_ddl(true);
    session_param.ddl_info_.set_source_table_hidden(child_table_schema.is_user_hidden_table());
    session_param.ddl_info_.set_dest_table_hidden(parent_table_schema.is_user_hidden_table());
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      int64_t i = 0;
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString child_ddl_schema_hint_str;
      ObSqlString parent_ddl_schema_hint_str;
      const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
      // print str like "select c1, c2 from db.t2 where c1 is not null and c2 is not null minus select c3, c4 from db.t1"
      if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set trx timeout failed", K(ret));
      } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
        LOG_WARN("set timeout failed", K(ret));
      } else if (OB_FAIL(ObDDLUtil::generate_ddl_schema_hint_str(child_table_schema.get_table_name_str(), child_table_schema.get_schema_version(), is_oracle_mode, child_ddl_schema_hint_str))) {
        LOG_WARN("failed to generate ddl schema hint", K(ret));
      } else if (OB_FAIL(ObDDLUtil::generate_ddl_schema_hint_str(parent_table_schema.get_table_name_str(), parent_table_schema.get_schema_version(), is_oracle_mode, parent_ddl_schema_hint_str))) {
        LOG_WARN("failed to generate ddl schema hint", K(ret));
      } else if (OB_FAIL(child_ddl_schema_hint_str.append_fmt(is_oracle_mode ?
                  " INDEX(\"%.*s\".\"%.*s\" PRIMARY)" : " INDEX(`%.*s`.`%.*s` PRIMARY)",
            static_cast<int>(child_database_schema.get_database_name_str().length()),
            child_database_schema.get_database_name_str().ptr(),
            static_cast<int>(child_table_schema.get_table_name_str().length()),
            child_table_schema.get_table_name_str().ptr()))) {
        LOG_WARN("fail to assign index hint", K(ret));
      } else if (OB_FAIL(parent_ddl_schema_hint_str.append_fmt(is_oracle_mode ?
                  " INDEX(\"%.*s\".\"%.*s\" PRIMARY)" : " INDEX(`%.*s`.`%.*s` PRIMARY)",
            static_cast<int>(parent_database_schema.get_database_name_str().length()),
            parent_database_schema.get_database_name_str().ptr(),
            static_cast<int>(parent_table_schema.get_table_name_str().length()),
            parent_table_schema.get_table_name_str().ptr()))) {
        LOG_WARN("fail to assign index hint", K(ret));
      }
      if (OB_SUCC(ret)) {
        // print "select "
        if (OB_FAIL(sql_string.assign_fmt("SELECT /*+ %.*s */",
            static_cast<int>(child_ddl_schema_hint_str.length()), child_ddl_schema_hint_str.ptr()))) {
          LOG_WARN("fail to assign format", K(ret));
        }
        // print "c1, "
        for (i = 0; OB_SUCC(ret) && i < child_column_names.count() - 1; ++i) {
          if (OB_FAIL(sql_string.append_fmt(is_oracle_mode ? "\"%.*s\", " : "`%.*s`, ",
                  static_cast<int>(child_column_names.at(i).length()),
                  child_column_names.at(i).ptr()))) {
            LOG_WARN("fail to append format", K(ret));
          }
        }
        // print "c2 from db.t2 where "
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sql_string.append_fmt(is_oracle_mode ?
                  "\"%.*s\" FROM \"%.*s\".\"%.*s\" WHERE " : "`%.*s` FROM `%.*s`.`%.*s` WHERE ",
                  static_cast<int>(child_column_names.at(i).length()),
                  child_column_names.at(i).ptr(),
                  static_cast<int>(child_database_schema.get_database_name_str().length()),
                  child_database_schema.get_database_name_str().ptr(),
                  static_cast<int>(child_table_schema.get_table_name_str().length()),
                  child_table_schema.get_table_name_str().ptr()))) {
            LOG_WARN("fail to append format", K(ret));
          }
        }
        // print "c1 is not null and "
        for (i = 0; OB_SUCC(ret) && i < child_column_names.count() - 1; ++i) {
          if (OB_FAIL(sql_string.append_fmt(is_oracle_mode ?
                  "\"%.*s\" IS NOT NULL AND " : "`%.*s` IS NOT NULL AND ",
                  static_cast<int>(child_column_names.at(i).length()),
                  child_column_names.at(i).ptr()))) {
            LOG_WARN("fail to append format", K(ret));
          }
        }
        // print "c2 is not null minus select "
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sql_string.append_fmt(is_oracle_mode ?
                  "\"%.*s\" IS NOT NULL MINUS SELECT /*+ %.*s */ " : "`%.*s` IS NOT NULL MINUS SELECT /*+ %.*s */ ",
                  static_cast<int>(child_column_names.at(i).length()), child_column_names.at(i).ptr(),
                  static_cast<int>(parent_ddl_schema_hint_str.length()), parent_ddl_schema_hint_str.ptr()))) {
            LOG_WARN("fail to append format", K(ret));
          }
        }
        // print "c3, "
        for (i = 0; OB_SUCC(ret) && i < parent_column_names.count() - 1; ++i) {
          if (OB_FAIL(sql_string.append_fmt(is_oracle_mode ? "\"%.*s\", " : "`%.*s`, ",
                  static_cast<int>(parent_column_names.at(i).length()),
                  parent_column_names.at(i).ptr()))) {
            LOG_WARN("fail to append format", K(ret));
          }
        }
        // print "c4 from db.t1"
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sql_string.append_fmt(is_oracle_mode ?
                  "\"%.*s\" FROM \"%.*s\".\"%.*s\"" : "`%.*s` FROM `%.*s`.`%.*s`",
                  static_cast<int>(parent_column_names.at(i).length()),
                  parent_column_names.at(i).ptr(),
                  static_cast<int>(parent_database_schema.get_database_name_str().length()),
                  parent_database_schema.get_database_name_str().ptr(),
                  static_cast<int>(parent_table_schema.get_table_name_str().length()),
                  parent_table_schema.get_table_name_str().ptr()))) {
            LOG_WARN("fail to append format", K(ret));
          }
        }
      }
      // check data valid
      if (OB_SUCC(ret)) {
        ObCommonSqlProxy *sql_proxy = nullptr;
        if (is_oracle_mode) {
          sql_proxy = &root_service->get_oracle_sql_proxy();
        } else {
          sql_proxy = &root_service->get_sql_proxy();
        }

        DEBUG_SYNC(BEFORE_CHECK_FK_DATA_VALID_SEND_SQL);

        if (OB_FAIL(sql_proxy->read(res, child_table_schema.get_tenant_id(), sql_string.ptr(), &session_param))) {
          LOG_WARN("execute sql failed", K(ret), K(sql_string.ptr()));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("execute sql failed", K(ret), K(child_table_schema.get_tenant_id()), K(sql_string));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("iterate next result fail", K(ret), K(sql_string));
          }
        } else {
          ret = OB_ERR_ORPHANED_CHILD_RECORD_EXISTS;
          LOG_WARN("add fk failed, because the table has orphaned child records",
              K(ret), K(sql_string));
        }
      }
    }
  }
  return ret;
}

ObAsyncTask *ObForeignKeyConstraintValidationTask::deep_copy(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObForeignKeyConstraintValidationTask *new_task = nullptr;
  if (nullptr == buf || buf_size < get_deep_copy_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), "deep_copy_size", get_deep_copy_size());
  } else {
    new_task = new (buf) ObForeignKeyConstraintValidationTask(tenant_id_, data_table_id_, foregin_key_id_, schema_version_, trace_id_, task_id_);
  }
  return new_task;
}

ObConstraintTask::ObConstraintTask()
  : ObDDLTask(ObDDLType::DDL_INVALID), lock_(), wait_trans_ctx_(),
    alter_table_arg_(), root_service_(nullptr), check_job_ret_code_(INT64_MAX), check_replica_request_time_(0),
    snapshot_held_(false)
{
}

int ObConstraintTask::init(
    const int64_t task_id,
    const share::schema::ObTableSchema *table_schema,
    const int64_t object_id,
    const ObDDLType type,
    const int64_t schema_version,
    const ObAlterTableArg &alter_table_arg,
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
    const int64_t parent_task_id,
    const int64_t status,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  const uint64_t tenant_id = alter_table_arg.exec_tenant_id_;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObConstraintTask has been inited twice", K(ret));
  } else if (OB_UNLIKELY(task_id <= 0 || nullptr == table_schema || OB_INVALID_ID == object_id
        || (ObDDLType::DDL_CHECK_CONSTRAINT != type && ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT != type
            && ObDDLType::DDL_ADD_NOT_NULL_COLUMN != type)
        || schema_version < 0 || !alter_table_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), K(table_schema), K(object_id), K(type), K(schema_version), K(alter_table_arg));
  } else if (OB_FAIL(deep_copy_table_arg(allocator_, alter_table_arg, alter_table_arg_))) {
    LOG_WARN("init alter constraint param failed", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else {
    set_gmt_create(ObTimeUtility::current_time());
    object_id_ = table_schema->get_table_id();
    target_object_id_ = object_id;
    tenant_id_ = tenant_id;
    task_status_ = static_cast<ObDDLTaskStatus>(status);
    task_type_ = type;
    snapshot_version_ = snapshot_version;
    schema_version_ = schema_version;
    root_service_ = root_service;
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
    consumer_group_id_ = consumer_group_id;
    sub_task_trace_id_ = sub_task_trace_id;
    task_version_ = OB_CONSTRAINT_TASK_VERSION;
    is_table_hidden_ = table_schema->is_user_hidden_table();
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    is_inited_ = true;
    ddl_tracing_.open();
  }
  return ret;
}

int ObConstraintTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = task_record.object_id_;
  const uint64_t target_object_id = task_record.target_object_id_;
  const int64_t schema_version = task_record.schema_version_;
  task_type_ = task_record.ddl_type_;
  ObSchemaGetterGuard schema_guard;
  ObRootService *root_service = GCTX.root_service_;
  const ObTableSchema *table_schema = nullptr;
  int64_t pos = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObConstraintTask has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(task_record.tenant_id_, schema_guard, schema_version))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(task_record.tenant_id_, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(task_record.tenant_id_), K(table_id));
  } else if (nullptr == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table schema must not be nullptr", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(), task_record.message_.length(), pos))) {
    LOG_WARN("deserialize params from message failed", K(ret));
  } else {
    object_id_ = table_id;
    target_object_id_ = target_object_id;
    tenant_id_ = task_record.tenant_id_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    schema_version_ = task_record.schema_version_;
    root_service_ = root_service;
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    is_table_hidden_ = table_schema->is_user_hidden_table();
    ret_code_ = task_record.ret_code_;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    is_inited_ = true;

    // set up span during recover task
    ddl_tracing_.open_for_recovery();
  }
  return ret;
}

int ObConstraintTask::hold_snapshot(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObDDLService &ddl_service = root_service_->get_ddl_service();
  ObSEArray<ObTabletID, 1> tablet_ids;
  SCN snapshot_scn;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(snapshot_version));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_HOLD_SNAPSHOT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot_version))) {
    LOG_WARN("failed to convert", K(snapshot_version), K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(object_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(object_id_), K(target_object_id_), KP(table_schema));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, tablet_ids))) {
    LOG_WARN("failed to get tablet snapshots", K(ret));
  } else if (table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob meta table snapshot", K(ret));
  } else if (table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob piece table snapshot", K(ret));
  } else if (OB_FAIL(ddl_service.get_snapshot_mgr().batch_acquire_snapshot(
          ddl_service.get_sql_proxy(), SNAPSHOT_FOR_DDL, tenant_id_, schema_version_, snapshot_scn, nullptr, tablet_ids))) {
    LOG_WARN("acquire snapshot failed", K(ret), K(tablet_ids));
  } else {
    snapshot_version_ = snapshot_version;
  }
  return ret;
}

int ObConstraintTask::release_snapshot(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObDDLService &ddl_service = root_service_->get_ddl_service();
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_RELEASE_SNAPSHOT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(object_id_));
  } else if (OB_ISNULL(table_schema)) {
    // ignore ret
    LOG_INFO("table not exist", K(ret), K(object_id_), K(target_object_id_));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, object_id_, tablet_ids))) {
    if (OB_TABLE_NOT_EXIST == ret || OB_TENANT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet snapshots", K(ret));
    }
  } else if (table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, table_schema->get_aux_lob_meta_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob meta table snapshot", K(ret));
  } else if (table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID &&
             OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, table_schema->get_aux_lob_piece_tid(), tablet_ids))) {
    LOG_WARN("failed to get data lob piece table snapshot", K(ret));
  } else if (OB_FAIL(batch_release_snapshot(snapshot_version, tablet_ids))) {
    LOG_WARN("failed to release snapshots", K(ret));
  }
  return ret;
}

int ObConstraintTask::wait_trans_end()
{
  int ret = OB_SUCCESS;
  ObDDLTaskStatus new_status = WAIT_TRANS_END;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (snapshot_version_ > 0 && snapshot_held_) {
    // already acquire snapshot, do nothing
    new_status = CHECK_CONSTRAINT_VALID;
  }

  if (OB_SUCC(ret) && new_status != CHECK_CONSTRAINT_VALID && !wait_trans_ctx_.is_inited()) {
    if (OB_FAIL(wait_trans_ctx_.init(tenant_id_, task_id_, object_id_, ObDDLWaitTransEndCtx::WaitTransType::WAIT_SCHEMA_TRANS, schema_version_))) {
      LOG_WARN("init wait trans ctx failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && new_status != CHECK_CONSTRAINT_VALID && snapshot_version_ <= 0) {
    bool is_trans_end = false;
    if (OB_FAIL(wait_trans_ctx_.try_wait(is_trans_end, snapshot_version_))) {
      LOG_WARN("try wait transaction failed", K(ret));
    }
  }

  DEBUG_SYNC(CONSTRAINT_WAIT_TRANS_END);

  if (OB_SUCC(ret) && new_status != CHECK_CONSTRAINT_VALID && snapshot_version_ > 0 && !snapshot_held_) {
    if (OB_FAIL(ObDDLTaskRecordOperator::update_snapshot_version(root_service_->get_sql_proxy(),
                                                                 tenant_id_,
                                                                 task_id_,
                                                                 snapshot_version_))) {
      LOG_WARN("update snapshot version failed", K(ret), K(task_id_));
    } else if (OB_FAIL(hold_snapshot(snapshot_version_))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        ret = OB_SUCCESS;
        snapshot_held_ = false;
        snapshot_version_ = 0;
        wait_trans_ctx_.reset();
      } else {
        LOG_WARN("hold snapshot version failed", K(ret));
      }
    } else {
      new_status = CHECK_CONSTRAINT_VALID;
      snapshot_held_ = true;
    }
  }

  if (OB_FAIL(switch_status(new_status, true, ret))) {
    LOG_WARN("switch status failed", K(ret));
  }
  return ret;
}

int ObConstraintTask::validate_constraint_valid()
{
  int ret = OB_SUCCESS;
  ObDDLTaskStatus state = CHECK_CONSTRAINT_VALID;
  bool is_check_replica_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected snapshot", K(ret), KPC(this));
  } else if (OB_FAIL(check_replica_end(is_check_replica_end))) {
    LOG_WARN("check build replica end", K(ret));
  } else {
    if (!is_check_replica_end) {
      if (check_replica_request_time_ == 0) {
        if (ObDDLType::DDL_CHECK_CONSTRAINT == task_type_
            || ObDDLType::DDL_ADD_NOT_NULL_COLUMN == task_type_) {
          if (OB_FAIL(send_check_constraint_request())) {
            LOG_WARN("validate check constraint failed", K(ret));
          }
        } else if (ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT == task_type_) {
          if (OB_FAIL(send_fk_constraint_request())) {
            LOG_WARN("validate fk constraint failed", K(ret));
          }
        }
        DEBUG_SYNC(CONSTRAINT_VALIDATE);
        ret_code_ = ret;
      }
    } else {
      state = SET_CONSTRAINT_VALIDATE;
    }
  }
  if (OB_FAIL(ret) || CHECK_CONSTRAINT_VALID != state) {
    if (OB_FAIL(switch_status(state, true, ret))) {
      LOG_WARN("switch status failed", K(ret));
    }
  }
  return ret;
}

int ObConstraintTask::send_check_constraint_request()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else {
    ObCheckConstraintValidationTask task(tenant_id_, object_id_, target_object_id_, target_object_id_, schema_version_,
                                        trace_id_, task_id_,
                                        ObDDLType::DDL_ADD_NOT_NULL_COLUMN == task_type_,
                                        alter_table_arg_.alter_constraint_type_);
    if (OB_FAIL(root_service_->submit_ddl_single_replica_build_task(task))) {
      LOG_WARN("submit ddl single replica build task failed", K(ret));
    } else {
      check_replica_request_time_ = ObTimeUtility::current_time();
      LOG_INFO("send check constraint request", K(object_id_), K(target_object_id_), K(schema_version_));
    }
  }
  return ret;
}

int ObConstraintTask::send_fk_constraint_request()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else {
    ObForeignKeyConstraintValidationTask task(tenant_id_, object_id_, target_object_id_, schema_version_, trace_id_, task_id_);
    if (OB_FAIL(root_service_->submit_ddl_single_replica_build_task(task))) {
      LOG_WARN("submit ddl single replica build task", K(ret));
    } else {
      check_replica_request_time_ = ObTimeUtility::current_time();
      LOG_INFO("send foreign key request", K(object_id_), K(target_object_id_), K(schema_version_));
    }
  }
  return ret;
}

int ObConstraintTask::check_replica_end(bool &is_end)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == check_job_ret_code_) {
    // not finish
  } else if (OB_SUCCESS != check_job_ret_code_) {
    ret_code_ = check_job_ret_code_;
    is_end = true;
    LOG_WARN("complete sstable job failed", K(ret_code_), K(object_id_), K(target_object_id_));
    if (is_replica_build_need_retry(ret_code_)) {
      check_replica_request_time_ = 0;
      check_job_ret_code_ = INT64_MAX;
      ret_code_ = OB_SUCCESS;
      is_end = false;
      LOG_INFO("ddl need retry", K(*this));
    }
    ret = ret_code_;
  } else {
    is_end = true;
    ret_code_ = check_job_ret_code_;
    ret = ret_code_;
  }

  if (OB_SUCC(ret) && !is_end) {
    if (check_replica_request_time_ + ObDDLUtil::calc_inner_sql_execute_timeout() < ObTimeUtility::current_time()) {
      check_replica_request_time_ = 0;
    }
  }
  return ret;
}

int ObConstraintTask::release_ddl_locks()
{
  int ret = OB_SUCCESS;
  int64_t total_tablet_cnt = 0;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const ObTableSchema *another_table_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table dropped", K(ret), K(object_id_));
  } else if (OB_FALSE_IT(total_tablet_cnt += table_schema->get_all_part_num())) {
  } else if (ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT == task_type_) {
    const ObIArray<ObBasedSchemaObjectInfo> &obj_infos = alter_table_arg_.based_schema_object_infos_;
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_infos.count(); i++) {
      const ObBasedSchemaObjectInfo &obj_info = obj_infos.at(i);
      if (TABLE_SCHEMA == obj_info.schema_type_ && object_id_ != obj_info.schema_id_) {
        const uint64_t another_table_id = obj_info.schema_id_;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, another_table_id, another_table_schema))) {
          LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(another_table_id));
        } else if (OB_ISNULL(another_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table dropped", K(ret), K(another_table_id));
        } else {
          total_tablet_cnt += another_table_schema->get_all_part_num();
          break;
        }
      }
    }
  }

  ObTimeoutCtx timeout_ctx;
  int64_t ddl_tx_timeout = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLUtil::get_ddl_tx_timeout(total_tablet_cnt, ddl_tx_timeout))) {
    LOG_WARN("get ddl tx timeout failed", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(ddl_tx_timeout))) {
    LOG_WARN("set timeout ctx failed", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_timeout(ddl_tx_timeout))) {
    LOG_WARN("set timeout failed", K(ret));
  }

  ObMySQLTransaction trans;
  transaction::tablelock::ObTableLockOwnerID owner_id;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), tenant_id_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                 task_id_))) {
    LOG_WARN("get owner id failed", K(ret), K(task_id_));
  } else if (OB_FAIL(ObDDLLock::unlock_for_common_ddl(*table_schema,
                                                      owner_id,
                                                      trans))) {
    LOG_WARN("failed to unlock ddl", K(ret));
  } else if (nullptr != another_table_schema) {
    if (OB_FAIL(ObDDLLock::unlock_for_common_ddl(*another_table_schema,
                                                 owner_id,
                                                 trans))) {
      LOG_WARN("failed to unlock ddl", K(ret));
    }
  }

  bool commit = (OB_SUCCESS == ret);
  int tmp_ret = trans.end(commit);
  if (OB_SUCCESS != tmp_ret) {
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  if (OB_TABLE_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObConstraintTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (snapshot_version_ > 0 && OB_FAIL(release_snapshot(snapshot_version_))) {
    LOG_WARN("release snapshot failed", K(ret));
  } else if (OB_FAIL(release_ddl_locks())) {
    LOG_WARN("failed to release ddl locks", K(ret));
  } else if (OB_FAIL(report_error_code())) {
    LOG_WARN("report error code failed", K(ret));
  }

  DEBUG_SYNC(CONSTRAINT_SUCCESS);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(remove_task_record())) {
    LOG_WARN("remove task record failed", K(ret));
  } else {
    need_retry_ = false;
  }

  if (OB_SUCC(ret) && parent_task_id_ > 0) {
    const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
    root_service_->get_ddl_task_scheduler().on_ddl_task_finish(parent_task_id, get_task_key(), ret_code_, trace_id_);
  }
  return ret;
}

int ObConstraintTask::remove_task_record()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(root_service_->get_sql_proxy(),
                                                            tenant_id_,
                                                            task_id_))) {
    LOG_WARN("delete record failed", K(ret), K(task_id_));
  }
  return ret;
}

int ObConstraintTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_FAIL(rollback_failed_schema())) {
    LOG_WARN("drop failed schema failed", K(ret));
  }
  DEBUG_SYNC(CONSTRAINT_FAIL);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("clean up failed", K(ret));
  }
  return ret;
}

int ObConstraintTask::succ()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_FAIL(cleanup())) {
    LOG_WARN("clean up failed", K(ret));
  }
  return ret;
}

int ObConstraintTask::report_error_code()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (ObDDLType::DDL_CHECK_CONSTRAINT == task_type_
            || ObDDLType::DDL_ADD_NOT_NULL_COLUMN == task_type_) {
    if (OB_FAIL(report_check_constraint_error_code())) {
      LOG_WARN("report check constraint error code failed", K(ret));
    }
  } else if (ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT == task_type_) {
    if (OB_FAIL(report_foreign_key_constraint_error_code())) {
      LOG_WARN("report foregin key constraint error code failed", K(ret));
    }
  }
  return ret;
}

int ObConstraintTask::report_check_constraint_error_code()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(alter_table_arg_.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check tenant is oracle mode failed", K(ret));
  } else {
    ObTableSchema::const_constraint_iterator iter = alter_table_arg_.alter_table_schema_.constraint_begin();
    const ObString &database_name = alter_table_arg_.alter_table_schema_.get_origin_database_name();
    ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
    const char *ddl_type_str = nullptr;
    error_message.ret_code_ = ret_code_;
    error_message.ddl_type_ = task_type_;
    LOG_INFO("report error code", K(ret_code_));
    if (OB_ISNULL(error_message.user_message_ = static_cast<char *>(error_message.allocator_.alloc(OB_MAX_ERROR_MSG_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FALSE_IT(memset(error_message.user_message_, 0, OB_MAX_ERROR_MSG_LEN))) {
    } else if (OB_SUCCESS == ret_code_) {
      if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "%s", "Successful ddl"))) {
        LOG_WARN("print ddl dba message failed", K(ret));
      } else if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, "%s", "Successful ddl"))) {
        LOG_WARN("print ddl user message failed", K(ret));
      }
    } else {
      const char *str_user_error = ob_errpkt_str_user_error(ret_code_, is_oracle_mode);
      const char *str_error = ob_errpkt_strerror(ret_code_, is_oracle_mode);
      if (OB_FAIL(get_ddl_type_str(task_type_, ddl_type_str))) {
        LOG_WARN("ddl type to string failed", K(ret));
      } else if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "ddl_type:%s", ddl_type_str))) {
        LOG_WARN("print ddl dba message failed", K(ret));
      } else if (OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED == ret_code_
                || OB_ERR_NOT_NULL_CONSTRAINT_VIOLATED == ret_code_) {
        if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, str_user_error, database_name.length(), database_name.ptr(),
            (*iter)->get_constraint_name_str().length(), (*iter)->get_constraint_name_str().ptr()))) {
          LOG_WARN("print ddl user message failed", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, "%s", str_error))) {
          LOG_WARN("print ddl user message failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLErrorMessageTableOperator::report_ddl_error_message(error_message, tenant_id_, trace_id_, task_id_, parent_task_id_, object_id_, schema_version_, -1/*object id*/, GCTX.self_addr(), root_service->get_sql_proxy()))) {
        LOG_WARN("report constraint ddl error message failed", K(ret));
      }
    }
  }
  return ret;
}

int ObConstraintTask::report_foreign_key_constraint_error_code()
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObRootService *root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_FAIL(alter_table_arg_.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check tenant is oracle mode failed", K(ret));
  } else {
    ObCreateForeignKeyArg &fk_arg = alter_table_arg_.foreign_key_arg_list_.at(alter_table_arg_.foreign_key_arg_list_.count() - 1);
    const ObString &database_name = alter_table_arg_.alter_table_schema_.get_origin_database_name();
    ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
    const char *ddl_type_str = nullptr;
    error_message.ret_code_ = ret_code_;
    error_message.ddl_type_ = task_type_;
    if (OB_ISNULL(error_message.user_message_ = static_cast<char *>(error_message.allocator_.alloc(OB_MAX_ERROR_MSG_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FALSE_IT(memset(error_message.user_message_, 0, OB_MAX_ERROR_MSG_LEN))) {
    } else if (OB_SUCCESS == ret_code_) {
      if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "%s", "Successful ddl"))) {
        LOG_WARN("print ddl dba message failed", K(ret));
      } else if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, "%s", "Successful ddl"))) {
        LOG_WARN("print ddl user message failed", K(ret));
      }
    } else {
      const char *str_user_error = ob_errpkt_str_user_error(ret_code_, is_oracle_mode);
      const char *str_error = ob_errpkt_strerror(ret_code_, is_oracle_mode);
      if (OB_FAIL(get_ddl_type_str(task_type_, ddl_type_str))) {
        LOG_WARN("ddl type to string failed", K(ret));
      } else if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "ddl_type:%s", ddl_type_str))) {
        LOG_WARN("print ddl dba message failed", K(ret));
      } else if (OB_ERR_ORPHANED_CHILD_RECORD_EXISTS == ret_code_) {
        if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, str_user_error, database_name.length(), database_name.ptr(),
            fk_arg.foreign_key_name_.length(), fk_arg.foreign_key_name_.ptr()))) {
          LOG_WARN("print ddl user message failed", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, "%s", str_error))) {
          LOG_WARN("print ddl user message failed", K(ret));
        }
      }
  }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLErrorMessageTableOperator::report_ddl_error_message(error_message, tenant_id_, trace_id_, task_id_, parent_task_id_, object_id_, schema_version_, -1/*object id*/, GCTX.self_addr(), root_service->get_sql_proxy()))) {
        LOG_WARN("report constraint ddl error message failed", K(ret));
      }
    }
  }
  return ret;
}

int ObConstraintTask::set_foreign_key_constraint_validated()
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = 0;
  int64_t tablet_count = 0;
  ObRootService *root_service = GCTX.root_service_;
  obrpc::ObAlterTableRes res;
  ObArenaAllocator allocator(lib::ObLabel("ConstraiTask"));
  SMART_VAR(ObAlterTableArg, alter_table_arg) {
    if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CONSTRAINT_TASK_SET_VALIDATED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(deep_copy_table_arg(allocator, alter_table_arg_, alter_table_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    } else if (alter_table_arg.foreign_key_arg_list_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, foreign key arg list must not be single", K(ret), K(alter_table_arg.foreign_key_arg_list_));
    } else if (OB_ISNULL(root_service)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, root serivce must not be nullptr", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
      LOG_WARN("get ddl rpc_timeout failed", K(ret));
    } else {
      DEBUG_SYNC(CONSTRAINT_BEFORE_SET_FK_VALIDATED_BEFORE_ALTER_TABLE);
      ObCreateForeignKeyArg &fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
      fk_arg.is_modify_fk_state_ = true;
      fk_arg.is_modify_validate_flag_ = true;
      fk_arg.validate_flag_ = CST_FK_VALIDATED;
      fk_arg.need_validate_data_ = false;
      alter_table_arg.exec_tenant_id_ = tenant_id_;
      alter_table_arg.based_schema_object_infos_.reset();
      alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id_);
      if (is_table_hidden_) {
        ObSArray<uint64_t> unused_ids;
        alter_table_arg.ddl_task_type_ = share::MODIFY_FOREIGN_KEY_STATE_TASK;
        alter_table_arg.hidden_table_id_ = object_id_;
        if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
              execute_ddl_task(alter_table_arg, unused_ids))) {
          LOG_WARN("fail to alter table", K(ret), K(alter_table_arg), K(fk_arg));
        }
      } else {
        if (OB_FAIL(ObDDLUtil::refresh_alter_table_arg(tenant_id_, object_id_, target_object_id_, alter_table_arg))) {
          LOG_WARN("failed to refresh name for alter table schema", K(ret));
        } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
            alter_table(alter_table_arg, res))) {
          LOG_WARN("alter table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObConstraintTask::check_column_is_nullable(const uint64_t column_id, bool &is_nullable) const
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const share::schema::ObColumnSchemaV2 *column_schema = nullptr;
  if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table dropped", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id))) {
    ret = OB_ERR_COLUMN_NOT_FOUND;
    LOG_WARN("column not found", K(ret), K(column_id));
  } else {
    is_nullable = column_schema->is_nullable();
  }
  return ret;
}

int ObConstraintTask::set_check_constraint_validated()
{
  int ret = OB_SUCCESS;
  ObRootService *root_service = GCTX.root_service_;
  obrpc::ObAlterTableRes res;
  bool is_oracle_mode = false;
  int64_t rpc_timeout = 0;
  int64_t tablet_count = 0;
  ObArenaAllocator allocator(lib::ObLabel("ConstraiTask"));
  SMART_VAR(ObAlterTableArg, alter_table_arg) {
    if (OB_ISNULL(root_service)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, root serivce must not be nullptr", K(ret));
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CONSTRAINT_TASK_SET_VALIDATED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(deep_copy_table_arg(allocator, alter_table_arg_, alter_table_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    } else {
      ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
      if (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_) {
        (*iter)->set_constraint_id(target_object_id_);
      }
      alter_table_arg.based_schema_object_infos_.reset();
    }
    if (OB_FAIL(ret)) {
    } else {
      alter_table_arg.index_arg_list_.reset();
      alter_table_arg.foreign_key_arg_list_.reset();
      ObTableSchema::constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin_for_non_const_iter();
      // need to set constraint validate when execute:
      // 1. alter table modify c1 not null;
      // 2. alter table modify constraint cst_not_null valdiate.
      if (alter_table_arg.alter_table_schema_.get_constraint_count() > 1) {
        for (; iter != alter_table_arg.alter_table_schema_.constraint_end_for_non_const_iter()
            && OB_SUCC(ret); iter++) {
          if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("constraint is null", K(ret));
          } else if (target_object_id_ == (*iter)->get_constraint_id()) {
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (alter_table_arg.alter_table_schema_.constraint_end_for_non_const_iter() == iter) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constraint not found", K(ret), K(target_object_id_), K(alter_table_arg));
      } else if (OB_FAIL(alter_table_arg.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("check if oracle compat mode failed", K(ret));
      } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
        LOG_WARN("get ddl rpc timeout failed", K(ret));
      } else if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
        alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id_);
        if (is_table_hidden_) {
          if (!is_oracle_mode) {
            // no need to refresh_alter_table_arg because MODIFY_NOT_NULL_COLUMN_STATE_TASK use constraint id instead of name
            // only mysql mode support modify not null column during offline ddl, support oracle later.
            ObSArray<uint64_t> unused_ids;
            alter_table_arg.ddl_task_type_ = share::MODIFY_NOT_NULL_COLUMN_STATE_TASK;
            alter_table_arg.hidden_table_id_ = object_id_;
            if (OB_FAIL(root_service_->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
                execute_ddl_task(alter_table_arg, unused_ids))) {
              LOG_WARN("alter table failed", K(ret));
              if (OB_TABLE_NOT_EXIST == ret) {
                ret = OB_NO_NEED_UPDATE;
              }
            }
          }
        } else {
          if ((obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_
                && (*iter)->is_validated())
              || (obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg.alter_constraint_type_
                && (*iter)->get_is_modify_validate_flag() && (*iter)->is_validated())) {
            alter_table_arg.exec_tenant_id_ = tenant_id_;
            uint64_t column_id = OB_INVALID_ID;
            if (is_oracle_mode) {
              if (OB_FAIL(ObDDLUtil::refresh_alter_table_arg(tenant_id_, object_id_, OB_INVALID_ID/*foreign_key_id*/, alter_table_arg))) {
                LOG_WARN("failed to refresh name for alter table schema", K(ret));
              } else {
                alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE;
                (*iter)->set_is_modify_validate_flag(true);
                (*iter)->set_validate_flag(CST_FK_VALIDATED);
                (*iter)->set_need_validate_data(false);
              }
            } else {
              alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::DROP_CONSTRAINT;
              if (OB_ISNULL((*iter)->cst_col_begin())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column of not null constraint is null", K(ret), KPC(*iter));
              } else if (OB_INVALID_ID != (column_id = *((*iter)->cst_col_begin()))) {
                ObColumnSchemaV2 *column = NULL;
                for (int64_t i = 0; i < alter_table_arg.alter_table_schema_.get_column_count(); i++) {
                  if (alter_table_arg.alter_table_schema_.get_column_schema_by_idx(i)->get_column_id() == column_id) {
                    column = alter_table_arg.alter_table_schema_.get_column_schema_by_idx(i);
                  }
                }
                if (OB_ISNULL(column)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("get column schema failed", K(ret), K(alter_table_arg), K(column_id));
                } else {
                  column->set_nullable(false);
                  column->drop_not_null_cst();
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid column id", K(ret), K(alter_table_arg), K(column_id));
              } else if (OB_FAIL(ObDDLUtil::refresh_alter_table_arg(tenant_id_, object_id_, OB_INVALID_ID/*foreign_key_id*/, alter_table_arg))) {
                if (OB_ERR_CONTRAINT_NOT_FOUND == ret) {
                  bool is_nullable = false;
                  if (OB_FAIL(check_column_is_nullable(column_id, is_nullable))) { // overwrite ret
                    LOG_WARN("failed to check column is nullable", K(ret));
                  } else if (is_nullable) {
                    ret = OB_ERR_CONTRAINT_NOT_FOUND;
                    LOG_WARN("column is nullable without constraint, maybe constraint dropped by others", K(ret));
                  } else {
                    ret = OB_NO_NEED_UPDATE;
                    LOG_INFO("already not null, maybe on retry", K(target_object_id_), K(column_id));
                  }
                } else {
                  LOG_WARN("failed to refresh name for alter table schema", K(ret));
                }
              }
            }
            DEBUG_SYNC(CONSTRAINT_BEFORE_SET_CHECK_CONSTRAINT_VALIDATED_BEFORE_ALTER_TABLE);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
                alter_table(alter_table_arg, res))) {
              LOG_WARN("alter table failed", K(ret));
            }
            if (OB_NO_NEED_UPDATE == ret) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObConstraintTask::set_new_not_null_column_validate()
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = 0;
  int64_t tablet_count = 0;
  ObRootService *root_service = GCTX.root_service_;
  obrpc::ObAlterTableRes res;
  ObArenaAllocator allocator(lib::ObLabel("ConstraiTask"));
  SMART_VAR(ObAlterTableArg, alter_table_arg) {
    if (OB_ISNULL(root_service)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, root serivce must not be nullptr", K(ret));
    } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CONSTRAINT_TASK_SET_VALIDATED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(deep_copy_table_arg(allocator, alter_table_arg_, alter_table_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    } else {
      ObSEArray<AlterColumnSchema *, 16> new_columns;
      alter_table_arg.based_schema_object_infos_.reset();
      alter_table_arg.exec_tenant_id_ = tenant_id_;
      alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::CONSTRAINT_NO_OPERATION;
      alter_table_arg.alter_table_schema_.clear_constraint();
      alter_table_arg.index_arg_list_.reset();
      alter_table_arg.foreign_key_arg_list_.reset();
      alter_table_arg.sequence_ddl_arg_.set_stmt_type(OB_INVALID_ID);
      for (int64_t i = 0; i < alter_table_arg.alter_table_schema_.get_column_count() && OB_SUCC(ret);
          i++) {
        AlterColumnSchema *col_schema = NULL;
        if (OB_ISNULL(col_schema =static_cast<AlterColumnSchema *>
              (alter_table_arg.alter_table_schema_.get_column_schema_by_idx(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret));
        } else if (OB_DDL_ADD_COLUMN == col_schema->alter_type_) {
          col_schema->alter_type_ = OB_DDL_MODIFY_COLUMN;
          col_schema->set_origin_column_name(col_schema->get_column_name_str());
          col_schema->set_is_hidden(false);
          if (OB_FAIL(new_columns.push_back(col_schema))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        alter_table_arg.alter_table_schema_.reset_column_count();
        for (int64_t i = 0; i < new_columns.count() && OB_SUCC(ret); i++) {
          if (OB_FAIL(alter_table_arg.alter_table_schema_.add_alter_column(
                  *new_columns.at(i), false))) {
            LOG_WARN("add columns failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
        LOG_WARN("get ddl rpc timeout failed", K(ret));
      } else if (OB_FAIL(root_service->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
          alter_table(alter_table_arg, res))) {
        LOG_WARN("alter table failed", K(ret));
      } else {
        LOG_TRACE("set new not null column validate", K(alter_table_arg));
      }
    }
  }
  return ret;
}

int ObConstraintTask::rollback_failed_schema()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else { 
    if (ObDDLType::DDL_CHECK_CONSTRAINT == task_type_) {
      if (OB_FAIL(rollback_failed_check_constraint())) {
        LOG_WARN("drop failed check constraint failed", K(ret));
      }
    } else if (ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT == task_type_) {
      if (OB_FAIL(rollback_failed_foregin_key())) {
        LOG_WARN("drop failed foregin key failed", K(ret));
      }
    } else if (ObDDLType::DDL_ADD_NOT_NULL_COLUMN == task_type_) {
      if (OB_FAIL(rollback_failed_add_not_null_columns())) {
        LOG_WARN("drop failed not null columns", K(ret));
      }
    }
  }
  return ret;
}

// rollback constraint ddl
// if ddl is alter table add consraint, then executes alter table tbl_name drop constraint cst_name
// else if alter table modify constraint state, then executes alter table modify constraint state back
int ObConstraintTask::rollback_failed_check_constraint()
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = 0;
  int64_t tablet_count = 0;
  obrpc::ObAlterTableRes tmp_res;
  ObArenaAllocator allocator(lib::ObLabel("ConstraiTask"));
  SMART_VAR(ObAlterTableArg, alter_table_arg) {
    if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CONSTRAINT_TASK_ROLL_BACK_SCHEMA))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(deep_copy_table_arg(allocator, alter_table_arg_, alter_table_arg))) {
      LOG_WARN("fail to deep copy table arg", K(ret));
    } else {
      alter_table_arg.based_schema_object_infos_.reset();
      ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
      if (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_) {
        (*iter)->set_constraint_id(target_object_id_);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::refresh_alter_table_arg(tenant_id_, object_id_, OB_INVALID_ID/*foreign_key_id*/, alter_table_arg))) {
      if (OB_TABLE_NOT_EXIST == ret || OB_ERR_CONTRAINT_NOT_FOUND == ret) {
        ret = OB_NO_NEED_UPDATE;
      } else {
        LOG_WARN("failed to refresh name for alter table schema", K(ret));
      }
    } else {
      ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
      if (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_) {
        if (OB_FAIL(set_drop_constraint_ddl_stmt_str(alter_table_arg, allocator))) {
          LOG_WARN("fail to set drop constraint ddl_stmt_str", K(ret));
        } else {
          alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::DROP_CONSTRAINT;
        }
      } else {
        if ((*iter)->get_is_modify_enable_flag()) {
          (*iter)->set_enable_flag(!(*iter)->get_enable_flag());
        }
        if ((*iter)->get_is_modify_rely_flag()) {
          (*iter)->set_rely_flag(!(*iter)->get_rely_flag());
        }
        if ((*iter)->get_is_modify_validate_flag()) {
          const ObCstFkValidateFlag validate_flag = (*iter)->is_validated()
            ? CST_FK_NO_VALIDATE : CST_FK_VALIDATED;
          (*iter)->set_validate_flag(validate_flag);
        }
        (*iter)->set_need_validate_data(false);
        alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE;
        if (OB_FAIL(set_alter_constraint_ddl_stmt_str_for_check(alter_table_arg, allocator))) {
          LOG_WARN("fail to set alter constraint ddl_stmt_str", K(ret));
        }
      }
    }
    DEBUG_SYNC(CONSTRAINT_ROLLBACK_FAILED_CHECK_CONSTRAINT_BEFORE_ALTER_TABLE);
    if (OB_SUCC(ret) && !is_table_hidden_) {
      alter_table_arg.is_inner_ = true;
      alter_table_arg.is_alter_columns_ = false;
      alter_table_arg.index_arg_list_.reset();
      alter_table_arg.foreign_key_arg_list_.reset();
      if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
        LOG_WARN("get ddl rpc timeout failed", K(ret));
      } else if (OB_FAIL(root_service_->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
          alter_table(alter_table_arg, tmp_res))) {
        LOG_WARN("alter table failed", K(ret));
        if (OB_TABLE_NOT_EXIST == ret || OB_ERR_CANT_DROP_FIELD_OR_KEY == ret || OB_ERR_CONTRAINT_NOT_FOUND == ret) {
          ret = OB_NO_NEED_UPDATE;
        }
      }
    }
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("rollback failed check constraint succussfully", K(alter_table_arg));
    }
  }
  return ret;
}

int ObConstraintTask::rollback_failed_foregin_key()
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = 0;
  int64_t tablet_count = 0;
  obrpc::ObAlterTableRes tmp_res;
  obrpc::ObDropForeignKeyArg drop_foreign_key_arg;
  ObCreateForeignKeyArg &fk_arg = alter_table_arg_.foreign_key_arg_list_.at(0);
  SMART_VAR(ObAlterTableArg, alter_table_arg) {
    ObArenaAllocator allocator(lib::ObLabel("ConstraiTask"));
    if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CONSTRAINT_TASK_ROLL_BACK_SCHEMA))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(deep_copy_table_arg(allocator, alter_table_arg_, alter_table_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    } else if (FALSE_IT(alter_table_arg.based_schema_object_infos_.reset())) {
    } else if (!is_table_hidden_ && OB_FAIL(ObDDLUtil::refresh_alter_table_arg(tenant_id_, object_id_, target_object_id_, alter_table_arg))) {
      if (OB_TABLE_NOT_EXIST == ret || OB_ERR_CONTRAINT_NOT_FOUND == ret) {
        ret = OB_NO_NEED_UPDATE;
      } else {
        LOG_WARN("failed to refresh name for alter table schema", K(ret));
      }
    } else if (!fk_arg.is_modify_fk_state_) {
      // alter table tbl_name drop constraint fk_cst_name without ddl_stmt_str
      drop_foreign_key_arg.index_action_type_ = obrpc::ObIndexArg::DROP_FOREIGN_KEY;
      drop_foreign_key_arg.foreign_key_name_ = alter_table_arg.foreign_key_arg_list_.at(0).foreign_key_name_;
      alter_table_arg.index_arg_list_.reset();
      if (OB_FAIL(alter_table_arg.index_arg_list_.push_back(&drop_foreign_key_arg))) {
        LOG_WARN("fail to push back arg to index_arg_list", K(ret));
      } else {
        alter_table_arg.ddl_stmt_str_.reset();
        alter_table_arg.foreign_key_arg_list_.reset();
      }
    } else {
      // alter table tbl_name modify constraint back to before
      ObCreateForeignKeyArg &modify_fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
      if (modify_fk_arg.is_modify_rely_flag_) {
        modify_fk_arg.rely_flag_ = !fk_arg.rely_flag_;
      }
      if (modify_fk_arg.is_modify_enable_flag_) {
        modify_fk_arg.enable_flag_ = !fk_arg.enable_flag_;
      }
      if (modify_fk_arg.is_modify_validate_flag_) {
        modify_fk_arg.validate_flag_ = fk_arg.validate_flag_ == CST_FK_VALIDATED ? CST_FK_NO_VALIDATE : CST_FK_VALIDATED;
      }
      alter_table_arg.index_arg_list_.reset();
      if (OB_FAIL(set_alter_constraint_ddl_stmt_str_for_fk(alter_table_arg, allocator))) {
        LOG_WARN("fail to set alter constraint ddl_stmt_str", K(ret));
      }
    }
    DEBUG_SYNC(CONSTRAINT_ROLLBACK_FAILED_FK_BEFORE_ALTER_TABLE);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      alter_table_arg.is_inner_ = true;
      if (is_table_hidden_) {
        ObSArray<uint64_t> unused_ids;
        alter_table_arg.ddl_task_type_ = share::MODIFY_FOREIGN_KEY_STATE_TASK;
        alter_table_arg.hidden_table_id_ = object_id_;
        alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id_);
        if (OB_FAIL(root_service_->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
            execute_ddl_task(alter_table_arg, unused_ids))) {
          LOG_WARN("alter table failed", K(ret));
          if (OB_TABLE_NOT_EXIST == ret || OB_ERR_CANT_DROP_FIELD_OR_KEY == ret) {
            ret = OB_NO_NEED_UPDATE;
          }
        }
      } else {
        if (OB_FAIL(root_service_->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
            alter_table(alter_table_arg, tmp_res))) {
          LOG_WARN("alter table failed", K(ret));
          if (OB_TABLE_NOT_EXIST == ret || OB_ERR_CANT_DROP_FIELD_OR_KEY == ret) {
            ret = OB_NO_NEED_UPDATE;
          }
        }
      }
    }
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("rollback failed foreign key constraint succussfully", K(alter_table_arg));
    }
  }
  return ret;
}

int ObConstraintTask::rollback_failed_add_not_null_columns()
{
  int ret = OB_SUCCESS;
  ObString cst_name;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  bool first_alter_clause = true;
  ObArenaAllocator allocator(lib::ObLabel("ConstraiTask"));
  SMART_VAR(ObAlterTableArg, alter_table_arg) {
    if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CONSTRAINT_TASK_ROLL_BACK_SCHEMA))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(deep_copy_table_arg(allocator, alter_table_arg_, alter_table_arg))) {
      LOG_WARN("deep copy table arg failed", K(ret));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
            "ALTER TABLE \"%.*s\".\"%.*s\" ",
            alter_table_arg.alter_table_schema_.get_origin_database_name().length(),
            alter_table_arg.alter_table_schema_.get_origin_database_name().ptr(),
            alter_table_arg.alter_table_schema_.get_origin_table_name().length(),
            alter_table_arg.alter_table_schema_.get_origin_table_name().ptr()))) {
      LOG_WARN("print stmt failed", K(ret));
    }
    for (ObTableSchema::const_column_iterator col_iter = alter_table_arg.alter_table_schema_.column_begin();
        OB_SUCC(ret) && col_iter != alter_table_arg.alter_table_schema_.column_end(); col_iter++) {
      AlterColumnSchema *alter_col_schema = static_cast<AlterColumnSchema *>(*col_iter);
      if (OB_DDL_ADD_COLUMN == alter_col_schema->alter_type_) {
        const ObString col_name = alter_col_schema->get_column_name();
        if (OB_UNLIKELY(col_name.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is null", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                first_alter_clause ? "DROP COLUMN \"%.*s\"" : ", DROP COLUMN \"%.*s\"",
                col_name.length(), col_name.ptr()))) {
          LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
        } else {
          first_alter_clause = false;
        }
      } else {
        LOG_ERROR("unexpected alter column type", KPC(alter_col_schema));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t rpc_timeout = 0;
      int64_t tablet_count = 0;
      obrpc::ObAlterTableRes tmp_res;
      ObSArray<uint64_t> objs;
      alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
      alter_table_arg.alter_table_schema_.alter_type_ = OB_DDL_DROP_COLUMN;
      alter_table_arg.alter_constraint_type_ = obrpc::ObAlterTableArg::DROP_CONSTRAINT;
      alter_table_arg.ddl_task_type_ = share::DELETE_COLUMN_FROM_SCHEMA;
      alter_table_arg.index_arg_list_.reset();
      alter_table_arg.foreign_key_arg_list_.reset();
      alter_table_arg.based_schema_object_infos_.reset();
      alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id_);
      AlterColumnSchema *col_schema = NULL;
      for (int64_t i = 0; i < alter_table_arg.alter_table_schema_.get_column_count() && OB_SUCC(ret); i++) {
        if (OB_ISNULL(col_schema = static_cast<AlterColumnSchema *>(
                alter_table_arg.alter_table_schema_.get_column_schema_by_idx(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret));
        } else {
          col_schema->alter_type_ = OB_DDL_DROP_COLUMN;
          col_schema->origin_column_name_.assign_ptr(col_schema->get_column_name(),
              col_schema->get_column_name_str().length());
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id_, object_id_, rpc_timeout))) {
        LOG_WARN("get ddl rpc timeout failed", K(ret));
      }
      if (OB_SUCC(ret)
          && OB_FAIL(root_service_->get_ddl_service().get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).timeout(rpc_timeout).
            execute_ddl_task(alter_table_arg, objs))) {
        LOG_WARN("alter table failed", K(ret));
        if (OB_TABLE_NOT_EXIST == ret || OB_ERR_CANT_DROP_FIELD_OR_KEY == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObConstraintTask::set_drop_constraint_ddl_stmt_str(
    obrpc::ObAlterTableArg &alter_table_arg,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString cst_name;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  bool is_oracle_mode = false;
  bool is_check_constraint = false;

  if (obrpc::ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_) {
    ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
    cst_name = (*iter)->get_constraint_name_str();
    is_check_constraint = true;
  } else if (1 == alter_table_arg.foreign_key_arg_list_.count()) {
    obrpc::ObCreateForeignKeyArg fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
    cst_name = fk_arg.foreign_key_name_;
    is_check_constraint = false;
  }
  if (OB_FAIL(ret)) {
  } else if (cst_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, cst name must not be empty", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(alter_table_arg.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check if oracle mode failed", K(ret));
  } else {
    if (is_oracle_mode) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
              "ALTER TABLE \"%.*s\".\"%.*s\" DROP CONSTRAINT \"%.*s\"",
              alter_table_schema.get_origin_database_name().length(),
              alter_table_schema.get_origin_database_name().ptr(),
              alter_table_schema.get_origin_table_name().length(),
              alter_table_schema.get_origin_table_name().ptr(),
              cst_name.length(), cst_name.ptr()))) {
        LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
      }
    } else {
      if (is_check_constraint) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                "ALTER TABLE `%.*s`.`%.*s` DROP CHECK (`%.*s`)",
                alter_table_schema.get_origin_database_name().length(),
                alter_table_schema.get_origin_database_name().ptr(),
                alter_table_schema.get_origin_table_name().length(),
                alter_table_schema.get_origin_table_name().ptr(),
                cst_name.length(), cst_name.ptr()))) {
          LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                "ALTER TABLE `%.*s`.`%.*s` DROP FOREIGN KEY `%.*s`",
                alter_table_schema.get_origin_database_name().length(),
                alter_table_schema.get_origin_database_name().ptr(),
                alter_table_schema.get_origin_table_name().length(),
                alter_table_schema.get_origin_table_name().ptr(),
                cst_name.length(), cst_name.ptr()))) {
          LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }

  return ret;
}

int ObConstraintTask::set_alter_constraint_ddl_stmt_str_for_check(
    obrpc::ObAlterTableArg &alter_table_arg,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString cst_name;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
  cst_name = (*iter)->get_constraint_name_str();
  if (OB_FAIL(ret)) {
  } else if (cst_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cst_name is empty", K(ret), K(cst_name));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                     "ALTER TABLE \"%.*s\".\"%.*s\" MODIFY CONSTRAINT %.*s ",
                     alter_table_schema.get_origin_database_name().length(),
                     alter_table_schema.get_origin_database_name().ptr(),
                     alter_table_schema.get_origin_table_name().length(),
                     alter_table_schema.get_origin_table_name().ptr(),
                     cst_name.length(), cst_name.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else if ((*iter)->get_is_modify_rely_flag()
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                (*iter)->get_rely_flag() ? "RELY ":"NORELY "))) {
    LOG_WARN("fail to print rely flag for rollback", K(ret));
  } else if ((*iter)->get_is_modify_enable_flag()
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                (*iter)->get_enable_flag() ? "ENABLE ":"DISABLE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else if ((*iter)->get_is_modify_validate_flag()
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                (*iter)->get_validate_flag() ? "VALIDATE ":"NOVALIDATE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObConstraintTask::set_alter_constraint_ddl_stmt_str_for_fk(
    obrpc::ObAlterTableArg &alter_table_arg,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  ObString cst_name;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  obrpc::ObCreateForeignKeyArg fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
  cst_name = fk_arg.foreign_key_name_;
  if (OB_FAIL(ret)) {
  } else if (cst_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cst_name is empty", K(ret), K(cst_name));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                     "ALTER TABLE \"%.*s\".\"%.*s\" MODIFY CONSTRAINT %.*s ",
                     alter_table_schema.get_origin_database_name().length(),
                     alter_table_schema.get_origin_database_name().ptr(),
                     alter_table_schema.get_origin_table_name().length(),
                     alter_table_schema.get_origin_table_name().ptr(),
                     cst_name.length(), cst_name.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else if (fk_arg.is_modify_rely_flag_
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                 fk_arg.rely_flag_ ? "RELY ":"NORELY "))) {
    LOG_WARN("fail to print rely flag for rollback", K(ret));
  } else if (fk_arg.is_modify_enable_flag_
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                 fk_arg.enable_flag_ ? "ENABLE ":"DISABLE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else if (fk_arg.is_modify_validate_flag_
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                 CST_FK_VALIDATED == fk_arg.validate_flag_ ? "VALIDATE ":"NOVALIDATE "))) {
    LOG_WARN("fail to print enable flag for rollback", K(ret));
  } else {
    alter_table_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

int ObConstraintTask::set_constraint_validated()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, CONSTRAINT_TASK_SET_VALIDATED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (task_type_ == ObDDLType::DDL_CHECK_CONSTRAINT) {
    if (OB_FAIL(set_check_constraint_validated())) {
      LOG_WARN("set check constraint validated failed", K(ret));
    }
  } else if (task_type_ == ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT) {
    if (OB_FAIL(set_foreign_key_constraint_validated())) {
      LOG_WARN("set foreign key constraint validated failed", K(ret));
    }
  } else if (ObDDLType::DDL_ADD_NOT_NULL_COLUMN == task_type_) {
    if (OB_FAIL(set_new_not_null_column_validate())) {
      LOG_WARN("set new not null column validate failed", K(ret));
    }
  }
  DEBUG_SYNC(CONSTRAINT_SET_VALID);
  if (OB_FAIL(switch_status(ObDDLTaskStatus::SUCCESS, true, ret))) {
    // overwrite ret
    LOG_WARN("switch status failed", K(ret));
  }
  return ret;
}

int ObConstraintTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConstraintTask has not been inited", K(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("check health failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch (task_status_) {
      case ObDDLTaskStatus::WAIT_TRANS_END:
        if (OB_FAIL(wait_trans_end())) {
          LOG_WARN("wait transaction end failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::CHECK_CONSTRAINT_VALID:
        if (OB_FAIL(validate_constraint_valid())) {
          LOG_WARN("check constraint valid failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::SET_CONSTRAINT_VALIDATE:
        if (OB_FAIL(set_constraint_validated())) {
          LOG_WARN("set constraint validated failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(fail())) {
          LOG_WARN("execute cleanup failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(succ())) {
          LOG_WARN("exeucte succ cleanup failed", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, task status is not valid", K(ret), K(ret), K(task_status_));
    }
    ddl_tracing_.release_span_hierarchy();
  }
  if (OB_FAIL(ret)) {
    add_event_info("constraint task process fail");
    LOG_INFO("process constraint task fail", "ddl_event_info", ObDDLEventInfo());
  }
  return ret;
}

int ObConstraintTask::update_check_constraint_finish(const int ret_code)
{
  int ret = OB_SUCCESS;
  check_job_ret_code_ = ret_code;
  return ret;
}

int ObConstraintTask::check_health()
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

int ObConstraintTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("ObDDLTask serialize failed", K(ret));
  } else if (OB_FAIL(alter_table_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize table arg failed", K(ret));
  }
  return ret;
}

int ObConstraintTask::deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObAlterTableArg tmp_arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, data_len, pos))) {
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

int64_t ObConstraintTask::get_serialize_param_size() const
{
  return alter_table_arg_.get_serialize_size() + ObDDLTask::get_serialize_param_size();
}
void ObConstraintTask::flt_set_task_span_tag() const
{
  FLT_SET_TAG(ddl_task_id, task_id_, ddl_parent_task_id, parent_task_id_,
              ddl_data_table_id, object_id_, ddl_schema_version, schema_version_,
              ddl_snapshot_version, snapshot_version_);
}

void ObConstraintTask::flt_set_status_span_tag() const
{
  switch (task_status_) {
  case ObDDLTaskStatus::WAIT_TRANS_END: {
    FLT_SET_TAG(ddl_data_table_id, object_id_, ddl_schema_version, schema_version_,
                ddl_snapshot_version, snapshot_version_, ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::CHECK_CONSTRAINT_VALID: {
    FLT_SET_TAG(ddl_ret_code, ret_code_);
    break;
  }
  case ObDDLTaskStatus::SET_CONSTRAINT_VALIDATE: {
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
