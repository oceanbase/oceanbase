/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "rootserver/mview/ob_mview_pending_task_define.h"
#include "rootserver/mview/ob_mview_pending_task_manager.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_ddl_common.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_common_id_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace lib;
using namespace number;
using namespace share;
using namespace share::schema;
using namespace sql;

int ObMViewExecutorUtil::number_to_int64(const ObNumber &number, int64_t &int64)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!number.is_valid_int64(int64))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("number is not int64", KR(ret), K(number));
  }
  return ret;
}

int ObMViewExecutorUtil::number_to_uint64(const ObNumber &number, uint64_t &uint64)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!number.is_valid_uint64(uint64))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("number is not uint64", KR(ret), K(number));
  }
  return ret;
}

int ObMViewExecutorUtil::split_table_list(const ObString &table_list, ObIArray<ObString> &tables)
{
  int ret = OB_SUCCESS;
  static const char split_character = ',';
  tables.reset();
  ObString list_str = table_list;
  const char *p = nullptr;
  ObString table_str;
  do {
    p = list_str.find(split_character);
    if (nullptr != p) {
      table_str = list_str.split_on(p);
    } else {
      table_str = list_str;
    }
    if (!table_str.empty() && OB_FAIL(tables.push_back(table_str))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  } while (OB_SUCC(ret) && OB_NOT_NULL(p));
  return ret;
}

int ObMViewExecutorUtil::check_min_data_version(const uint64_t tenant_id,
                                                const uint64_t min_data_version, const char *errmsg)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < min_data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id),
             K(compat_version), K(min_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, errmsg);
  }
  return ret;
}

int ObMViewExecutorUtil::resolve_table_name(const ObCollationType cs_type,
                                            const ObNameCaseMode case_mode,
                                            const bool is_oracle_mode, const ObString &name,
                                            ObString &database_name, ObString &table_name)
{
  int ret = OB_SUCCESS;
  static const char split_character = '.';
  database_name.reset();
  table_name.reset();
  if (OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(name));
  } else {
    ObString name_str = name;
    const char *p = name_str.find(split_character);
    if (p == nullptr) {
      table_name = name_str;
    } else {
      database_name = name_str.split_on(p);
      table_name = name_str;
      if (OB_UNLIKELY(database_name.empty() || table_name.empty() ||
                      nullptr != table_name.find(split_character))) {
        ret = OB_WRONG_TABLE_NAME;
        LOG_WARN("wrong table name", KR(ret), K(name));
      }
    }
    if (OB_SUCC(ret)) {
      const bool preserve_lettercase =
        is_oracle_mode ? true : (case_mode != OB_LOWERCASE_AND_INSENSITIVE);
      upper_db_table_name(case_mode, is_oracle_mode, database_name);
      upper_db_table_name(case_mode, is_oracle_mode, table_name);
      if (!database_name.empty() && OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                                      cs_type, preserve_lettercase, database_name))) {
        LOG_WARN("fail to check and convert database name", KR(ret), K(database_name));
      } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, preserve_lettercase,
                                                                  table_name, is_oracle_mode))) {
        LOG_WARN("fail to check and convert table name", KR(ret), K(cs_type),
                 K(preserve_lettercase), K(table_name));
      }
    }
  }
  return ret;
}

void ObMViewExecutorUtil::upper_db_table_name(const ObNameCaseMode case_mode,
                                              const bool is_oracle_mode, ObString &name)
{
  if (is_oracle_mode) {
    str_toupper(name.ptr(), name.length());
  } else {
    if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      str_tolower(name.ptr(), name.length());
    }
  }
}

int ObMViewExecutorUtil::to_refresh_method(const char c, ObMVRefreshMethod &refresh_method)
{
  int ret = OB_SUCCESS;
  refresh_method = ObMVRefreshMethod::MAX;
  switch (c) {
    case 'a':
    case 'A':
    case 'c':
    case 'C':
      refresh_method = ObMVRefreshMethod::COMPLETE;
      break;
    case 'f':
    case 'F':
      refresh_method = ObMVRefreshMethod::FAST;
      break;
    case '?':
      refresh_method = ObMVRefreshMethod::FORCE;
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid method", KR(ret), K(c));
      break;
  }
  return ret;
}

int ObMViewExecutorUtil::to_refresh_method(const ObMVRefreshMethod refresh_method,
                                           ObString &method_str)
{
  int ret = OB_SUCCESS;
  method_str.reset();
  switch (refresh_method) {
    case ObMVRefreshMethod::COMPLETE:
      method_str = ObString::make_string("C");
      break;
    case ObMVRefreshMethod::FAST:
      method_str = ObString::make_string("F");
      break;
    case ObMVRefreshMethod::FORCE:
      method_str = ObString::make_string("?");
      break;
    case ObMVRefreshMethod::MAX:
      // Caller has not resolved a method yet; leave method_str empty.
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid refresh method", KR(ret), K(refresh_method));
      break;
  }
  return ret;
}

int ObMViewExecutorUtil::to_collection_level(const ObString &str,
                                             ObMVRefreshStatsCollectionLevel &collection_level)
{
  int ret = OB_SUCCESS;
  collection_level = ObMVRefreshStatsCollectionLevel::MAX;
  if (0 == str.case_compare("NONE")) {
    collection_level = ObMVRefreshStatsCollectionLevel::NONE;
  } else if (0 == str.case_compare("TYPICAL")) {
    collection_level = ObMVRefreshStatsCollectionLevel::TYPICAL;
  } else if (0 == str.case_compare("ADVANCED")) {
    collection_level = ObMVRefreshStatsCollectionLevel::ADVANCED;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collection level", KR(ret), K(str));
  }
  return ret;
}

int ObMViewExecutorUtil::generate_refresh_id(const uint64_t tenant_id, int64_t &refresh_id)
{
  int ret = OB_SUCCESS;
  ObCommonID unique_id;
  MTL_SWITCH(tenant_id)
  {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id, unique_id))) {
      LOG_WARN("failed to gen unique id", KR(ret));
    }
  }
  else
  {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id, unique_id))) {
      LOG_WARN("failed to gen unique id", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    refresh_id = unique_id.id();
  }
  return ret;
}

bool ObMViewExecutorUtil::is_mview_refresh_retry_ret_code(int ret_code)
{
  return OB_OLD_SCHEMA_VERSION == ret_code || OB_EAGAIN == ret_code ||
         OB_INVALID_QUERY_TIMESTAMP == ret_code || OB_TASK_EXPIRED == ret_code ||
         OB_REPLICA_NOT_READABLE == ret_code || OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST == ret_code ||
         OB_SCHEMA_ERROR == ret_code ||
         is_master_changed_error(ret_code) || is_partition_change_error(ret_code) ||
         is_ddl_stmt_packet_retry_err(ret_code);
}

int ObMViewExecutorUtil::resolve_mview_list_and_method(ObSchemaGetterGuard *schema_guard,
                                                       ObSQLSessionInfo *session_info,
                                                       const ObString &list,
                                                       const ObString &method,
                                                       uint64_t &mview_id,
                                                       ObMVRefreshMethod &refresh_method)
{
  int ret = OB_SUCCESS;
  mview_id = OB_INVALID_ID;
  refresh_method = ObMVRefreshMethod::MAX;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  ObArray<uint64_t> mview_ids;
  ObArray<ObMVRefreshMethod> refresh_methods;
  ObSchemaChecker schema_checker;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", K(ret), KP(schema_guard));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), KP(session_info));
  } else if (OB_FALSE_IT(tenant_id = session_info->get_effective_tenant_id())) {
  } else if (OB_FAIL(schema_checker.init(*schema_guard, session_info->get_server_sid()))) {
    LOG_WARN("fail to init schema checker", KR(ret));
  } else if (OB_FAIL(session_info->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  }
  // resolve list
  if (OB_SUCC(ret)) {
    ObArray<ObString> mview_names;
    if (OB_FAIL(ObMViewExecutorUtil::split_table_list(list, mview_names))) {
      LOG_WARN("fail to split table list", KR(ret), K(list));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mview_names.count(); ++i) {
      const ObString &mview_name = mview_names.at(i);
      ObString database_name, table_name;
      bool has_synonym = false;
      ObString new_db_name, new_tbl_name;
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObMViewExecutorUtil::resolve_table_name(cs_type, case_mode, lib::is_oracle_mode(),
                                                          mview_name, database_name, table_name))) {
        LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(mview_name));
        LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(mview_name.length()),
                       mview_name.ptr());
      } else if (database_name.empty() &&
                 FALSE_IT(database_name = session_info->get_database_name())) {
      } else if (OB_UNLIKELY(database_name.empty())) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("No database selected", KR(ret));
      } else if (OB_FAIL(schema_checker.get_table_schema_with_synonym(
                   tenant_id, database_name, table_name, false /*is_index_table*/, has_synonym,
                   new_db_name, new_tbl_name, table_schema))) {
        LOG_WARN("fail to get table schema with synonym", KR(ret), K(database_name), K(table_name));
      } else if (OB_ISNULL(table_schema) || OB_UNLIKELY(!table_schema->is_materialized_view())) {
        ret = OB_ERR_MVIEW_NOT_EXIST;
        LOG_WARN("mview not exist", KR(ret), K(database_name), K(table_name), KPC(table_schema));
      } else if (OB_FAIL(add_var_to_array_no_dup(mview_ids, table_schema->get_table_id()))) {
        LOG_WARN("fail to add var to array no duplicate", KR(ret), K(table_schema->get_table_id()));
      }
    }
  }
  // resolve method
  if (OB_SUCC(ret)) {
    ObMVRefreshMethod method_item = ObMVRefreshMethod::MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < method.length(); ++i) {
      const char c = method.ptr()[i];
      if (OB_FAIL(ObMViewExecutorUtil::to_refresh_method(c, method_item))) {
        LOG_WARN("fail to refresh method", KR(ret));
      } else if (OB_FAIL(refresh_methods.push_back(method_item))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (mview_ids.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(mview_ids.count() > 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported refresh multiple mviews", KR(ret), K(mview_ids), K(list), K(method));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh multiple materialized views");
  } else if (OB_UNLIKELY(refresh_methods.count() > 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported refresh using multiple methods", KR(ret), K(refresh_methods), K(list), K(method));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh using multiple methods");
  } else {
    mview_id = mview_ids.at(0);
    refresh_method = refresh_methods.empty() ? ObMVRefreshMethod::MAX : refresh_methods.at(0);
  }
  return ret;
}

int ObMViewExecutorUtil::wait_mview_refresh(sql::ObExecContext &ctx,
                                           uint64_t tenant_id,
                                           int64_t refresh_id,
                                           uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = nullptr;
  const int64_t POLL_INTERVAL_US = 100 * 1000L;
  bool done = false;
  ObSqlString sql;
  if (OB_ISNULL(sql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT status FROM %s"
                                     " WHERE tenant_id = 0 AND refresh_id = %ld AND mview_id = %lu",
                                     OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                                     refresh_id, mview_id))) {
    LOG_WARN("fail to assemble pending task poll sql", KR(ret), K(refresh_id), K(mview_id));
  }
  // Poll pending task table by primary key until the row is gone or terminal.
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("worker interrupted during sync wait", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("fail to query pending task", KR(ret), K(refresh_id), K(mview_id));
        } else if (OB_ISNULL(res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mysql result is null", KR(ret), K(refresh_id), K(mview_id));
        } else if (OB_FAIL(res.get_result()->next())) {
          if (OB_ITER_END == ret) {
            // Row deleted — task finished and was recycled.
            ret = OB_SUCCESS;
            done = true;
          } else {
            LOG_WARN("failed to get result", KR(ret), K(refresh_id), K(mview_id));
          }
        } else {
          int64_t status = 0;
          EXTRACT_INT_FIELD_MYSQL(*res.get_result(), "status", status, int64_t);
          if (OB_SUCC(ret)) {
            done = (status == rootserver::MV_TASK_SUCCESS
                    || status == rootserver::MV_TASK_FAILED
                    || status == rootserver::MV_TASK_CANCELLED);
          }
        }
      }
      if (OB_SUCC(ret) && !done) {
        ob_usleep(POLL_INTERVAL_US);
      }
    }
  }
  // When the loop exits abnormally (timeout / interrupt / poll failure),
  // best-effort fire a kill so RS stops running tasks and recycles ctx.
  // Preserve the original ret — kill is informational; failures are logged.
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    rootserver::ObMViewMaintenanceService *service = MTL(rootserver::ObMViewMaintenanceService *);
    if (OB_ISNULL(service) || OB_ISNULL(service->get_pending_task_manager())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null service or pending task manager", K(tmp_ret), K(tenant_id), K(refresh_id));
    // force_rpc=true: the current session is interrupted (kill query), its
    // interrupted state would cause inner-SQL inside kill_refresh_local to
    // fail with OB_ERR_QUERY_INTERRUPTED; sending RPC runs on a clean thread.
    } else {
      obrpc::ObKillMViewRefreshArg arg;
      arg.tenant_id_ = tenant_id;
      arg.refresh_id_ = refresh_id;
      if (OB_TMP_FAIL(service->get_pending_task_manager()->kill_refresh(arg, true /*force_rpc*/))) {
        LOG_WARN("kill mview refresh failed after abnormal sync wait exit", K(tmp_ret), K(tenant_id), K(refresh_id));
      } else {
        LOG_INFO("kill mview refresh issued after abnormal sync wait exit",
                  K(tenant_id), K(refresh_id), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(load_refresh_run_stats_error_message(ctx, tenant_id, refresh_id))) {
      LOG_WARN("fail to read mview refresh run stats", KR(ret), K(refresh_id));
    }
  }
  return ret;
}

int ObMViewExecutorUtil::load_refresh_run_stats_error_message(sql::ObExecContext &ctx,
                                                              uint64_t tenant_id,
                                                              int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = nullptr;
  if (OB_ISNULL(sql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == refresh_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid refresh id", KR(ret), K(refresh_id));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT result, error_message FROM %s"
                               " WHERE tenant_id = 0 AND refresh_id = %ld",
                               OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME,
                               refresh_id))) {
      LOG_WARN("fail to assemble stats poll sql", KR(ret), K(refresh_id));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("fail to query run stats", KR(ret), K(refresh_id));
        } else if (OB_ISNULL(res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mysql result is null", KR(ret), K(refresh_id));
        } else if (OB_FAIL(res.get_result()->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("run stats row not found after refresh done", KR(ret), K(refresh_id));
          }
        } else {
          int64_t db_result = 0;
          ObString err_msg;
          EXTRACT_INT_FIELD_MYSQL(*res.get_result(), "result", db_result, int64_t);
          if (OB_SUCC(ret)) {
            int tmp_ret = res.get_result()->get_varchar("error_message", err_msg);
            if (OB_SUCCESS != tmp_ret && OB_ERR_NULL_VALUE != tmp_ret) {
              LOG_WARN("get error_message failed, ignored", K(tmp_ret), K(refresh_id));
            }
          }
          if (OB_SUCC(ret) && db_result != 0) {
            ret = static_cast<int>(db_result);
            if (!err_msg.empty()) {
              char err_buf[common::OB_MAX_ERROR_MSG_LEN] = {0};
              const int64_t copy_len = MIN(static_cast<int64_t>(err_msg.length()),
                                           static_cast<int64_t>(sizeof(err_buf) - 1));
              MEMCPY(err_buf, err_msg.ptr(), copy_len);
              err_buf[copy_len] = '\0';
              FORWARD_USER_ERROR(ret, err_buf);
            }
            LOG_WARN("mview refresh failed", KR(ret), K(refresh_id), K(err_msg));
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewExecutorUtil::check_kill_refresh_privilege(sql::ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx.get_my_session()), K(ctx.get_sql_ctx()));
  } else if (OB_FAIL(schema_checker.init(*(ctx.get_sql_ctx()->schema_guard_)))) {
    LOG_WARN("fail to init schema checker", KR(ret));
  } else if (ObSchemaChecker::is_ora_priv_check()) {
    if (OB_FAIL(schema_checker.check_ora_ddl_priv(ctx.get_my_session()->get_effective_tenant_id(),
                                                  ctx.get_my_session()->get_priv_user_id(),
                                                  ObString(""),
                                                  stmt::T_ALTER_SYSTEM_SET_PARAMETER,
                                                  ctx.get_my_session()->get_enable_role_array()))) {
      LOG_WARN("fail to check ora ddl priv", KR(ret));
    }
  } else {
    ObNeedPriv need_priv;
    ObStmtNeedPrivs stmt_need_privs;
    ObSessionPrivInfo session_priv;
    const common::ObIArray<uint64_t> &enable_role_id_array = ctx.get_my_session()->get_enable_role_array();
    stmt_need_privs.need_privs_.set_allocator(&ctx.get_allocator());
    need_priv.priv_set_ = OB_PRIV_ALTER_SYSTEM;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    if (OB_FAIL(ctx.get_my_session()->get_session_priv_info(session_priv))) {
      LOG_WARN("fail to get session priv info", KR(ret));
    } else if (OB_FAIL(stmt_need_privs.need_privs_.init(1))) {
      LOG_WARN("fail to init need privs", KR(ret));
    } else if (OB_FAIL(stmt_need_privs.need_privs_.push_back(need_priv))) {
      LOG_WARN("fail to push back need priv", KR(ret));
    } else if (OB_FAIL(schema_checker.check_priv(session_priv, enable_role_id_array, stmt_need_privs))) {
      LOG_WARN("fail to check priv", KR(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
