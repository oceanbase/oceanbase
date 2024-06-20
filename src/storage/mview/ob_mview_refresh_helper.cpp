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

#include "storage/mview/ob_mview_refresh_helper.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/ob_mview_transaction.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace lib;
using namespace number;
using namespace observer;
using namespace share;
using namespace sql;
using namespace transaction;
using namespace transaction::tablelock;

int ObMViewRefreshHelper::get_current_scn(SCN &current_scn)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", KR(ret));
  } else {
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else if (OB_FAIL(
                 txs->get_read_snapshot_version(timeout_ctx.get_abs_timeout(), current_scn))) {
      LOG_WARN("get read snapshot version", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshHelper::lock_mview(ObMViewTransaction &trans, const uint64_t tenant_id,
                                     const uint64_t mview_id, const bool try_lock)
{
  int ret = OB_SUCCESS;
  ObTableLockOwnerID owner_id;
  if (OB_UNLIKELY(!trans.is_started() || OB_INVALID_TENANT_ID == tenant_id ||
                  OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(trans.is_started()), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                 get_tid_cache()))) {
    LOG_WARN("failed to get owner id", K(ret), K(get_tid_cache()));
  } else {
    const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
    ObInnerSQLConnection *conn = nullptr;
    ObLockObjRequest lock_arg;
    lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_MATERIALIZED_VIEW;
    lock_arg.obj_id_ = mview_id;
    lock_arg.owner_id_ = owner_id;
    lock_arg.lock_mode_ = EXCLUSIVE;
    lock_arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("conn_ is NULL", KR(ret));
    } else if (try_lock) {
      lock_arg.timeout_us_ = 0;
    } else {
      ObTimeoutCtx timeout_ctx;
      if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT))) {
        LOG_WARN("fail to set default timeout ctx", KR(ret));
      } else {
        lock_arg.timeout_us_ = timeout_ctx.get_timeout();
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("lock obj start", K(lock_arg));
      if (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, lock_arg, conn))) {
        LOG_WARN("fail to lock obj", KR(ret));
      }
      LOG_DEBUG("lock obj end", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshHelper::generate_purge_mlog_sql(ObSchemaGetterGuard &schema_guard,
                                                  const uint64_t tenant_id, const uint64_t mlog_id,
                                                  const SCN &purge_scn, const int64_t purge_log_parallel,
                                                  ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reuse();
  const ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  bool is_oracle_mode = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mlog_id ||
                  !purge_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mlog_id), K(purge_scn));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mlog_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mlog_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is nullptr", KR(ret), K(tenant_id), K(mlog_id));
  } else if (OB_UNLIKELY(!table_schema->is_mlog_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type not mlog", KR(ret), KPC(table_schema));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is nullptr", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(tenant_id, mlog_id,
                                                                            is_oracle_mode))) {
    LOG_WARN("check if oracle mode failed", KR(ret), K(mlog_id));
  } else {
    ObArenaAllocator allocator("ObMVRefTmp");
    ObString database_name;
    ObString table_name;
    if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(
          allocator, database_schema->get_database_name_str(), database_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(database_schema->get_database_name_str()), K(is_oracle_mode));
    } else if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(
                 allocator, table_schema->get_table_name_str(), table_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(table_schema->get_table_name_str()), K(is_oracle_mode));
    } else {
      if (is_oracle_mode) {
        if (OB_FAIL(sql_string.assign_fmt("DELETE /*+ ENABLE_PARALLEL_DML PARALLEL(%d)*/ FROM \"%.*s\".\"%.*s\" WHERE ora_rowscn <= %lu;",
                                          static_cast<int>(purge_log_parallel),
                                          static_cast<int>(database_name.length()),
                                          database_name.ptr(),
                                          static_cast<int>(table_name.length()), table_name.ptr(),
                                          purge_scn.get_val_for_sql()))) {
          LOG_WARN("fail to assign sql", KR(ret));
        }
      } else {
        if (OB_FAIL(sql_string.assign_fmt("DELETE /*+ ENABLE_PARALLEL_DML PARALLEL(%d)*/ FROM `%.*s`.`%.*s` WHERE ora_rowscn <= %lu;",
                                          static_cast<int>(purge_log_parallel),
                                          static_cast<int>(database_name.length()),
                                          database_name.ptr(),
                                          static_cast<int>(table_name.length()), table_name.ptr(),
                                          purge_scn.get_val_for_sql()))) {
          LOG_WARN("fail to assign sql", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObMViewRefreshHelper::get_table_row_num(ObMViewTransaction &trans, const uint64_t tenant_id,
                                            const uint64_t table_id, const SCN &scn,
                                            int64_t &num_rows)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  bool is_oracle_mode = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is nullptr", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is nullptr", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(tenant_id, table_id,
                                                                            is_oracle_mode))) {
    LOG_WARN("check if oracle mode failed", KR(ret), K(table_id));
  } else {
    ObArenaAllocator allocator("ObMVRefTmp");
    ObString database_name;
    ObString table_name;
    if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(
          allocator, database_schema->get_database_name_str(), database_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(database_schema->get_database_name_str()), K(is_oracle_mode));
    } else if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(
                 allocator, table_schema->get_table_name_str(), table_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(table_schema->get_table_name_str()), K(is_oracle_mode));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObSqlString sql;
        sqlclient::ObMySQLResult *sql_result = nullptr;
        int64_t count = 0;
        if (is_oracle_mode) {
          if (OB_FAIL(sql.assign_fmt("select count(*) as COUNT from \"%.*s\".\"%.*s\"",
                                     static_cast<int>(database_name.length()), database_name.ptr(),
                                     static_cast<int>(table_name.length()), table_name.ptr()))) {
            LOG_WARN("fail to assign sql", KR(ret));
          } else if (scn.is_valid() &&
                     OB_FAIL(sql.append_fmt(" as of scn %ld", scn.get_val_for_sql()))) {
            LOG_WARN("fail to append sql", KR(ret));
          }
        } else {
          if (OB_FAIL(sql.assign_fmt("select count(*) as COUNT from `%.*s`.`%.*s`",
                                     static_cast<int>(database_name.length()), database_name.ptr(),
                                     static_cast<int>(table_name.length()), table_name.ptr()))) {
            LOG_WARN("fail to assign sql", KR(ret));
          } else if (scn.is_valid() &&
                     OB_FAIL(sql.append_fmt(" as of snapshot %ld", scn.get_val_for_sql()))) {
            LOG_WARN("fail to append sql", KR(ret));
          }
        }
        OZ(trans.read(res, tenant_id, sql.ptr()), sql);
        CK(OB_NOT_NULL(res.get_result()));
        OX(sql_result = res.get_result());
        OZ(sql_result->next());
        if (is_oracle_mode) {
          EXTRACT_INT_FIELD_FROM_NUMBER_MYSQL(*sql_result, COUNT, count);
        } else {
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "COUNT", count, int64_t);
        }
        OX(num_rows = count);
      }
    }
  }
  return ret;
}

int ObMViewRefreshHelper::get_mlog_dml_row_num(ObMViewTransaction &trans, const uint64_t tenant_id,
                                               const uint64_t table_id, const ObScnRange &scn_range,
                                               int64_t &num_rows_ins, int64_t &num_rows_upd,
                                               int64_t &num_rows_del)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  bool is_oracle_mode = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is nullptr", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(!table_schema->is_mlog_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not materialized view log", KR(ret), KPC(table_schema));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is nullptr", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(tenant_id, table_id,
                                                                            is_oracle_mode))) {
    LOG_WARN("check if oracle mode failed", KR(ret), K(table_id));
  } else {
    ObArenaAllocator allocator("ObMVRefTmp");
    ObString database_name;
    ObString table_name;
    if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(
          allocator, database_schema->get_database_name_str(), database_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(database_schema->get_database_name_str()), K(is_oracle_mode));
    } else if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(
                 allocator, table_schema->get_table_name_str(), table_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", KR(ret),
               K(table_schema->get_table_name_str()), K(is_oracle_mode));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObSqlString sql;
        sqlclient::ObMySQLResult *sql_result = nullptr;
        ObString dml_type;
        int64_t count = 0;
        if (is_oracle_mode) {
          if (scn_range.start_scn_.is_valid()) {
            OZ(sql.assign_fmt(
              "select DMLTYPE$$, count(*) as COUNT from \"%.*s\".\"%.*s\""
              " where ora_rowscn > %lu and ora_rowscn <= %lu"
              " group by DMLTYPE$$;",
              static_cast<int>(database_name.length()), database_name.ptr(),
              static_cast<int>(table_name.length()), table_name.ptr(),
              scn_range.start_scn_.get_val_for_sql(), scn_range.end_scn_.get_val_for_sql()));
          } else {
            OZ(
              sql.assign_fmt("select DMLTYPE$$, count(*) as COUNT from \"%.*s\".\"%.*s\""
                             " where ora_rowscn <= %lu"
                             " group by DMLTYPE$$;",
                             static_cast<int>(database_name.length()), database_name.ptr(),
                             static_cast<int>(table_name.length()), table_name.ptr(),
                             scn_range.end_scn_.get_val_for_sql()));
          }
        } else {
          if (scn_range.start_scn_.is_valid()) {
            OZ(sql.assign_fmt(
              "select DMLTYPE$$, count(*) as COUNT from `%.*s`.`%.*s`"
              " where ora_rowscn > %lu and ora_rowscn <= %lu"
              " group by DMLTYPE$$;",
              static_cast<int>(database_name.length()), database_name.ptr(),
              static_cast<int>(table_name.length()), table_name.ptr(),
              scn_range.start_scn_.get_val_for_sql(), scn_range.end_scn_.get_val_for_sql()));
          } else {
            OZ(
              sql.assign_fmt("select DMLTYPE$$, count(*) as COUNT from `%.*s`.`%.*s`"
                             " where ora_rowscn <= %lu"
                             " group by DMLTYPE$$;",
                             static_cast<int>(database_name.length()), database_name.ptr(),
                             static_cast<int>(table_name.length()), table_name.ptr(),
                             scn_range.end_scn_.get_val_for_sql()));
          }
        }
        OZ(trans.read(res, tenant_id, sql.ptr()), sql);
        CK(OB_NOT_NULL(res.get_result()));
        OX(sql_result = res.get_result());
        while (OB_SUCC(ret)) {
          if (OB_FAIL(sql_result->next())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get next", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            OZ(sql_result->get_varchar("DMLTYPE$$", dml_type));
            if (is_oracle_mode) {
              EXTRACT_INT_FIELD_FROM_NUMBER_MYSQL(*sql_result, COUNT, count);
            } else {
              EXTRACT_INT_FIELD_MYSQL(*sql_result, "COUNT", count, int64_t);
            }
            CK(dml_type.length() == 1);
            if (OB_SUCC(ret)) {
              switch (dml_type.ptr()[0]) {
                case 'I':
                case 'i':
                  num_rows_ins = count;
                  break;
                case 'U':
                case 'u':
                  num_rows_upd = count;
                  break;
                case 'D':
                case 'd':
                  num_rows_del = count;
                  break;
                default:
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected dmp type", KR(ret), K(table_id), K(dml_type));
                  break;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
