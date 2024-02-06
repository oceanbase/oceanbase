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

#define USING_LOG_PREFIX SERVER

#include "ob_inner_sql_rpc_processor.h"
#include "ob_inner_sql_connection.h"
#include "ob_inner_sql_connection_pool.h"
#include "ob_inner_sql_result.h"
#include "ob_resource_inner_sql_connection_pool.h"
#include "observer/omt/ob_multi_tenant.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_iarray.h"
#include "storage/tx/ob_multi_data_source.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::transaction::tablelock;

namespace oceanbase
{
namespace obrpc
{

int ObInnerSqlRpcP::process_start_transaction(
    sqlclient::ObISQLConnection *conn,
    const ObSqlString &start_trans_sql,
    const ObInnerSQLTransmitArg &transmit_arg,
    ObInnerSQLTransmitResult &transmit_result)
{
  int ret = OB_SUCCESS;
  bool with_snap_shot = (0 == start_trans_sql.string().compare(
                                "START TRANSACTION WITH CONSISTENT SNAPSHOT"));

  if (OB_FAIL(conn->start_transaction(transmit_arg.get_tenant_id(), with_snap_shot))) {
    transmit_result.set_err_code(ret);
    LOG_WARN("execute start_transaction failed", K(ret), K(transmit_arg), K(with_snap_shot));
  } else {
    transmit_result.set_conn_id(
        static_cast<observer::ObInnerSQLConnection *>(conn)->get_resource_conn_id());
  }

  return ret;
}

int ObInnerSqlRpcP::process_register_mds(sqlclient::ObISQLConnection *conn,
                                         const ObInnerSQLTransmitArg &arg)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *inner_conn = static_cast<observer::ObInnerSQLConnection *>(conn);
  transaction::ObMDSInnerSQLStr mds_str;
  int64_t pos = 0;
  if (OB_FAIL(mds_str.deserialize(arg.get_inner_sql().ptr(), arg.get_inner_sql().length(), pos))) {
    LOG_WARN("deserialize multi data source str failed", K(ret), K(arg), K(pos));
  } else if (OB_FAIL(inner_conn->register_multi_data_source(
                 arg.get_tenant_id(), mds_str.get_ls_id(), mds_str.get_msd_type(),
                 mds_str.get_msd_buf(), mds_str.get_msd_buf_len(), mds_str.get_register_flag()))) {
    LOG_WARN("register multi data source failed", K(ret), K(arg.get_tenant_id()), K(mds_str));
  }

  return ret;
}

int ObInnerSqlRpcP::process_rollback(
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(conn->rollback())) {
    LOG_WARN("execute rollback failed", K(ret));
  }

  return ret;
}

int ObInnerSqlRpcP::process_commit(
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(conn->commit())) {
    LOG_WARN("execute commit failed", K(ret));
  }

  return ret;
}

int ObInnerSqlRpcP::process_write(
    sqlclient::ObISQLConnection *conn,
    const ObSqlString &write_sql,
    const ObInnerSQLTransmitArg &transmit_arg,
    ObInnerSQLTransmitResult &transmit_result)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  ResourceGroupGuard guard(transmit_arg.get_consumer_group_id());
  if (OB_FAIL(conn->execute_write(transmit_arg.get_tenant_id(), write_sql.ptr(), affected_rows))) {
    LOG_WARN("execute write failed", K(ret), K(transmit_arg), K(write_sql));
  } else {
    transmit_result.set_affected_rows(affected_rows);
    transmit_result.set_stmt_type(
    static_cast<observer::ObInnerSQLConnection *>(conn)->get_session().get_stmt_type());
  }

  return ret;
}

int ObInnerSqlRpcP::process_read(
    sqlclient::ObISQLConnection *conn,
    const ObSqlString &read_sql,
    const ObInnerSQLTransmitArg &transmit_arg,
    ObInnerSQLTransmitResult &transmit_result)
{
  int ret = OB_SUCCESS;
  common::ObScanner &scanner = transmit_result.get_scanner();
  scanner.set_found_rows(0);

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *sql_result = NULL;
    if (OB_FAIL(conn->execute_read(GCONF.cluster_id, transmit_arg.get_tenant_id(), read_sql.ptr(), res))) {
      LOG_WARN("failed to exec read_sql", K(ret), K(read_sql));
    } else {
      observer::ObInnerSQLResult *inner_result = NULL;
      const common::ObIArray<common::ObField> *col_field_array = NULL;
      if (OB_ISNULL(sql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is NULL", K(ret));
      } else if (FALSE_IT(inner_result = static_cast<observer::ObInnerSQLResult*>(sql_result))) {
      } else if (OB_ISNULL(col_field_array = inner_result->result_set().get_field_columns())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_field_array is NULL", K(ret));
      } else if (OB_FAIL(transmit_result.copy_field_columns(*col_field_array))) {
        LOG_WARN("copy_field_columns failed", K(ret), K(col_field_array->count()));
      } else {
        int64_t total_row_cnt = 0;
        int64_t row_count = 0; // for single scanner
        bool need_flush = false;
        bool need_reset_field_columns = true;
        const ObNewRow *row = NULL;
        if (OB_FAIL(sql_result->next())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get next row", K(ret));
          }
        }
        while (OB_SUCC(ret)) {
          if (OB_ISNULL(row = sql_result->get_row())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row is NULL", K(ret));
          } else if (OB_FAIL(scanner.add_row(*row))) {
            if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) { // error happened, will break
              LOG_WARN("fail add row to scanner", K(ret));
            } else {
              ret = OB_SUCCESS;
              need_flush = true;
              scanner.set_found_rows(row_count);
            }
          } else {
            ++row_count;
            ++total_row_cnt;
          }
          if (OB_SUCC(ret)) {
            if (need_flush) { // last row is not be used
              if (OB_FAIL(obrpc::ObRpcProcessor< obrpc::ObInnerSQLRpcProxy::ObRpc<
                  obrpc::OB_INNER_SQL_SYNC_TRANSMIT> >::flush(THIS_WORKER.get_timeout_remain()))) {
                LOG_WARN("fail to flush", K(ret));
              } else {
                LOG_DEBUG("flush scanner successfully", K(scanner), K(scanner.get_found_rows()));
                if (need_reset_field_columns) { // field columns info only need to be returned once
                  transmit_result.reset_field_columns();
                  need_reset_field_columns = false;
                }
                scanner.reuse();
                scanner.get_datum_store().set_dumped(false); // not support serialize if enable dump
                scanner.get_row_store().set_use_compact(false);
                need_flush = false;
                row_count = 0;
              }
            } else if (OB_FAIL(sql_result->next())) { // last row is be used
              if (OB_UNLIKELY(OB_ITER_END != ret)) {
                LOG_WARN("failed to get next row", K(ret));
              }
            }
          }
        } // while end
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (0 != row_count) {
            scanner.set_found_rows(row_count);
          }
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObInnerSqlRpcP::create_tmp_session(
    uint64_t tenant_id,
    sql::ObSQLSessionInfo *&tmp_session,
    sql::ObFreeSessionCtx &free_session_ctx,
    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != tmp_session)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tmp_session is not null.", K(ret));
  } else if (NULL == GCTX.session_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session manager is NULL", K(ret));
  } else {
    uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
    uint64_t proxy_sid = 0;
    if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
      LOG_WARN("alloc session id failed", K(ret));
    } else if (OB_FAIL(GCTX.session_mgr_->create_session(tenant_id, sid, proxy_sid,
                                                         ObTimeUtility::current_time(),
                                                         tmp_session))) {
      GCTX.session_mgr_->mark_sessid_unused(sid);
      tmp_session = NULL;
      LOG_WARN("create session failed", K(ret), K(sid));
    } else {
      const bool is_extern_session = true;
      if (OB_NOT_NULL(tmp_session)
            && OB_FAIL(observer::ObInnerSQLConnection::init_session_info(
            tmp_session,
            is_extern_session,
            is_oracle_mode,
            false))) {
        LOG_WARN("fail to init session info", K(ret), KPC(tmp_session));
      }
      free_session_ctx.sessid_ = sid;
      free_session_ctx.proxy_sessid_ = proxy_sid;
    }
  }
  return ret;
}

void ObInnerSqlRpcP::cleanup_tmp_session(
    sql::ObSQLSessionInfo *&tmp_session,
    sql::ObFreeSessionCtx &free_session_ctx)
{
  if (NULL != GCTX.session_mgr_ && NULL != tmp_session) {
    tmp_session->set_session_sleep();
    GCTX.session_mgr_->revert_session(tmp_session);
    GCTX.session_mgr_->free_session(free_session_ctx);
    tmp_session = NULL;
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
  }
  ObActiveSessionGuard::setup_default_ash(); // enforce cleanup for future RPC cases
}

int ObInnerSqlRpcP::process()
{
  int ret = OB_SUCCESS;
  const ObInnerSQLTransmitArg &transmit_arg = arg_;
  ObInnerSQLTransmitResult &transmit_result = result_;

  sql::ObFreeSessionCtx free_session_ctx;
  sql::ObSQLSessionInfo *tmp_session = NULL; // session got from session_mgr_

  if (OB_ISNULL(gctx_.res_inner_conn_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr",  K(ret), K(gctx_.sql_proxy_));
  } else if (!gctx_.res_inner_conn_pool_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_inner_conn_pool_ is not inited", K(ret));
  } else if (OB_INVALID_ID == transmit_arg.get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id in transmit_arg is invalid", K(ret), K(transmit_arg));
  } else if (transmit_arg.get_inner_sql().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_sql in transmit_arg is empty", K(ret), K(transmit_arg));
  } else if (!GCTX.omt_->is_available_tenant(transmit_arg.get_tenant_id())) {
    ret = OB_TENANT_NOT_IN_SERVER;
    LOG_WARN("tenant not in server", K(ret), K(transmit_arg));
  } else {
    ObSqlString sql_str;
    int64_t affected_rows = 0;
    sqlclient::ObISQLConnection *conn = NULL;
    observer::ObInnerSQLConnection *inner_conn = NULL;
    observer::ObResourceInnerSQLConnectionPool *pool = gctx_.res_inner_conn_pool_;
    transmit_result.set_tenant_id(transmit_arg.get_tenant_id());
    common::ObScanner &scanner = transmit_result.get_scanner();
    scanner.get_row_store().set_use_compact(false);
    scanner.get_datum_store().set_dumped(false); // just for serializing
    const int64_t owner_cluster_id = GCONF.cluster_id;
    if (0 == transmit_arg.get_inner_sql().length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner sql length is 0", K(ret));
    } else if (owner_cluster_id != transmit_arg.get_source_cluster_id()
               && ObInnerSQLTransmitArg::OPERATION_TYPE_EXECUTE_READ != transmit_arg.get_operation_type()) {
      //only read can across cluster
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("only read can execute across cluster", KR(ret), K(owner_cluster_id), K(transmit_arg));
    } else if (transmit_arg.get_use_external_session()
                && ((ObInnerSQLTransmitArg::OPERATION_TYPE_EXECUTE_READ != transmit_arg.get_operation_type() && ObInnerSQLTransmitArg::OPERATION_TYPE_EXECUTE_WRITE != transmit_arg.get_operation_type())
                   || OB_INVALID_ID != transmit_arg.get_conn_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only init remote conn with sess in read/write and not in trans", KR(ret), K(transmit_arg));
    } else if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", K(ret));
    } else if (transmit_arg.get_use_external_session()
                && OB_FAIL(create_tmp_session(transmit_arg.get_tenant_id(), tmp_session, free_session_ctx, transmit_arg.get_is_oracle_mode()))) {
      LOG_WARN("fail to create_tmp_session", K(ret), K(transmit_arg));
    } else if (OB_FAIL(sql_str.assign_fmt("%.*s", transmit_arg.get_inner_sql().length(),
                                                  transmit_arg.get_inner_sql().ptr()))) {
      LOG_WARN("assign sql to write_sql failed", K(ret), K(transmit_arg.get_inner_sql()));
    } else if (OB_FAIL(pool->acquire(transmit_arg.get_conn_id(), transmit_arg.get_is_oracle_mode(),
               ObInnerSQLTransmitArg::OPERATION_TYPE_ROLLBACK == transmit_arg.get_operation_type(),
               conn, tmp_session))) {
      LOG_WARN("failed to acquire inner connection", K(ret), K(transmit_arg));
    }
    /* init session info */
    if (OB_SUCC(ret) && OB_NOT_NULL(tmp_session)) {
      uint64_t tenant_id = transmit_arg.get_tenant_id();
      share::schema::ObSchemaGetterGuard schema_guard;
      const ObSimpleTenantSchema *tenant_schema = NULL;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", K(ret));
      } else {
        tmp_session->set_current_trace_id(ObCurTraceId::get_trace_id());
        tmp_session->switch_tenant_with_name(transmit_arg.get_tenant_id(), tenant_schema->get_tenant_name_str());
        ObString sql_stmt(sql_str.ptr());
        if (OB_FAIL(tmp_session->set_session_active(
            sql_stmt,
            0,  /* ignore this parameter */
            ObTimeUtility::current_time(),
            obmysql::COM_QUERY))) {
          LOG_WARN("failed to set tmp session active", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(conn))) {
      // do nothing, because conn was set null if need kill_using_conn in aquire
      LOG_WARN("conn is null", K(ret), K(transmit_arg.get_conn_id()), KP(conn));
    } else if (OB_FAIL(set_session_param_to_conn(conn, transmit_arg))) {
      LOG_WARN("failed to set session param to conn", K(ret), K(transmit_arg));
    } else {
      // backup && restore worker/session timeout.
      observer::ObInnerSQLConnection::TimeoutGuard timeout_guard(*inner_conn);
      if (OB_FAIL(inner_conn->set_session_timeout(transmit_arg.get_query_timeout(), transmit_arg.get_trx_timeout()))) {
        LOG_WARN("failed to set_session_timeout", K(ret), K(transmit_arg));
      } else if (FALSE_IT(THIS_WORKER.set_timeout_ts(transmit_arg.get_worker_timeout()))) {
      } else {
        switch (transmit_arg.get_operation_type()) {
          case ObInnerSQLTransmitArg::OPERATION_TYPE_START_TRANSACTION: {
            if (OB_FAIL(process_start_transaction(conn, sql_str, transmit_arg, transmit_result))) {
              LOG_WARN("process_start_transaction failed", K(ret), K(transmit_arg), K(sql_str));
            }
            break;
          }
          case ObInnerSQLTransmitArg::OPERATION_TYPE_ROLLBACK: {
            if (OB_FAIL(process_rollback(conn))) {
              LOG_WARN("process_rollback failed", K(ret));
            }
            break;
          }
          case ObInnerSQLTransmitArg::OPERATION_TYPE_COMMIT: {
            if (OB_FAIL(process_commit(conn))) {
              LOG_WARN("process_commit failed", K(ret));
            }
            break;
          }
          case ObInnerSQLTransmitArg::OPERATION_TYPE_EXECUTE_WRITE: {
            if (OB_FAIL(process_write(conn, sql_str, transmit_arg, transmit_result))) {
              LOG_WARN("process_write failed", K(ret), K(transmit_arg), K(sql_str));
            }
            break;
          }
          case ObInnerSQLTransmitArg::OPERATION_TYPE_EXECUTE_READ: {
            if (OB_FAIL(process_read(conn, sql_str, transmit_arg, transmit_result))) {
              LOG_WARN("process_read failed", K(ret), K(sql_str));
            }
            break;
          }
          case ObInnerSQLTransmitArg::OPERATION_TYPE_REGISTER_MDS: {
            if (OB_FAIL(process_register_mds(conn, transmit_arg))) {
              LOG_WARN("process register multi source data failed", K(ret));
            }
            break;
          }
          // deal with lock rpc
          case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLE:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLET:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJ:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJ:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_PART:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_PART:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_SUBPART:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_SUBPART:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_ALONE_TABLET:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_ALONE_TABLET:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJS:
          case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJS: {

            if (OB_FAIL(ObInnerConnectionLockUtil::process_lock_rpc(transmit_arg, conn))) {
              LOG_WARN("process lock rpc failed", K(ret), K(transmit_arg.get_operation_type()));
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Unknown operation type", K(ret), K(transmit_arg.get_operation_type()));
            break;
          }
        }
      }
    }
    if (OB_NOT_NULL(conn)) {
      bool need_reuse_conn = true;
      const bool is_start_trans = ObInnerSQLTransmitArg::OPERATION_TYPE_START_TRANSACTION
                                    == transmit_arg.get_operation_type();
      const bool in_trans = is_start_trans || OB_INVALID_ID != transmit_arg.get_conn_id();
      if (!in_trans
          // need to release conn if not in a trans
          || ObInnerSQLTransmitArg::OPERATION_TYPE_ROLLBACK == transmit_arg.get_operation_type()
          || ObInnerSQLTransmitArg::OPERATION_TYPE_COMMIT == transmit_arg.get_operation_type()
          // need to release conn after commit or rollback
          || (OB_FAIL(ret) && is_start_trans)
          // need to release conn if start_trans was failed
          ) {
        need_reuse_conn = false;
      }
      int tmp_ret = gctx_.res_inner_conn_pool_->release(need_reuse_conn, conn);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret; // don't cover errno if old errno was not OB_SUCCESS
        LOG_WARN("release connection failed", K(ret), K(tmp_ret), K(transmit_arg.get_conn_id()));
      }
      if (OB_FAIL(ret)) {
        transmit_result.set_err_code(ret);
      }
    }
    cleanup_tmp_session(tmp_session, free_session_ctx);
  }

  return ret;
}

int ObInnerSqlRpcP::set_session_param_to_conn(
    sqlclient::ObISQLConnection *conn,
    const ObInnerSQLTransmitArg &transmit_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is null", K(ret));
  } else {
    conn->set_is_load_data_exec(transmit_arg.get_is_load_data_exec());
    conn->set_nls_formats(transmit_arg.get_nls_formats());
    if (OB_FAIL(conn->set_ddl_info(&transmit_arg.get_ddl_info()))) {
      LOG_WARN("fail to set ddl info", K(ret), K(transmit_arg));
    } else if (0 != transmit_arg.get_sql_mode() && OB_FAIL(conn->set_session_variable("sql_mode", transmit_arg.get_sql_mode()))) {
      LOG_WARN("fail to set sql mode", K(ret), K(transmit_arg));
    } else if (transmit_arg.get_tz_info_wrap().is_valid() && OB_FAIL(conn->set_tz_info_wrap(transmit_arg.get_tz_info_wrap()))) {
      LOG_WARN("fail to set tz info wrap", K(ret), K(transmit_arg));
    }
  }
  return ret;
}

ResourceGroupGuard::ResourceGroupGuard(const int32_t group_id)
  : group_change_(false), old_group_id_(0)
{
  if (is_user_group(group_id)) {
    old_group_id_ = THIS_WORKER.get_group_id();
    THIS_WORKER.set_group_id(group_id);
    group_change_ = true;
  }
}

ResourceGroupGuard::~ResourceGroupGuard()
{
  if (group_change_) {
    THIS_WORKER.set_group_id(old_group_id_);
  }
}

}
} // namespace oceanbase

