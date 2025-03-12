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
#include "observer/mysql/obmp_reset_connection.h"
#include "sql/ob_sql.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
namespace oceanbase
{
namespace observer
{

int ObMPResetConnection::process()
{
  int ret = OB_SUCCESS;
  bool need_disconnect = false;
  bool need_response_error = false;
  ObSQLSessionInfo *session = NULL;
  ObSMConnection *conn = NULL;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = OB_INVALID_ID;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  if (OB_FAIL(get_session(session))) {
    LOG_ERROR("get session  fail", K(ret));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null conn", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get session info", K(ret), K(session));
  } else {
    THIS_WORKER.set_session(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    int64_t execution_id = 0;
    int64_t query_timeout = 0;
    ObWaitEventStat total_wait_desc;
    ObMaxWaitGuard max_wait_guard(nullptr);
    ObTotalWaitGuard total_wait_guard(nullptr);
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    session->update_last_active_time();
    session->set_query_start_time(ObTimeUtility::current_time());
    LOG_TRACE("begin reset connection. ", K(session->get_sessid()), K(session->get_effective_tenant_id()));
    tenant_id = session->get_effective_tenant_id();
    session->set_txn_free_route(pkt.txn_free_route());
    if (OB_FAIL(process_extra_info(*session, pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else if (FALSE_IT(session->post_sync_session_info())) {
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      OB_LOG(WARN,"fail get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
      LOG_WARN("get sys variable schema failed", K(ret));
    } else if (OB_ISNULL(sys_variable_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable schema is null", K(ret));
    } else if (OB_FAIL(session->load_all_sys_vars(*sys_variable_schema, true))) {
      LOG_WARN("load system variables failed", K(ret));
    } else if (OB_FAIL(session->update_database_variables(&schema_guard))) {
      OB_LOG(WARN, "failed to update database variables", K(ret));
    } else if (OB_FAIL(update_proxy_and_client_sys_vars(*session))) {
      LOG_WARN("update_proxy_and_client_sys_vars failed", K(ret));
    } else if (OB_FAIL(update_charset_sys_vars(*conn, *session))) {
      LOG_WARN("fail to update charset sys vars", K(ret));
    } else if (OB_FAIL(session->get_query_timeout(query_timeout))) {
      LOG_WARN("failed to get query timeout", K(ret), K(query_timeout));
    } else if (OB_ISNULL(gctx_.sql_engine_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid sql engine", K(ret), K(gctx_));
    } else if (FALSE_IT(execution_id = gctx_.sql_engine_->get_execution_id())) {
      //nothing to do
    } else if (OB_FAIL(set_session_active("reset connection.", *session, ObTimeUtil::current_time()))) {
      LOG_WARN("fail to set session active", K(ret));
    } else {
      THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
      session->set_current_execution_id(execution_id);
    }

    /*
     * https://dev.mysql.com/doc/c-api/5.7/en/mysql-reset-connection.html
     * according to mysql doc , following work needs to be done
     *  1. Rolls back any active transactions and resets autocommit mode.
     *  2. Releases all table locks. (OB use row lock, do not need do this)
     *  3. Closes (and drops) all TEMPORARY tables.
     *  4. Reinitializes session system variables to the values of the corresponding global system variables,
     *       including system variables that are set implicitly by statements such as SET NAMES.
     *  5. Loses user-defined variable settings.
     *  6. Releases prepared statements. (include ps stmt, ps cursor, piece)
     *  7. Closes HANDLER variables. (OB not support HANDLER)
     *  8. Resets the value of LAST_INSERT_ID() to 0.
     *  9. Releases locks acquired with GET_LOCK(). (OB not support GET_LOCK())
     *  10.OB unique design
     *      10.1  pl debug
     *      10.2  package state
    */

    // 1. Rolls back any active transactions and resets autocommit mode.
    if (OB_SUCC(ret)) {
      // for XA trans, can not rollback it directly, use kill_tx to abort it
      if (session->associated_xa()) {
        if (OB_FAIL(ObSqlTransControl::kill_tx(session, OB_TRANS_ROLLBACKED))) {
          OB_LOG(WARN, "fail to kill xa trans for reset connection", K(ret));
        }
      } else if (OB_FAIL(ObSqlTransControl::rollback_trans(session, need_disconnect))) {
        OB_LOG(WARN, "fail to rollback trans for reset connection", K(ret), K(need_disconnect));
      }
    }

    // 3. Closes (and drops) all TEMPORARY tables.
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(OB_FAIL(session->drop_temp_tables(false, false, true)))) {
        LOG_WARN("fail to drop temp tables", K(ret));
      }
      session->refresh_temp_tables_sess_active_time();
    }

    // 4. Reinitializes session system variables to the values of the corresponding global system variables
    if (OB_SUCC(ret)) {
      //session->load_all_sys_vars_default();
      OZ (session->reset_sys_vars());
    }

    // 5. Loses user-defined variable settings.
    if (OB_SUCC(ret)) {
      session->get_user_var_val_map().reuse();
    }

    // 6. Releases prepared statements. (include ps stmt, ps cursor, piece)
    if (OB_SUCC(ret)) {
      // 6.1 ps stmt
      if (OB_FAIL(session->close_all_ps_stmt())) {
        LOG_WARN("failed to close all stmt", K(ret));
      }

      // 6.2 ps cursor
      if (OB_SUCC(ret) && session->get_cursor_cache().is_inited()) {
        if (OB_FAIL(session->get_cursor_cache().close_all(*session))) {
          LOG_WARN("failed to close all cursor", K(ret));
        } else {
          session->get_cursor_cache().reset();
        }
      }

      // 6.3 piece
      if (OB_SUCC(ret) && NULL != session->get_piece_cache()) {
        observer::ObPieceCache* piece_cache =
          static_cast<observer::ObPieceCache*>(session->get_piece_cache());
        if (OB_FAIL(piece_cache->close_all(*session))) {
          LOG_WARN("failed to close all piece", K(ret));
        }
        piece_cache->reset();
        session->get_session_allocator().free(session->get_piece_cache());
        session->set_piece_cache(NULL);
      }

      if (OB_SUCC(ret)) {
        // 6.4 ps session info
        session->reset_ps_session_info();

        // 6.5 ps name
        session->reset_ps_name();
      }
    }

    // 8. Resets the value of LAST_INSERT_ID() to 0.
    if (OB_SUCC(ret)) {
      ObObj last_insert_id;
      last_insert_id.set_uint64(0);
      // FIXME @qianfu 暂时写成update_sys_variable函数，以后再实现一个可以传入ObSysVarClassType并且
      // 和set系统变量语句走同一套逻辑的函数，在这里调用
      if (OB_FAIL(session->update_sys_variable(SYS_VAR_LAST_INSERT_ID, last_insert_id))) {
        LOG_WARN("fail to update last_insert_id", K(ret));
      } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_IDENTITY, last_insert_id))) {
        LOG_WARN("succ update last_insert_id, but fail to update identity", K(ret));
      } else {
        NG_TRACE_EXT(last_insert_id, OB_ID(last_insert_id), 0);
      }
    }

    // 9. Releases locks acquired with GET_LOCK().
    if (OB_SUCC(ret)) {
      ObTableLockOwnerID raw_owner_id;
      if (OB_FAIL(raw_owner_id.convert_from_client_sessid(session->get_client_sessid(), session->get_client_create_time()))) {
        LOG_WARN("failed to convert from client sessid", K(ret));
      } else if (OB_FAIL(ObTableLockDetector::remove_lock_by_owner_id(raw_owner_id.raw_value()))) {
        LOG_WARN("failed to remove lock by owner id", K(ret));
      }
    }

    // 10. OB unique design
    if (OB_SUCC(ret)) {
      // 10.1 pl debug 功能, pl debug不支持分布式调试，但调用也不会有副作用
#ifdef OB_BUILD_ORACLE_PL
      session->reset_pl_debugger_resource();
      session->reset_pl_profiler_resource();
#endif

      // 10.2 非分布式需要的话，分布式也需要，用于清理package的全局变量值
      session->reset_all_package_state();

      // 10.3 currval 清理
      session->reuse_all_sequence_value();

      // reuse_context_map 没有使用 malloc free 内存，会导致内存泄漏，
      // mem_context_ 清理时会产生 unfree error 日志
      //session->reuse_context_map();

      // 10.5 warning buf
      session->reset_warnings_buf();
      session->reset_show_warnings_buf();

      // 10.6 client identifier
      session->get_client_identifier_for_update().reset();

      // 10.7 session label
      session->reuse_labels();

      // 10.8 clean mem context for context (dbms_session.create_context)
      session->destory_mem_context();
    }


    if (OB_SUCC(ret)) {
      session->clean_status();
    }
  }

  //send packet to client
  if (OB_SUCC(ret)) {
    ObOKPParam ok_param;
    //update_last_pkt_pos();
    ok_param.affected_rows_ = 0;
    ok_param.is_partition_hit_ = session->partition_hit().get_bool();
    ok_param.has_more_result_ = false;
    if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      OB_LOG(WARN, "response ok packet fail", K(ret));
    }
  } else {
    if (OB_FAIL(send_error_packet(ret, NULL))) {
      OB_LOG(WARN,"response fail packet fail", K(ret));
    }
    force_disconnect();
  }

  if (OB_UNLIKELY(need_disconnect) && is_conn_valid()) {
    if (NULL == session) {
      LOG_WARN("will disconnect connection", K(ret), K(need_disconnect));
    } else {
      LOG_WARN("will disconnect connection", K(ret), K(session->get_sessid()),
               K(need_disconnect));
    }
    force_disconnect();
  }

  THIS_WORKER.set_session(NULL);
  if (session != NULL) {
    revert_session(session);
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
