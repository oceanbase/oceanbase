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

#include "obmp_stmt_get_piece_data.h"

#include "lib/worker.h"
#include "lib/oblog/ob_log.h"
#include "sql/ob_sql_context.h"
#include "lib/stat/ob_session_stat.h"
#include "rpc/ob_request.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql.h"
#include "observer/ob_req_time_service.h"
#include "observer/mysql/obmp_utils.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"
#include "rpc/obmysql/packet/ompk_piece.h"
#include "observer/omt/ob_tenant.h"
#include "sql/plan_cache/ob_ps_cache.h"

namespace oceanbase
{

using namespace rpc;
using namespace common;
using namespace share;
using namespace obmysql;
using namespace sql;

namespace observer
{

ObMPStmtGetPieceData::ObMPStmtGetPieceData(const ObGlobalContext &gctx)
    : ObMPBase(gctx),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      stmt_id_(0),
      column_id_(-1),
      offset_(0),
      piece_size_(0)
{
  ctx_.exec_type_ = MpQuery;
}

/*
 * request packet:
 * 1  COM_STMT_GET_PIECE_data
 * 4  stmt_id
 * 2  offset
 * 4  offset_type
 * 4  column_id
 * 4  piece_size
 */
int ObMPStmtGetPieceData::before_process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMPBase::before_process())) {
    LOG_WARN("failed to pre processing packet", K(ret));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* pos = pkt.get_cdata();
    // stmt_id
    ObMySQLUtil::get_int4(pos, stmt_id_);
    int16_t offset_type = 0;
    ObMySQLUtil::get_int2(pos, offset_type);
    ObMySQLUtil::get_int4(pos, offset_);
    ObMySQLUtil::get_int2(pos, column_id_);
    ObMySQLUtil::get_int8(pos, piece_size_);
    LOG_DEBUG("get piece data", K(stmt_id_), K(column_id_), K(piece_size_));
  }
  return ret;
}

int ObMPStmtGetPieceData::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = NULL;
  bool need_response_error = true;
  bool need_disconnect = true;
  bool async_resp_used = false; // 由事务提交线程异步回复客户端
  int64_t query_timeout = 0;
  ObSMConnection *conn = get_conn();

  if (OB_ISNULL(req_) || OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("req or conn is null", K_(req), K(stmt_id_), K(conn), K(ret));
  } else if (OB_UNLIKELY(!conn->is_in_authed_phase())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("receive sql without session", K(ret), K(stmt_id_));
  } else if (OB_ISNULL(conn->tenant_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", K(conn->tenant_), K(ret), K(stmt_id_));
  } else if (OB_FAIL(get_session(sess))) {
    LOG_WARN("get session fail", K(ret), K(stmt_id_));
  } else if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or invalid", K(sess), K(ret), K(stmt_id_));
  } else if (OB_FAIL(update_transmission_checksum_flag(*sess))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret), K(stmt_id_));
  } else {
    ObSQLSessionInfo &session = *sess;
    THIS_WORKER.set_session(sess);
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
    session.init_use_rich_format();
    session.get_raw_audit_record().request_memory_used_ = 0;
    observer::ObProcessMallocCallback pmcb(0,
          session.get_raw_audit_record().request_memory_used_);
    lib::ObMallocCallbackGuard guard(pmcb);
    int64_t tenant_version = 0;
    int64_t sys_version = 0;
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    int64_t packet_len = pkt.get_clen();
    if (OB_UNLIKELY(!session.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid session", K_(stmt_id), K(ret));
    } else if (OB_FAIL(process_kill_client_session(session))) {
      LOG_WARN("client session has been killed", K(ret));
    } else if (OB_UNLIKELY(session.is_zombie())) {
      ret = OB_ERR_SESSION_INTERRUPTED;
      LOG_WARN("session has been killed", K(session.get_session_state()), K_(stmt_id),
               K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(), K(ret));
    } else if (OB_UNLIKELY(packet_len > session.get_max_packet_size())) {
      ret = OB_ERR_NET_PACKET_TOO_LARGE;
      LOG_WARN("packet too large than allowd for the session", K_(stmt_id), K(ret));
    } else if (OB_FAIL(session.get_query_timeout(query_timeout))) {
      LOG_WARN("fail to get query timeout", K_(stmt_id), K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                session.get_effective_tenant_id(), tenant_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                OB_SYS_TENANT_ID, sys_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (pkt.exist_trace_info()
               && OB_FAIL(session.update_sys_variable(SYS_VAR_OB_TRACE_INFO,
                                                      pkt.get_trace_info()))) {
      LOG_WARN("fail to update trace info", K(ret));
    } else if (FALSE_IT(session.set_txn_free_route(pkt.txn_free_route()))) {
    } else if (OB_FAIL(process_extra_info(session, pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else if (FALSE_IT(session.post_sync_session_info())) {
    } else {
      need_disconnect = false;
      THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
      session.partition_hit().reset();
      if (OB_FAIL(process_get_piece_data_stmt(session))) {
        LOG_WARN("execute sql failed", K_(stmt_id), K(ret));
      }
    }

    session.set_last_trace_id(ObCurTraceId::get_trace_id());
    THIS_WORKER.set_session(NULL);
    revert_session(sess); //current ignore revert session ret
  }

  if (OB_FAIL(ret) && need_response_error && is_conn_valid()) {
    send_error_packet(ret, NULL);
  }

  if (OB_FAIL(ret) && need_disconnect && is_conn_valid()) {
    force_disconnect();
    LOG_WARN("disconnect connection when process query", K(ret));
  }
  return ret;
}

int ObMPStmtGetPieceData::process_get_piece_data_stmt(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  bool need_response_error = true;
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  setup_wb(session);

  ObVirtualTableIteratorFactory vt_iter_factory(*gctx_.vt_iter_creator_);
  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  ObThreadLogLevelUtils::init(session.get_log_id_level_map());
  ret = do_process(session);
  ObThreadLogLevelUtils::clear();

  //对于tracelog的处理，不影响正常逻辑，错误码无须赋值给ret
  int tmp_ret = OB_SUCCESS;
  //清空WARNING BUFFER
  tmp_ret = do_after_process(session, ctx_, false);
  UNUSED(tmp_ret);
  return ret;
}

int ObMPStmtGetPieceData::do_process(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObAuditRecordData &audit_record = session.get_raw_audit_record();
  audit_record.try_cnt_++;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit = GCONF.enable_sql_audit
                                && session.get_local_ob_enable_sql_audit();
  single_process_timestamp_ = ObTimeUtility::current_time();
  bool is_diagnostics_stmt = false;

  ObWaitEventStat total_wait_desc;
  ObDiagnoseSessionInfo *di = ObDiagnoseSessionInfo::get_local_diagnose_info();
  {
    ObMaxWaitGuard max_wait_guard(enable_perf_event 
                                    ? &audit_record.exec_record_.max_wait_event_ 
                                    : NULL, di);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
    if (enable_perf_event) {
      audit_record.exec_record_.record_start(di);
    }
    int64_t execution_id = 0;
    ObString sql = "get piece info";
    //监控项统计开始
    exec_start_timestamp_ = ObTimeUtility::current_time();
    if (FALSE_IT(execution_id = gctx_.sql_engine_->get_execution_id())) {
      //nothing to do
    } else if (OB_FAIL(set_session_active(sql, session, ObTimeUtil::current_time(), 
                                          obmysql::ObMySQLCmd::COM_STMT_GET_PIECE_DATA))) {
      LOG_WARN("fail to set session active", K(ret));
    } else if (OB_FAIL(response_result(session))) {
      exec_end_timestamp_ = ObTimeUtility::current_time();
    } else {
      session.set_current_execution_id(execution_id);

      //监控项统计结束
      exec_end_timestamp_ = ObTimeUtility::current_time();

      // some statistics must be recorded for plan stat, even though sql audit disabled
      bool first_record = (1 == audit_record.try_cnt_);
      ObExecStatUtils::record_exec_timestamp(*this, first_record, audit_record.exec_timestamp_);
      audit_record.exec_timestamp_.update_stage_time();

      if (enable_perf_event) {
        audit_record.exec_record_.record_end(di);
        audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
        audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
        audit_record.update_event_stage_state();
        const int64_t time_cost = exec_end_timestamp_ - get_receive_timestamp();
        EVENT_INC(SQL_PS_PREPARE_COUNT);
        EVENT_ADD(SQL_PS_PREPARE_TIME, time_cost);
      }
    }
  } // diagnose end

  // store the warning message from the most recent statement in the current session
  if (OB_SUCC(ret) && is_diagnostics_stmt) {
    // if diagnostic stmt execute successfully, it dosen't clear the warning message
    session.update_show_warnings_buf();
  } else {
    session.set_show_warnings_buf(ret); // TODO: 挪个地方性能会更好，减少部分wb拷贝
  }

  //set read_only
  if (OB_FAIL(ret)) {
    bool is_partition_hit = session.partition_hit().get_bool();
    int err = send_error_packet(ret, NULL, is_partition_hit);
    if (OB_SUCCESS != err) {  // 发送error包
      LOG_WARN("send error packet failed", K(ret), K(err));
    }
  }
  if (enable_sql_audit) {
    audit_record.status_ = ret;
    audit_record.client_addr_ = session.get_peer_addr();
    audit_record.user_client_addr_ = session.get_user_client_addr();
    audit_record.user_group_ = THIS_WORKER.get_group_id();
    audit_record.ps_stmt_id_ = stmt_id_;
    audit_record.plan_id_ = column_id_;
    audit_record.return_rows_ = piece_size_;
    audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();
    if (OB_NOT_NULL(session.get_ps_cache())) {
      ObPsStmtInfoGuard guard;
      ObPsStmtInfo *ps_info = NULL;
      ObPsStmtId inner_stmt_id = OB_INVALID_ID;
      if (OB_SUCC(session.get_inner_ps_stmt_id(stmt_id_, inner_stmt_id))
            && OB_SUCC(session.get_ps_cache()->get_stmt_info_guard(inner_stmt_id, guard))
            && OB_NOT_NULL(ps_info = guard.get_stmt_info())) {
        audit_record.ps_inner_stmt_id_ = inner_stmt_id;
        audit_record.sql_ = const_cast<char *>(ps_info->get_ps_sql().ptr());
        audit_record.sql_len_ = min(ps_info->get_ps_sql().length(), OB_MAX_SQL_LENGTH);
      } else {
        LOG_WARN("get sql fail in get piece data", K(stmt_id_));
      }
    }
  }
  ObSQLUtils::handle_audit_record(false, EXECUTE_PS_GET_PIECE, session, ctx_.is_sensitive_);

  clear_wb_content(session);
  return ret;
}

int ObMPStmtGetPieceData::response_result(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObPieceBuffer piece_buf;
  ObPieceCache *piece_cache = session.get_piece_cache();
  if (OB_ISNULL(piece_cache)) {
    // must be init in fetch
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece cache is null.", K(ret), K(stmt_id_), K(column_id_));
  } else if (OB_FAIL(piece_cache->get_piece_buffer(stmt_id_, 
                                                  column_id_, 
                                                  offset_, 
                                                  piece_size_, 
                                                  piece_buf,
                                                  session))) {
    LOG_WARN("get piece buffer fail.", K(ret), K(stmt_id_));
  } else if (NULL == piece_buf.get_piece_buffer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(" piece buffer is null. ", K(ret));
  } else {
    // response piece packet
    OMPKPiece piece_packet(piece_buf.get_piece_mode(),
                           piece_buf.is_null(),
                           piece_buf.get_piece_buffer()->length(),
                           *piece_buf.get_piece_buffer());
    if (OB_FAIL(response_packet(piece_packet, &session))) {
      LOG_WARN("response piece packet fail.", K(ret), K(stmt_id_), K(column_id_));
    } else {
      ObPiece *piece = NULL;
      if (OB_FAIL(update_last_pkt_pos())) {
        LOG_WARN("failed to update last packet pos", K(ret));
      } else if (OB_FAIL(piece_cache->get_piece(stmt_id_, column_id_, piece))) {
        LOG_WARN("get piece fail", K(stmt_id_), K(column_id_), K(ret) );
      } else if (NULL != piece) {
        uint64_t count = NULL == piece->get_buffer_array() 
                          ? 0 
                          : piece->get_buffer_array()->count();
        if (0 != count && offset_ == count - 1 && ObLastPiece == piece_buf.get_piece_mode()) {
          // 证明发送完全部数据
          if (OB_FAIL(piece_cache->remove_piece(piece_cache->get_piece_key(stmt_id_, column_id_), 
                                                session))) {
            LOG_WARN("remove piece fail", K(stmt_id_), K(column_id_));
          }
        }
      }
      // for obproxy
      if (OB_SUCC(ret)) {
        // in multi-stmt, send extra ok packet in the last stmt(has no more result)
        if (need_send_extra_ok_packet()) {
          ObOKPParam ok_param;
          ok_param.affected_rows_ = 0;
          ok_param.is_partition_hit_ = session.partition_hit().get_bool();
          ok_param.has_more_result_ = false;
          if (OB_FAIL(send_ok_packet(session, ok_param))) {
            LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

} //end of namespace observer
} //end of namespace oceanbase
