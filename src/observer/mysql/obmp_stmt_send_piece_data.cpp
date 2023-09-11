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

#include "observer/mysql/obmp_stmt_send_piece_data.h"

#include "lib/worker.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_session_stat.h"
#include "rpc/ob_request.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql.h"
#include "observer/ob_req_time_service.h"
#include "observer/omt/ob_tenant.h"
#include "observer/mysql/obsm_utils.h"
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

ObMPStmtSendPieceData::ObMPStmtSendPieceData(const ObGlobalContext &gctx)
    : ObMPBase(gctx),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      stmt_id_(0),
      param_id_(-1),
      buffer_len_(0),
      buffer_(),
      piece_mode_(ObInvalidPiece),
      is_null_(false)
{
  ctx_.exec_type_ = MpQuery;
}

/*
 * request packet:
 * 1  COM_STMT_SEND_PIECE_DATA
 * 4  stmt_id
 * 2  param_id
 * 1  piece_mode
 * 1  is_null
 * 8  buffer_length
 * n  data
 */
int ObMPStmtSendPieceData::before_process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMPBase::before_process())) {
    LOG_WARN("failed to pre processing packet", K(ret));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* pos = pkt.get_cdata();
    // stmt_id
    ObMySQLUtil::get_int4(pos, stmt_id_);
    ObMySQLUtil::get_uint2(pos, param_id_);
    ObMySQLUtil::get_int1(pos, piece_mode_);
    int8_t is_null = 0;
    ObMySQLUtil::get_int1(pos, is_null);
    1 == is_null ? is_null_ = true : is_null_ = false;
    ObMySQLUtil::get_int8(pos, buffer_len_);
    if (stmt_id_ < 1 || param_id_ < 0 || buffer_len_ < 0) {
      ret = OB_ERR_PARAM_INVALID;
      LOG_WARN("send long data get error info.", K(stmt_id_), K(param_id_), K(buffer_len_));
    } else {
      buffer_.assign_ptr(pos, static_cast<ObString::obstr_size_t>(buffer_len_));
      pos += buffer_len_;
      LOG_DEBUG("get info success in send long data protocol.", 
                  K(stmt_id_), K(param_id_));
    }
    LOG_DEBUG("send long data get param",K(stmt_id_), K(param_id_), 
              K(piece_mode_), K(buffer_len_), K(buffer_.length()));
  }
  return ret;
}

int ObMPStmtSendPieceData::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = NULL;
  bool need_response_error = true;
  bool async_resp_used = false; // 由事务提交线程异步回复客户端
  int64_t query_timeout = 0;
  ObSMConnection *conn = get_conn();

  if (OB_ISNULL(req_) || OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("req or conn is null", K_(req), K(conn), K(ret));
  } else if (OB_UNLIKELY(!conn->is_in_authed_phase())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("receive sql without session", K_(stmt_id), K_(param_id), K(ret));
  } else if (OB_ISNULL(conn->tenant_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", K_(stmt_id), K_(param_id), K(conn->tenant_), K(ret));
  } else if (OB_FAIL(get_session(sess))) {
    LOG_WARN("get session fail", K_(stmt_id), K_(param_id), K(ret));
  } else if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or invalid", K_(stmt_id), K_(param_id), K(sess), K(ret));
  } else if (OB_FAIL(update_transmission_checksum_flag(*sess))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    ObSQLSessionInfo &session = *sess;
    THIS_WORKER.set_session(sess);
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
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
      LOG_ERROR("invalid session", K_(stmt_id), K_(param_id), K(ret));
    } else if (OB_UNLIKELY(session.is_zombie())) {
      ret = OB_ERR_SESSION_INTERRUPTED;
      LOG_WARN("session has been killed", K(session.get_session_state()), K_(stmt_id), K_(param_id),
               K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(), K(ret));
    } else if (OB_UNLIKELY(packet_len > session.get_max_packet_size())) {
      ret = OB_ERR_NET_PACKET_TOO_LARGE;
      LOG_WARN("packet too large than allowd for the session", K_(stmt_id), K_(param_id), K(ret));
    } else if (OB_FAIL(session.get_query_timeout(query_timeout))) {
      LOG_WARN("fail to get query timeout", K_(stmt_id), K_(param_id), K(ret));
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
    } else {
      THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
      session.partition_hit().reset();
      if (OB_FAIL(process_send_long_data_stmt(session))) {
        LOG_WARN("execute sql failed", K_(stmt_id), K_(param_id), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      send_error_packet(ret, NULL);
      force_disconnect();
      LOG_WARN("disconnect connection when process query", K(ret));
    }

    session.set_last_trace_id(ObCurTraceId::get_trace_id());
    THIS_WORKER.set_session(NULL);
    revert_session(sess); //current ignore revert session ret
  }
  return ret;
}

int ObMPStmtSendPieceData::process_send_long_data_stmt(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  bool need_response_error = true;
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  setup_wb(session);

  ObVirtualTableIteratorFactory vt_iter_factory(*gctx_.vt_iter_creator_);
  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  const bool enable_trace_log = lib::is_trace_log_enabled();
  if (enable_trace_log) {
    ObThreadLogLevelUtils::init(session.get_log_id_level_map());
  }
  ret = do_process(session);
  if (enable_trace_log) {
    ObThreadLogLevelUtils::clear();
  }

  //对于tracelog的处理，不影响正常逻辑，错误码无须赋值给ret
  int tmp_ret = OB_SUCCESS;
  //清空WARNING BUFFER
  tmp_ret = do_after_process(session, ctx_, false);
  UNUSED(tmp_ret);
  return ret;
}

int ObMPStmtSendPieceData::do_process(ObSQLSessionInfo &session)
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
                                    ? &audit_record.exec_record_.max_wait_event_ : NULL, di);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
    if (enable_perf_event) {
      audit_record.exec_record_.record_start(di);
    }
    int64_t execution_id = 0;
    ObString sql = "send long data";
    //监控项统计开始
    exec_start_timestamp_ = ObTimeUtility::current_time();
    if (FALSE_IT(execution_id = gctx_.sql_engine_->get_execution_id())) {
      //nothing to do
    } else if (OB_FAIL(set_session_active(sql, session, ObTimeUtil::current_time(), 
                                          obmysql::ObMySQLCmd::COM_STMT_SEND_PIECE_DATA))) {
      LOG_WARN("fail to set session active", K(ret));
    } else if (OB_FAIL(store_piece(session))) {
      exec_end_timestamp_ = ObTimeUtility::current_time();
    } else {
      //监控项统计结束
      exec_end_timestamp_ = ObTimeUtility::current_time();

      session.set_current_execution_id(execution_id);

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
    audit_record.plan_id_ = param_id_;
    audit_record.return_rows_ = buffer_len_;
    audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();
    audit_record.ps_stmt_id_ = stmt_id_;
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
        LOG_WARN("get sql fail in send piece data", K(stmt_id_));
      }
    }
  }
  ObSQLUtils::handle_audit_record(false, EXECUTE_PS_SEND_PIECE, session, ctx_.is_sensitive_);

  clear_wb_content(session);
  return ret;
}

int ObMPStmtSendPieceData::store_piece(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObPieceCache *piece_cache = static_cast<ObPieceCache*>(session.get_piece_cache(true));
  if (OB_ISNULL(piece_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece cache is null.", K(ret), K(stmt_id_));
  } else {
    ObPiece *piece = NULL;
    if (OB_FAIL(piece_cache->get_piece(stmt_id_, param_id_, piece))) {
      LOG_WARN("get piece fail", K(stmt_id_), K(param_id_), K(ret) );
    } else if (NULL == piece) {
      if (OB_FAIL(piece_cache->make_piece(stmt_id_, param_id_, piece, session))) {
        LOG_WARN("make piece fail.", K(ret), K(stmt_id_));
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == piece) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece is null.", K(ret), K(stmt_id_), K(param_id_));
      } else if (OB_FAIL(piece_cache->add_piece_buffer(piece, 
                                                      piece_cache->get_piece_mode(piece_mode_), 
                                                      &buffer_))) {
        LOG_WARN("add piece buffer fail.", K(ret), K(stmt_id_));
      } else {
        if (is_null_) {
          OZ (piece->get_is_null_map().add_member(piece->get_position()));
        }
        ObOKPParam ok_param;
        ok_param.affected_rows_ = 0;
        ok_param.is_partition_hit_ = session.partition_hit().get_bool();
        ok_param.has_more_result_ = false;
        if (OB_SUCC(ret) && OB_FAIL(send_ok_packet(session, ok_param))) {
          LOG_WARN("send ok packet fail.", K(ret), K(stmt_id_));
        }
      }
    }
  }
  return ret;
}

int64_t ObPieceBuffer::to_string(char *buffer, int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buffer, len, pos,
                  "piece_mode:%d",
                  //"buf:%.*s",
                  mode_//, 
                  /*buffer_->length(), buffer_->ptr()*/);
  return pos;
}


int ObPiece::piece_init(ObSQLSessionInfo &session, 
                        int32_t stmt_id, 
                        uint16_t param_id) {
  int ret = OB_SUCCESS;
  set_stmt_id(stmt_id);
  set_param_id(param_id);
  lib::MemoryContext entity = NULL;
  lib::ContextParam param;
  param.set_mem_attr(session.get_effective_tenant_id(),
                      ObModIds::OB_PL_TEMP, ObCtxIds::DEFAULT_CTX_ID);
  param.set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);
  if (OB_FAIL((static_cast<ObPieceCache*>(session.get_piece_cache()))
                    ->mem_context_->CREATE_CONTEXT(entity, param))) {
    LOG_WARN("failed to create ref cursor entity", K(ret));
  } else if (OB_ISNULL(entity)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ref cursor entity", K(ret));
  } else {
    void *buf = NULL;
    ObPieceBufferArray *buf_array = NULL;
    ObIAllocator *alloc = &entity->get_arena_allocator();
    OV (OB_NOT_NULL(buf = alloc->alloc(sizeof(ObPieceBufferArray))),
        OB_ALLOCATE_MEMORY_FAILED, sizeof(ObPieceBufferArray));
    OX (MEMSET(buf, 0, sizeof(ObPieceBufferArray)));
    OV (OB_NOT_NULL(buf_array = new (buf) ObPieceBufferArray(alloc)));
    OZ (buf_array->reserve(OB_MAX_PIECE_COUNT));
    if (OB_SUCC(ret)) {
        set_allocator(alloc);
        set_buffer_array(buf_array);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc buffer array fail.", K(ret), K(stmt_id), K(param_id));
    }
  }
  LOG_DEBUG("piece init.", K(ret), K(stmt_id), K(param_id));
  // The failure is handed over to the upper layer to release the memory space
  return ret;
}

int ObPieceCache::init_piece_cache(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    if (OB_FAIL(init(session.get_effective_tenant_id()))) {
      LOG_WARN("piece_cache init fail", K(ret));
    }
  }
  LOG_DEBUG("init piece cache. ", K(session.get_effective_tenant_id()));
  return ret;
}

int ObPieceCache::make_piece(int32_t stmt_id, 
                             uint16_t param_id,
                             ObPiece *&piece, 
                             ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_piece_cache(session))) {
    LOG_WARN("piece_cache init fail", K(ret));
  } else if (NULL == mem_context_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece_cache mem_context_ is null", K(ret));
  } else {
    void *buf = NULL;
    OV (OB_NOT_NULL(buf = mem_context_->get_malloc_allocator().alloc(sizeof(ObPiece))),
        OB_ALLOCATE_MEMORY_FAILED, sizeof(ObPiece));
    OX (MEMSET(buf, 0, sizeof(ObPiece)));
    OV (OB_NOT_NULL(piece = new (buf) ObPiece()));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(piece->piece_init(session, stmt_id, param_id))) {
        LOG_WARN("piece init fail.", K(ret), K(stmt_id), K(param_id));
      } else if (OB_FAIL(add_piece(piece))) {
        LOG_WARN("add piece fail.", K(ret), K(stmt_id), K(param_id));
      }
      if (OB_SUCCESS != ret) {
        // clean up memory when failed.
        piece->~ObPiece();
        mem_context_->get_malloc_allocator().free(piece);
        piece = NULL;
      }
    }
  }
  LOG_DEBUG("make piece: ", K(ret), K(stmt_id), K(param_id), K(session.get_effective_tenant_id()));
  return ret;
}

int ObPieceCache::add_piece(ObPiece *piece)
{
  int ret = OB_SUCCESS;
  int64_t key = get_piece_key(piece->get_stmt_id(), piece->get_param_id());
  if (OB_INVALID_ID == key) {
    ret = OB_ERR_PARAM_INVALID;
    LOG_WARN("piece key is invalid.", K(ret), K(key));
  } else if (OB_FAIL(piece_map_.set_refactored(key, piece))) {
    LOG_WARN("fail insert ps id to hash map", K(key), K(ret));
  }
  LOG_DEBUG("add piece: ", K(ret), K(key),
            K(piece->get_stmt_id()), K(piece->get_param_id()));
  return ret;
}

int ObPieceCache::remove_piece(int64_t key, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObPiece *piece = NULL;
  if (OB_FAIL(piece_map_.erase_refactored(key, &piece))) {
    LOG_WARN("piece info not exist", K(key), K(ret));
  } else if (OB_ISNULL(piece)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else {
    close_piece(piece, session);
    LOG_DEBUG("remove piece success.", K(key));
  }
  return ret;
}

void ObPieceCache::close_piece(ObPiece *&piece, ObSQLSessionInfo &session)
{
  if (NULL != piece && NULL != mem_context_) {
    LOG_DEBUG("remove piece", K(piece->get_stmt_id()), 
                              K(piece->get_param_id()));
    piece->~ObPiece();
    mem_context_->get_malloc_allocator().free(piece);
    piece = NULL;
  }
}

int ObPieceCache::close_all(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    common::ObSEArray<int64_t, 32> piece_keys;
    for (PieceMap::iterator iter = piece_map_.begin();  //ignore ret
        iter != piece_map_.end();
        ++iter) {
      ObPiece *piece = iter->second;
      piece_keys.push_back(get_piece_key(piece->get_stmt_id(), piece->get_param_id()));
    }
    for (int64_t i = 0; i < piece_keys.count(); i++) {
      int64_t key = piece_keys.at(i);
      int64_t tmp_ret = remove_piece(key, session);
      // only save first error ret
      ret = ret == OB_SUCCESS ? tmp_ret : ret;
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("remove piece fail.", K(key), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObPieceCache::get_piece(int32_t stmt_id, uint16_t param_id, ObPiece *&piece)
{
  int ret = OB_SUCCESS;
  piece = NULL;
  if (!is_inited()) {
    LOG_DEBUG("piece_cache_ is not init.", K(stmt_id), K(param_id));
    // do nothing, do not init piece_cache_ here
  } else {
    if (OB_FAIL(piece_map_.get_refactored(
                            get_piece_key(stmt_id, param_id), piece))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get piece info failed", K(stmt_id), K(param_id));
      }
    }
  }
  return ret;
}

/*
 * The difference between piece_buffer in fetch and execute:
 * fetch: Character data: each piece has and only one piece_buffer. 
 *                        In this piece_buffer, 
 *                        multiple rows of data are distinguished by piece_mode.
 * execute: character data: with multiple piece_buffer
 */
// for fetch
int ObPieceCache::get_piece_buffer(int32_t stmt_id, 
                                   uint16_t param_id,
                                   int32_t offset,
                                   uint64_t piece_size, 
                                   ObPieceBuffer &piece_buf,
                                   ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObPieceBufferArray *buf_array = NULL;
  ObPieceBuffer *old_piece_buf = NULL;
  ObPiece *piece = NULL;
  if (OB_FAIL(get_piece(stmt_id, param_id, piece))) {
    LOG_WARN("get piece fail", K(stmt_id), K(param_id), K(ret) );
  } else if (NULL == piece) {
    ret = OB_ERR_PARAM_INVALID;
    LOG_WARN("piece is null", K(stmt_id), K(ret));
  } else if (NULL == piece->get_buffer_array()
              || 0 == piece->get_buffer_array()->count()) {
    // piecebuffer 空了的话， 就直接摘掉piece
    if (OB_FAIL(remove_piece(get_piece_key(stmt_id, param_id), session))) {
      LOG_WARN("remove piece fail", K(stmt_id), K(param_id));
    } else {
      // fetch阶段，前一段读到了最后一个数据，但是长度恰好和piecesize相等，所以没有设置last标记
      piece_buf.set_piece_mode(ObLastPiece);
      piece_buf.set_piece_buffer(NULL);
    }
  } else {
    buf_array = piece->get_buffer_array();
    if (0 == piece_size) {
      // data array
      ret = OB_NOT_SUPPORTED;
      LOG_WARN(" not support array type yet.", K(ret));
    } else if (offset < buf_array->count()) {
      // text
      old_piece_buf = &buf_array->at(offset);
      ObString *buf = old_piece_buf->get_piece_buffer();
      char *&pos = old_piece_buf->get_position();
      int64_t len = piece_size;
      // buf 需要根据piece_size截断
      if ((buf->length() - (pos - (buf->ptr()))) <= piece_size) {
        old_piece_buf->set_piece_mode(ObLastPiece);
        piece_buf.set_piece_mode(ObLastPiece);
        len = buf->length() - (pos - (buf->ptr()));
      } else if (ObInvalidPiece == old_piece_buf->get_piece_mode()) {
        old_piece_buf->set_piece_mode(ObFirstPiece);
        piece_buf.set_piece_mode(ObFirstPiece);
      } else if (ObFirstPiece == old_piece_buf->get_piece_mode()) {
        old_piece_buf->set_piece_mode(ObNextPiece);
        piece_buf.set_piece_mode(ObNextPiece);
      } else if (ObNextPiece == old_piece_buf->get_piece_mode()) { 
        piece_buf.set_piece_mode(ObNextPiece);
      }
      piece_buf.get_piece_buffer()->set_length(len);
      piece_buf.get_piece_buffer()->assign_ptr(pos, static_cast<ObString::obstr_size_t>(len));
      pos += len;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get last piece already.", K(ret), K(offset),
                K(buf_array->count()));
    }
  }
  LOG_DEBUG("get piece buffer.", K(ret), K(stmt_id), K(param_id), 
                                 K(piece_size), K(piece_buf.get_piece_mode()));
  return ret;
}

// for execute
// buf needs to allocate memory in the outer layer ！！！
int ObPieceCache::get_buffer(int32_t stmt_id, 
                             uint16_t param_id,
                             uint64_t count,
                             uint64_t &length, 
                             common::ObFixedArray<ObSqlString, ObIAllocator> &str_buf,
                             char *is_null_map) {
  int ret = lib::is_oracle_mode()
    ? get_oracle_buffer(stmt_id, param_id, count, length, str_buf, is_null_map)
    : get_mysql_buffer(stmt_id, param_id, length, str_buf.at(0));
  return ret;
}

int ObPieceCache::get_oracle_buffer(int32_t stmt_id, 
                                    uint16_t param_id,
                                    uint64_t count,
                                    uint64_t &length, 
                                    common::ObFixedArray<ObSqlString, ObIAllocator> &str_buf,
                                    char *is_null_map)
{
  int ret = OB_SUCCESS;
  ObPiece *piece = NULL;
  if (OB_FAIL(get_piece(stmt_id, param_id, piece))) {
    LOG_WARN("get piece fail", K(stmt_id), K(param_id), K(ret) );
  } else if (NULL == piece) {
    ret = OB_ERR_PARAM_INVALID;
    LOG_WARN("piece is null", K(stmt_id), K(ret));
  } else {
    // maybe because the retry mechanism repeatedly executes this piece of code, 
    // position should be set 0 
    piece->set_position(0);
    for (int64_t i=0; OB_SUCC(ret) && i < count; i++) {
      if (OB_FAIL(merge_piece_buffer(piece, str_buf.at(i)))) {
        LOG_WARN("merge piece buffer fail.", K(ret), K(stmt_id));
      } else {
        length = length + get_length_length(str_buf.at(i).length());
        length = length + str_buf.at(i).length();
      }
    }
    if (OB_SUCC(ret) && NULL != is_null_map) {
      piece->get_is_null_map(is_null_map, count);
    }
  }
  LOG_DEBUG("get buffer.", K(ret), K(stmt_id), K(param_id), K(length));
  return ret;
}

int ObPieceCache::get_mysql_buffer(int32_t stmt_id, 
                                  uint16_t param_id,
                                  uint64_t &length, 
                                  ObSqlString &str_buf)
{
  int ret = OB_SUCCESS;
  ObPiece *piece = NULL;
  length = 0;
  str_buf.reset();
  if (OB_FAIL(get_piece(stmt_id, param_id, piece))) {
    LOG_WARN("get piece fail", K(stmt_id), K(param_id), K(ret) );
  } else if (NULL == piece) {
    ret = OB_ERR_PARAM_INVALID;
    LOG_WARN("piece is null", K(stmt_id), K(ret));
  } else {
    ObPieceBufferArray *buffer_array = piece->get_buffer_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < buffer_array->count(); i++) {
      ObPieceBuffer *piece_buffer = &buffer_array->at(i);
      if (NULL != piece_buffer->get_piece_buffer()) {
        const ObString buffer = *(piece_buffer->get_piece_buffer());
        if (OB_FAIL(str_buf.append(buffer))) {
          LOG_WARN("append long data fail.", K(ret));
        } else {}
      }
    }
    length += get_length_length(str_buf.length());
    length += str_buf.length();
  }
  LOG_DEBUG("get buffer.", K(ret), K(stmt_id), K(param_id), K(length));
  return ret;
}

int ObPieceCache::make_piece_buffer(ObIAllocator *allocator,
                                    ObPieceBuffer *&piece_buffer, 
                                    ObPieceMode mode, 
                                    ObString *buf)
{
  // 这个buf是不是要deep copy一份，外层的生命周期不可控
  int ret = OB_SUCCESS;
  void *piece_mem = NULL;
  OV (OB_NOT_NULL(piece_mem = allocator->alloc(sizeof(ObPieceBuffer))),
      OB_ALLOCATE_MEMORY_FAILED, sizeof(ObPieceBuffer));
  OX (MEMSET(piece_mem, 0, sizeof(ObPieceBuffer)));
  OV (OB_NOT_NULL(piece_buffer = new (piece_mem) ObPieceBuffer(allocator, mode)));
  CK (OB_NOT_NULL(piece_buffer));
  OX (piece_buffer->set_piece_buffer(buf));
  LOG_DEBUG("make piece buffer.", K(ret), K(mode), K(buf->length()));
  return ret;
}

ObPieceMode ObPieceCache::get_piece_mode(int8_t mode)
{
  ObPieceMode piece_mode = ObInvalidPiece;
  switch (mode) {
    case 0:
      piece_mode = ObInvalidPiece;
      break;
    case 1:
      piece_mode = ObFirstPiece;
      break;
    case 2:
      piece_mode = ObNextPiece;
      break;
    case 3:
      piece_mode = ObLastPiece;
      break;
    default:
      piece_mode = ObInvalidPiece;
  }
  return piece_mode;
}

int ObPieceCache::add_piece_buffer(ObPiece *piece,
                                   ObPieceMode piece_mode,
                                   ObString *buf)
{
  int ret = OB_SUCCESS;
  ObPieceBuffer *piece_buffer = NULL;

  // 这里直接push进去了，piece_buffer的生命周期是不是也要考虑
  // 实现应该改一下，不要再传piece_buffer了，传buf和piece_mode
  // 内部调用make_piece_buffer分配内存
  if (OB_ISNULL(piece) || OB_ISNULL(piece->get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece is null.", K(ret));
  } else if (OB_FAIL(make_piece_buffer(piece->get_allocator(), 
                                        piece_buffer, 
                                        piece_mode,
                                        buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece or piece_buffer is null when add piece buffer", 
              K(ret), K(piece), K(piece_buffer));
  } else if (NULL == piece->get_buffer_array()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer array is null.", K(ret), K(piece->get_stmt_id()), K(piece->get_param_id()));
  } else { /* do nothing */ }
  if (OB_SUCC(ret) && OB_NOT_NULL(piece->get_buffer_array())) {
    ObPieceBufferArray *buffer_array = piece->get_buffer_array();
    if (OB_FAIL(buffer_array->push_back(*piece_buffer))) {
      LOG_WARN("push buffer array fail.", K(ret));
    } else if (lib::is_oracle_mode()) {
      // 1. pos ++
      if (ObInvalidPiece != piece_mode) {
        // fetch do not need 
        piece->add_position();
      }
      // 2. if is last piece, set position = 0, use for new row piece
      if (ObLastPiece == piece_mode) {
        piece->set_position(0);
      }
    } else { /* mysql do nothing */ }
  }
  LOG_DEBUG("add piece buffer.", K(ret), K(piece_mode));
  return ret;
}

static int pre_extend_str(ObPiece *piece,
                           ObSqlString &str,
                           ObPieceBufferArray *buffer_array,
                           const int32_t first_piece_size,
                           const int64_t array_size,
                           bool &is_enable)
{
  int ret = OB_SUCCESS;
  // may need extend str before append if piece size more then 4M
  const int32_t pre_extend_thres = 4194304;
  if (!is_enable) {
  } else if (first_piece_size < pre_extend_thres) {
    // just disable pre extend
    is_enable = false;
  } else {
    int64_t index_pre = piece->get_position() - 1;
    int64_t total_len = 0;
    do {
      index_pre++;
      if (index_pre < 0 || index_pre >= array_size) {
        break;
      }
      ObPieceBuffer *piece_buffer = &buffer_array->at(index_pre);
      if (NULL != piece_buffer->get_piece_buffer()) {
        const ObString buffer = *(piece_buffer->get_piece_buffer());
        total_len += buffer.length();
      }
    } while (ObLastPiece != buffer_array->at(index_pre).get_piece_mode()
             && ObInvalidPiece != buffer_array->at(index_pre).get_piece_mode());
    ret = str.extend(total_len + 1); // one more bytes for EOF
    is_enable = false;
  }
  return ret;
}

// buf needs to allocate memory in the outer layer ！！！
int ObPieceCache::merge_piece_buffer(ObPiece *piece,
                                     ObSqlString &str)
{
  int ret = OB_SUCCESS;
  ObPieceBufferArray *buffer_array = piece->get_buffer_array();
  if (NULL == buffer_array || 0 == buffer_array->count()) {
    ret = OB_ERR_PARAM_INVALID;
    LOG_WARN("buffer array is null.", K(ret), 
                                      K(piece->get_stmt_id()), 
                                      K(piece->get_param_id()), 
                                      K(buffer_array));
  } else {
    int64_t array_size = buffer_array->count();
    int64_t index = piece->get_position() - 1;
    int64_t len = 0;
    bool enable_pre_extern = true;
    do {
      index++;
      if (index < 0 || index >= array_size) {
        break;
      }
      ObPieceBuffer *piece_buffer = &buffer_array->at(index);
      if (NULL != piece_buffer->get_piece_buffer()) {
        const ObString buffer = *(piece_buffer->get_piece_buffer());
        // reduce alloc/free/memcpy for large buffers
        OZ (pre_extend_str(piece, str, buffer_array, buffer.length(), array_size, enable_pre_extern));
        OX (str.append(buffer));
        OX (len += buffer.length());
      }
    } while (ObLastPiece != buffer_array->at(index).get_piece_mode()
              && ObInvalidPiece != buffer_array->at(index).get_piece_mode()
              && OB_SUCC(ret));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (index < 0) {
      piece->set_position(0);
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error index.", K(array_size), K(index));
    } else {
      piece->set_position(index+1);
    }
    
    if (OB_FAIL(ret)) {
      //do nothing.
    } else if (str.length() != len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("total length is not match total piece length.", K(ret), 
                                                                K(str.length()),
                                                                K(len));
    }
  }
  return ret;
}

} //end of namespace observer
} //end of namespace oceanbase
