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

#include "obmp_base.h"

#include "share/ob_worker.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/packet/ompk_change_user.h"
#include "rpc/obmysql/packet/ompk_error.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/client_feedback/ob_feedback_partition_struct.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/mysql/obmp_utils.h"
#include "observer/mysql/obsm_struct.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "share/config/ob_server_config.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
using namespace share;
using namespace rpc;
using namespace sql;
using namespace obmysql;
using namespace common;
using namespace transaction;
using namespace share::schema;
namespace observer {

void ObOKPParam::reset()
{
  affected_rows_ = 0;
  message_ = NULL;
  is_on_connect_ = false;
  is_partition_hit_ = true;
  has_more_result_ = false;
  take_trace_id_to_client_ = false;
  warnings_count_ = 0;
  lii_ = 0;
  reroute_info_ = NULL;
}

int64_t ObOKPParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(KT_(affected_rows),
      K_(is_on_connect),
      K_(is_partition_hit),
      K_(take_trace_id_to_client),
      K_(has_more_result),
      K_(lii));
  return pos;
}

ObMPPacketSender::ObMPPacketSender()
    : req_(NULL),
      sess_info_(NULL),
      seq_(0),
      comp_context_(),
      proto20_context_(),
      ez_buf_(NULL),
      conn_valid_(true),
      sessid_(0),
      req_has_wokenup_(true),
      query_receive_ts_(0),
      io_thread_mark_(false)
{}

ObMPPacketSender::~ObMPPacketSender()
{}

int ObMPPacketSender::init(rpc::ObRequest* req, sql::ObSQLSessionInfo* sess_info, uint8_t packet_seq, bool conn_status,
    bool req_has_wokenup, int64_t query_receive_ts, bool io_thread_mark)
{
  int ret = OB_SUCCESS;
  req_ = req;
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(req) || OB_ISNULL(sess_info) || OB_ISNULL(conn)) {
    ret = OB_ERR_NULL_VALUE;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(req), K(conn_status), K(sess_info), K(packet_seq), K(conn));
  } else {
    // memory for ok eof error packet
    sess_info_ = sess_info;
    seq_ = packet_seq;
    conn_valid_ = conn_status;
    req_has_wokenup_ = req_has_wokenup;
    query_receive_ts_ = query_receive_ts;
    io_thread_mark_ = io_thread_mark;

    sessid_ = conn->sessid_;
    // init comp_context
    comp_context_.reset();
    comp_context_.type_ = conn->get_compress_type();
    comp_context_.seq_ = seq_;
    comp_context_.sessid_ = sessid_;

    // init proto20 context
    bool is_proto20_supported = (OB_2_0_CS_TYPE == conn->get_cs_protocol_type());
    if (is_proto20_supported) {
      proto20_context_.reset();
      proto20_context_.is_proto20_used_ = is_proto20_supported;
      proto20_context_.comp_seq_ = seq_;
      proto20_context_.request_id_ = conn->proto20_pkt_context_.proto20_last_request_id_;
      proto20_context_.proto20_seq_ = static_cast<uint8_t>(conn->proto20_pkt_context_.proto20_last_pkt_seq_ + 1);
      proto20_context_.header_len_ = OB20_PROTOCOL_HEADER_LENGTH + OB_MYSQL_COMPRESSED_HEADER_SIZE;
      proto20_context_.tailer_len_ = OB20_PROTOCOL_TAILER_LENGTH;
      proto20_context_.next_step_ = START_TO_FILL_STEP;
      proto20_context_.is_checksum_off_ = false;
    }

    if (conn->proxy_cap_flags_.is_checksum_swittch_support()) {
      bool is_enable_checksum = false;
      if (OB_FAIL(sess_info->is_use_transmission_checksum(is_enable_checksum))) {
        if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret || OB_SYS_VARS_MAYBE_DIFF_VERSION == ret) {
          LOG_INFO("system variable is_enable_transmission maybe from diff version");
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("is use transmission checksum failed", K(ret));
        }
      } else {
        comp_context_.is_checksum_off_ = !is_enable_checksum;
        proto20_context_.is_checksum_off_ = !is_enable_checksum;
      }
    }
  }
  return ret;
}

int ObMPPacketSender::alloc_ezbuf()
{
  int ret = OB_SUCCESS;
  easy_request_t* ez_req = nullptr;
  char* buf = nullptr;
  if (OB_ISNULL(ez_buf_)) {
    if (OB_ISNULL(req_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("request or data buffer is null", KP(req_), K(ret));
    } else if (OB_ISNULL(ez_req = req_->get_request())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("easy request is null", K(ret));
    } else if (OB_ISNULL(buf = req_->easy_alloc(OB_MULTI_RESPONSE_BUF_SIZE + sizeof(easy_buf_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to easy_alloc", "size", OB_MULTI_RESPONSE_BUF_SIZE + sizeof(easy_buf_t), K(ret));
    } else {
      ez_buf_ = reinterpret_cast<easy_buf_t*>(buf);
      init_easy_buf(ez_buf_, reinterpret_cast<char*>(ez_buf_ + 1), ez_req, OB_MULTI_RESPONSE_BUF_SIZE);
    }
  }
  return ret;
}

int ObMPPacketSender::response_packet(obmysql::ObMySQLPacket& pkt)
{
  int ret = OB_SUCCESS;
  if (!conn_valid_) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection already disconnected", K(ret));
  } else if (OB_FAIL(alloc_ezbuf())) {
    LOG_ERROR("easy buffer alloc failed", K(ret));
  } else {
    int64_t seri_size = 0;
    pkt.set_seq(seq_);
    if (OB_FAIL(try_encode_with(pkt, ez_buf_->end - ez_buf_->pos, seri_size, 0))) {
      LOG_WARN("failed to encode packet", K(ret));
    } else {
      LOG_DEBUG("succ encode packet", K(pkt), K(seri_size));
      seq_ = pkt.get_seq();  // here will point next avail seq
      EVENT_INC(MYSQL_PACKET_OUT);
      EVENT_ADD(MYSQL_PACKET_OUT_BYTES, seri_size);
    }
  }
  return ret;
}

int ObMPPacketSender::send_error_packet(
    int err, const char* errmsg, bool is_partition_hit /* = true */, void* extra_err_info /* = NULL */)
{
  int ret = OB_SUCCESS;
  BACKTRACE(ERROR, (OB_SUCCESS == err), "BUG send error packet but err code is 0");
  LOG_INFO("sending error packet", K(err), "bt", lbt(), K(extra_err_info));
  ObSMConnection* conn = NULL;
  if (OB_SUCCESS == err || (OB_ERR_PROXY_REROUTE == err && OB_ISNULL(extra_err_info))) {
    // @BUG work around
    ret = OB_ERR_UNEXPECTED;
  } else if (!conn_valid_) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection already disconnected", K(ret));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get connection fail", K(conn), K(ret));
  } else if (OB_FAIL(alloc_ezbuf())) {
    LOG_WARN("ez_buf_ alloc failed", K_(ez_buf), K(ret));
  } else {
    // error message
    ObString message;
    const int32_t MAX_MSG_BUF_SIZE = 512;
    char msg_buf[MAX_MSG_BUF_SIZE];
    const ObWarningBuffer* wb = common::ob_get_tsi_warning_buffer();
    if (NULL != wb && (wb->get_err_code() == err ||
                          (err >= OB_MIN_RAISE_APPLICATION_ERROR && err <= OB_MAX_RAISE_APPLICATION_ERROR))) {
      message = wb->get_err_msg();
    }
    if (message.length() <= 0) {
      if (OB_LIKELY(NULL != errmsg) && 0 < strlen(errmsg)) {
        message = ObString::make_string(errmsg);
      } else if (err >= OB_MIN_RAISE_APPLICATION_ERROR && err <= OB_MAX_RAISE_APPLICATION_ERROR) {
        // do nothing ...
      } else {
        snprintf(msg_buf, MAX_MSG_BUF_SIZE, "%s", ob_errpkt_strerror(err, lib::is_oracle_mode()));
        message = ObString::make_string(msg_buf);  // default error message
      }
    }

    if (ObServerConfig::get_instance().enable_rich_error_msg) {

      int32_t msg_buf_size = 0;
      const uint64_t* trace_id = ObCurTraceId::get();
      uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
      uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];
      const ObAddr addr = ObCurTraceId::get_addr();

      struct timeval tv;
      struct tm tm;
      char addr_buf[32];
      (void)gettimeofday(&tv, NULL);
      ::localtime_r((const time_t*)&tv.tv_sec, &tm);
      addr.ip_port_to_string(addr_buf, 32);

      char tmp_msg_buf[MAX_MSG_BUF_SIZE];
      strncpy(tmp_msg_buf, message.ptr(), message.length());  // msg_buf is overwriten
      msg_buf_size = snprintf(msg_buf,
          MAX_MSG_BUF_SIZE,
          "%.*s\n"
          "[%s] "
          "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
          "[" TRACE_ID_FORMAT "]",
          message.length(),
          tmp_msg_buf,
          addr_buf,
          tm.tm_year + 1900,
          tm.tm_mon + 1,
          tm.tm_mday,
          tm.tm_hour,
          tm.tm_min,
          tm.tm_sec,
          tv.tv_usec,
          trace_id_0,
          trace_id_1);
      (void)msg_buf_size;                        // make compiler happy
      message = ObString::make_string(msg_buf);  // default error message
    }

    OMPKError epacket;
    // TODO Negotiate a err for rerouting sql
    epacket.set_errcode(static_cast<uint16_t>(ob_errpkt_errno(err, lib::is_oracle_mode())));
    ret = epacket.set_sqlstate(ob_sqlstate(err));
    if (OB_SUCC(ret) && OB_SUCC(epacket.set_message(message))) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
      if (OB_FAIL(response_packet(epacket))) {
        RPC_LOG(WARN, "failed to send error packet", K(epacket), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "failed to set error info", K(err), K(errmsg), K(ret));
    }
  }

  // for obproxy or OCJ, followed by another OK packet
  if (conn_valid_) {
    if (OB_FAIL(ret)) {
      disconnect();
    } else if (conn->need_send_extra_ok_packet()) {
      if (conn->is_in_authed_phase()) {
        sql::ObSQLSessionInfo* session = NULL;
        if (OB_FAIL(get_session(session))) {
          LOG_WARN("fail to get session", K(ret));
        } else if (OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql session info is null", K(ret));
        } else {
          ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
          ObOKPParam ok_param;
          if (session->is_track_session_info()) {
            ok_param.is_partition_hit_ = is_partition_hit;
            ok_param.take_trace_id_to_client_ = true;
            if (OB_ERR_PROXY_REROUTE == err) {
              ok_param.reroute_info_ = static_cast<ObFeedbackRerouteInfo*>(extra_err_info);
            }
          }
          if (OB_FAIL(send_ok_packet(*session, ok_param))) {
            LOG_WARN("failed to send ok packet", K(ok_param), K(ret));
          }
        }
        if (OB_LIKELY(NULL != session)) {
          revert_session(session);
        }
      } else {  // just a basic ok packet contain nothing
        OMPKOK okp;
        okp.set_capability(conn->cap_flags_);
        if (OB_FAIL(response_packet(okp))) {
          LOG_WARN("response ok packet fail", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append ok pkt after error pkt, need disconnect", K(ret));
        disconnect();
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObMPPacketSender::revert_session(ObSQLSessionInfo* sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_) || OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session mgr or session info is null", K_(GCTX.session_mgr), K(sess_info), K(ret));
  } else {
    if (OB_FAIL(GCTX.session_mgr_->revert_session(sess_info))) {
      LOG_ERROR("revert session fail",
          "sess_id",
          sess_info->get_sessid(),
          "proxy_sessid",
          sess_info->get_proxy_sessid(),
          K(ret));
    }
  }
  return ret;
}

int ObMPPacketSender::get_session(ObSQLSessionInfo*& sess_info)
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(conn) || OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn or sessoin mgr is NULL", K(ret), K(conn), K(GCTX.session_mgr_));
  } else if (OB_FAIL(GCTX.session_mgr_->get_session(conn->version_, conn->sessid_, sess_info))) {
    LOG_ERROR("get session fail",
        K(ret),
        "version",
        conn->version_,
        "sessid",
        conn->sessid_,
        "proxy_sessid",
        conn->proxy_sessid_);
  } else {
    NG_TRACE_EXT(session, OB_ID(sid), sess_info->get_sessid(), OB_ID(tenant_id), sess_info->get_priv_tenant_id());
  }
  return ret;
}

int ObMPPacketSender::send_ok_packet(ObSQLSessionInfo& session, ObOKPParam& ok_param)
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
  OMPKOK okp;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is null", K(ret));
  } else {
    okp.set_affected_rows(ok_param.affected_rows_);
    if (OB_UNLIKELY(NULL != ok_param.message_)) {
      ObString message_str;
      message_str.assign_ptr(ok_param.message_, static_cast<int32_t>(strlen(ok_param.message_)));
      okp.set_message(message_str);
    }

    okp.set_capability(session.get_capability());
    okp.set_last_insert_id(ok_param.lii_);
    okp.set_warnings(ok_param.warnings_count_);

    // set is_partition_hit
    if (!ok_param.is_partition_hit_) {
      ret = session.set_partition_hit(false);
    }

    if (OB_SUCC(ret)) {
      if (!ok_param.take_trace_id_to_client_) {
        const int64_t elapsed_time = ObTimeUtility::current_time() - query_receive_ts_;
        bool is_slow = (elapsed_time > GCONF.trace_log_slow_query_watermark);
        ok_param.take_trace_id_to_client_ = is_slow;
      }
      if (ok_param.take_trace_id_to_client_) {
        ObCurTraceId::TraceId* trace_id = ObCurTraceId::get_trace_id();
        if (NULL != trace_id) {
          if (OB_FAIL(session.update_last_trace_id(*trace_id))) {
            LOG_WARN("fail to update last trace id", KPC(trace_id), K(ret));
            ret = OB_SUCCESS;  // ignore ret
          }
        }
      }
    }
  }

  // set is_trans_specfied
  if (OB_SUCC(ret)) {
    ret = session.save_trans_status();
  }

  bool need_track_session_info = false;
  if (OB_SUCC(ret)) {
    if (session.is_track_session_info()) {
      // in hand shake response's ok packet, return all session variable
      if (ok_param.is_on_connect_) {
        need_track_session_info = true;
        if (conn->is_normal_client()) {
          okp.set_use_standard_serialize(true);
          if (OB_FAIL(ObMPUtils::add_nls_format(okp, session))) {
            LOG_WARN("fail to add_nls_format", K(ret));
          }
        } else {
          if (OB_FAIL(ObMPUtils::add_session_info_on_connect(okp, session))) {
            LOG_WARN("fail to add all session system variables", K(ret));
          }
        }
      } else {
        // in proxy or OCJ mode will track session info in the last ok packet
        if (!ok_param.has_more_result_) {
          need_track_session_info = true;
          // if safe weak read support, we should add safe weak read
          ObProxyCapabilityFlags client_cap(conn->proxy_cap_flags_);
          const int64_t snapshot_version = session.get_safe_weak_read_snapshot();
          if (client_cap.is_safe_weak_read_support() && snapshot_version > 0) {
            if (OB_FAIL(session.set_safe_weak_read_snapshot_variable(snapshot_version))) {
              SERVER_LOG(WARN, "fail set safe weak read snapshot ", K(ret));
            } else {
              SERVER_LOG(DEBUG, "succ set safe weak read snapshot", K(snapshot_version));
            }
          }
          // send changed session variables to obproxy
          if (OB_SUCC(ret)) {
            if (conn->is_normal_client()) {
              okp.set_use_standard_serialize(true);
              if (OB_FAIL(ObMPUtils::add_nls_format(okp, session, true))) {
                LOG_WARN("fail to add_nls_format", K(ret));
              }
            } else {
              if (OB_FAIL(ObMPUtils::add_changed_session_info(okp, session))) {
                SERVER_LOG(WARN, "fail to add changed session info", K(ret));
              } else if (ok_param.reroute_info_ != NULL &&
                         OB_FAIL(ObMPUtils::add_client_reroute_info(okp, session, *ok_param.reroute_info_))) {
                LOG_WARN("failed to add reroute info", K(ret));
              }
            }
          }
        }
      }
    } else {
      // use to compatible with older OCJ(< 2.0.9)
      if (ok_param.is_on_connect_ && conn->is_java_client_ && conn->proxy_cap_flags_.is_cap_used()) {
        okp.set_track_session_cap(true);
        if (OB_FAIL(ObMPUtils::add_session_info_on_connect(okp, session))) {
          LOG_WARN("fail to add all session system variables", K(ret));
        } else {
          need_track_session_info = true;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool ac = true;
    if (OB_FAIL(session.get_autocommit(ac))) {
      LOG_WARN("fail to get autocommit", K(ret));
    } else {
      ObServerStatusFlags flags = okp.get_server_status();
      flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = (session.is_server_status_in_transaction() ? 1 : 0);
      flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
      flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = ok_param.has_more_result_;
      if (!conn->is_proxy_) {
        // in java client or others, use slow query bit to indicate partition hit or not
        flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !ok_param.is_partition_hit_;
      }
      if (ok_param.is_on_connect_) {
        flags.status_flags_.OB_SERVER_STATUS_RESERVED_OR_ORACLE_MODE =
            (ORACLE_MODE == session.get_compatibility_mode() ? 1 : 0);
      }
      // todo: set OB_SERVER_STATUS_IN_TRANS_READONLY flag if need?
      okp.set_server_status(flags);
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(alloc_ezbuf())) {
    LOG_WARN("ez_buf_ alloc failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (NULL != ez_buf_ && !ok_param.has_more_result_) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
    }
    if (OB_FAIL(response_packet(okp))) {
      LOG_WARN("response ok packet fail", K(ret));
    }
  }
  if (need_track_session_info) {
    session.reset_session_changed_info();
  }
  // if somethig wrong during send_ok_paket, disconnect
  if (OB_FAIL(ret)) {
    disconnect();
  }

  return ret;
}

ObSMConnection* ObMPPacketSender::get_conn() const
{
  ObSMConnection* conn = NULL;
  if (OB_ISNULL(req_)) {
    LOG_ERROR("request is null");
  } else if (conn_valid_) {
    conn = reinterpret_cast<ObSMConnection*>(req_->get_session());
  } else {
    // do nothing
  }
  return conn;
}

void ObMPPacketSender::disconnect()
{
  if (conn_valid_) {
    if (OB_ISNULL(req_)) {  // do nothing
    } else {
      easy_request_t* ez_req = req_->get_request();
      ObSMConnection* conn = reinterpret_cast<ObSMConnection*>(req_->get_session());
      if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms)) {  // do nothing
      } else {
        easy_connection_destroy_dispatch(ez_req->ms->c);
        if (conn != NULL) {
          LOG_WARN(
              "server close connection", "sessid", conn->sessid_, "proxy_sessid", conn->proxy_sessid_, "stack", lbt());
        } else {
          LOG_WARN("server close connection");
        }
      }
    }
  }
}

int ObMPPacketSender::send_eof_packet(const ObSQLSessionInfo& session, const ObMySQLResultSet& result)
{
  int ret = OB_SUCCESS;
  OMPKEOF eofp;
  const ObWarningBuffer* warnings_buf = common::ob_get_tsi_warning_buffer();
  uint16_t warning_count = 0;
  bool ac = true;
  if (OB_ISNULL(warnings_buf)) {
    LOG_WARN("can not get thread warnings buffer");
  } else {
    warning_count = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
  }
  eofp.set_warning_count(warning_count);
  ObServerStatusFlags flags = eofp.get_server_status();
  if (session.is_server_status_in_transaction()) {
    flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = 1;
  }
  if (ac) {
    flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = 1;
  }
  flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = result.has_more_result();
  eofp.set_server_status(flags);
  if (OB_FAIL(response_packet(eofp))) {
    LOG_WARN("response packet fail", K(ret));
  }
  return ret;
}

int ObMPPacketSender::try_encode_with(ObMySQLPacket& pkt, int64_t current_size, int64_t& seri_size, int64_t try_steps)
{
  int ret = OB_SUCCESS;
  ObProtoEncodeParam param;
  seri_size = 0;
  if (OB_UNLIKELY(!conn_valid_)) {
    // force core dump in debug
    ret = OB_CONNECT_ERROR;
    LOG_ERROR("connection already disconnected", K(ret));
  } else if (OB_UNLIKELY(req_has_wokenup_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("req_has_wokenup, resource maybe has destroy", K(ret));
  } else if (OB_ISNULL(ez_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy buffer is null", K(ret));
  } else if (OB_ISNULL(req_) || OB_ISNULL(req_->get_ez_req())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input", K(ret), KP(req_));
  } else if (OB_FAIL(build_encode_param(param, &pkt, false))) {
    LOG_ERROR("fail to build encode param", K(ret));
  } else if (OB_FAIL(ObProto20Utils::do_packet_encode(param))) {
    LOG_ERROR(" fail to do packet encode", K(param), K(ret));
  } else {
    if (param.need_flush_) {
      // if failed, try flush ---> alloc larger mem ----> continue encoding
      int last_ret = param.encode_ret_;

      if (need_flush_buffer()) {
        // try again with same buf size
        if (OB_FAIL(flush_buffer(false))) {
          LOG_WARN("failed to flush_buffer", K(ret), K(last_ret));
        } else {
          ret = try_encode_with(pkt, current_size, seri_size, try_steps);
        }
      } else {
        if (try_steps >= MAX_TRY_STEPS) {
          ret = last_ret;
          LOG_ERROR("encode MySQL packet fail", "buffer size", ez_buf_->end - ez_buf_->last, K(ret));
        } else {
          // try again with larger buf size
          const int64_t new_alloc_size = TRY_EZ_BUF_SIZES[try_steps++];
          // open later
          //          if (OB_SIZE_OVERFLOW != last_ret) {
          //            ret = last_ret;
          //            LOG_WARN("last_ret is not size overflow, need check code", K(last_ret));
          //          } else
          if (OB_FAIL(resize_ezbuf(new_alloc_size))) {
            LOG_ERROR("fail to resize_ezbuf", K(ret), K(last_ret));
          } else {
            ret = try_encode_with(pkt, new_alloc_size, seri_size, try_steps);
          }
        }
      }
    } else {
      // nothing, continue encoding next
    }
  }
  return ret;
}

bool ObMPPacketSender::need_flush_buffer() const
{
  bool bret = false;
  // there is data in buf
  if (NULL != ez_buf_ && ez_buf_->pos != ez_buf_->last) {
    if (comp_context_.is_proxy_compress_based() && comp_context_.last_pkt_pos_ == ez_buf_->pos) {
      // the whole buf is proxy last packet, no need flush part
    } else {
      bret = true;
    }
  }
  return bret;
}

int ObMPPacketSender::flush_buffer(const bool is_last)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!conn_valid_)) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection in error, maybe has disconnected", K(ret));
  } else if (req_has_wokenup_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("req_has_wokenup, resource maybe has destroy", K(ret));
  } else if (OB_ISNULL(req_) || OB_ISNULL(req_->get_ez_req())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("req_ is null", KP_(ez_buf), KP_(req), K(ret));
  } else if (OB_FAIL(alloc_ezbuf())) {
    LOG_ERROR("easy buffer alloc failed", KP_(ez_buf), K(ret));
  } else {
    if (proto20_context_.is_proto20_used() && (FILL_PAYLOAD_STEP == proto20_context_.next_step_)) {
      ObProtoEncodeParam param;
      if (OB_FAIL(build_encode_param(param, NULL, is_last))) {
        LOG_ERROR("fail to build encode param", K(ret));
      } else if (OB_FAIL(ObProto20Utils::fill_proto20_header_and_tailer(param))) {
        LOG_ERROR("fail to fill ob20 protocol header and tailer", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObFlushBufferParam flush_param(
          *ez_buf_, *req_->get_ez_req(), comp_context_, conn_valid_, req_has_wokenup_, io_thread_mark_, is_last);
      if (OB_FAIL(ObMySQLRequestUtils::flush_buffer(flush_param))) {
        LOG_WARN("failed to flush_buffer", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (proto20_context_.is_proto20_used()) {
        // after flush, set pos to 0
        proto20_context_.curr_proto20_packet_start_pos_ = 0;
      }
    }
  }
  return ret;
}

inline int ObMPPacketSender::build_encode_param(ObProtoEncodeParam& param, ObMySQLPacket* pkt, const bool is_last)
{
  INIT_SUCC(ret);
  uint32_t conn_id = 0;
  if (OB_FAIL(get_conn_id(conn_id))) {
    LOG_WARN("fail to build param", K(ret));
  } else {
    ObProtoEncodeParam::build_param(param, pkt, *ez_buf_, conn_id, is_last, proto20_context_);
  }
  return ret;
}

int ObMPPacketSender::get_conn_id(uint32_t& sessid) const
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get connection fail");
  } else {
    sessid = conn->sessid_;
  }
  return ret;
}

int ObMPPacketSender::resize_ezbuf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_) || OB_ISNULL(ez_buf_) || OB_UNLIKELY(size < static_cast<int64_t>(sizeof(easy_buf_t)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input size", K(size), KP_(req), KP_(ez_buf), K(ret));
  } else {
    const int64_t remain_data_size = ez_buf_->last - ez_buf_->pos;
    char* buf = NULL;
    if (size - sizeof(easy_buf_t) < remain_data_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new size is too little", K(remain_data_size), K(size), K(ret));
    } else if (OB_ISNULL(buf = req_->easy_alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory failed", K(size), K(ret));
      disconnect();
    } else {
      easy_buf_t* tmp = reinterpret_cast<easy_buf_t*>(buf);
      init_easy_buf(tmp, reinterpret_cast<char*>(tmp + 1), req_->get_ez_req(), size - sizeof(easy_buf_t));
      if (remain_data_size > 0) {
        // if ezbuf has leave data, we need move it
        MEMCPY(tmp->last, ez_buf_->pos, remain_data_size);
        tmp->last = tmp->pos + remain_data_size;
        ez_buf_->last = ez_buf_->pos;
      }
      if (comp_context_.last_pkt_pos_ == ez_buf_->pos) {
        comp_context_.last_pkt_pos_ = tmp->pos;
      }
      ez_buf_ = tmp;
    }
  }
  return ret;
}

// current lob will <= 64MB, TODO oushen
int64_t ObMPPacketSender::TRY_EZ_BUF_SIZES[] = {
    64 * 1024, 2 * 1024 * 1024 - 1024, 4 * 1024 * 1024 - 1024, 64 * 1024 * 1024 - 1024, 128 * 1024 * 1024};

// current lob will <= 64MB, TODO oushen
int64_t ObMPBase::TRY_EZ_BUF_SIZES[] = {
    64 * 1024, 2 * 1024 * 1024 - 1024, 4 * 1024 * 1024 - 1024, 64 * 1024 * 1024 - 1024, 128 * 1024 * 1024};

ObMPBase::ObMPBase(const ObGlobalContext& gctx)
    : gctx_(gctx), seq_(0), comp_context_(), ez_buf_(NULL), process_timestamp_(0)
{}

ObMPBase::~ObMPBase()
{
  if (!THIS_WORKER.need_retry()) {
    ObReqProcessor::wakeup_request();
  }
}

int ObMPBase::serialize()
{
  return OB_SUCCESS;
}

int ObMPBase::response(const int retcode)
{
  UNUSED(retcode);
  int ret = OB_SUCCESS;
  if (!THIS_WORKER.need_retry()) {
    if (OB_FAIL(flush_buffer(true))) {
      LOG_WARN("failed to flush_buffer", K(ret));
    }
  }
  return ret;
}

int ObMPBase::before_process()
{
  int ret = OB_SUCCESS;
  process_timestamp_ = common::ObTimeUtility::current_time();
  easy_request_t* ez_req = NULL;
  ObSMConnection* conn = NULL;
  char* buf = NULL;
  if (OB_ISNULL(req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("request or data buffer is null", KP(req_), K(ret));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get conn", K(ret));
  } else if (OB_ISNULL(ez_req = req_->get_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy request is null", K(ret));
  } else if (OB_ISNULL(buf = req_->easy_reusable_alloc(OB_MULTI_RESPONSE_BUF_SIZE + sizeof(easy_buf_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to easy_alloc", "size", OB_MULTI_RESPONSE_BUF_SIZE + sizeof(easy_buf_t), K(ret));
  } else {
    ez_buf_ = reinterpret_cast<easy_buf_t*>(buf);
    init_easy_buf(ez_buf_, reinterpret_cast<char*>(ez_buf_ + 1), ez_req, OB_MULTI_RESPONSE_BUF_SIZE);
    seq_ = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet()).get_seq();
    seq_++;
    comp_context_.reset();
    comp_context_.seq_ = seq_;
    comp_context_.type_ = conn->get_compress_type();
    comp_context_.sessid_ = conn->sessid_;

    bool is_proto20_supported = (OB_2_0_CS_TYPE == conn->get_cs_protocol_type());
    if (is_proto20_supported) {
      proto20_context_.reset();
      proto20_context_.is_proto20_used_ = is_proto20_supported;
      proto20_context_.comp_seq_ = seq_;
      proto20_context_.proto20_seq_ = static_cast<uint8_t>(conn->proto20_pkt_context_.proto20_last_pkt_seq_ + 1);
      proto20_context_.request_id_ = conn->proto20_pkt_context_.proto20_last_request_id_;
      proto20_context_.header_len_ = OB20_PROTOCOL_HEADER_LENGTH + OB_MYSQL_COMPRESSED_HEADER_SIZE;
      proto20_context_.tailer_len_ = OB20_PROTOCOL_TAILER_LENGTH;
      proto20_context_.next_step_ = START_TO_FILL_STEP;
      proto20_context_.is_checksum_off_ = false;
    }
  }

  if (OB_FAIL(ret)) {
    send_error_packet(ret, NULL);
  }
  return ret;
}

int ObMPBase::update_transmission_checksum_flag(const ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = NULL;
  if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get conn", K(ret));
  } else if (conn->proxy_cap_flags_.is_checksum_swittch_support()) {
    bool is_enable_checksum = false;
    if (OB_FAIL(session.is_use_transmission_checksum(is_enable_checksum))) {
      if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret || OB_SYS_VARS_MAYBE_DIFF_VERSION == ret) {
        LOG_INFO("is enable checksum system variable maby from diff version");
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("is use transmission checksum failed", K(ret));
      }
    } else {
      comp_context_.is_checksum_off_ = !is_enable_checksum;
      proto20_context_.is_checksum_off_ = !is_enable_checksum;
    }
  }
  return ret;
}

int ObMPBase::update_proxy_sys_vars(ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = NULL;
  if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get conn", K(ret));
  } else if (OB_FAIL(session.set_proxy_user_privilege(session.get_user_priv_set()))) {
    LOG_WARN("fail to set proxy user privilege system variables", K(ret));
  } else if (OB_FAIL(session.set_proxy_capability(conn->proxy_cap_flags_.capability_))) {
    LOG_WARN("fail to set proxy capability", K(ret));
  }
  return ret;
}

int ObMPBase::before_response()
{
  return OB_SUCCESS;
}

int ObMPBase::after_process()
{
  int ret = OB_SUCCESS;
  if (!lib::is_diagnose_info_enabled()) {
  } else {
    NG_TRACE_EXT(process_end, OB_ID(run_ts), get_run_timestamp());
    const int64_t elapsed_time = common::ObTimeUtility::current_time() - get_receive_timestamp();
    bool is_slow = (elapsed_time > GCONF.trace_log_slow_query_watermark) && !THIS_WORKER.need_retry();
    if (is_slow) {
      if (THIS_WORKER.need_retry() && OB_TRY_LOCK_ROW_CONFLICT == process_ret_) {
      } else {
        FORCE_PRINT_TRACE(THE_TRACE, "[slow query]");
      }
    } else if (can_force_print(process_ret_)) {
      NG_TRACE_EXT(process_ret, Y_(process_ret));
      FORCE_PRINT_TRACE(THE_TRACE, "[err query]");
    } else if (THIS_WORKER.need_retry()) {
      if (OB_TRY_LOCK_ROW_CONFLICT != process_ret_) {
        FORCE_PRINT_TRACE(THE_TRACE, "[packet retry query]");
      }
    } else {
      PRINT_TRACE(THE_TRACE);
    }
  }
  return ret;
}

void ObMPBase::disconnect()
{
  if (conn_valid_) {
    if (OB_ISNULL(req_)) {  // do nothing
    } else {
      easy_request_t* ez_req = req_->get_request();
      ObSMConnection* conn = reinterpret_cast<ObSMConnection*>(req_->get_session());
      if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms)) {  // do nothing
      } else {
        easy_connection_destroy_dispatch(ez_req->ms->c);
        if (conn != NULL) {
          LOG_WARN(
              "server close connection", "sessid", conn->sessid_, "proxy_sessid", conn->proxy_sessid_, "stack", lbt());
        } else {
          LOG_WARN("server close connection");
        }
      }
    }
  }
}

int ObMPBase::flush_buffer(const bool is_last)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!conn_valid_)) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection in error, maybe has disconnected", K(ret));
  } else if (req_has_wokenup_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("req_has_wokenup, resource maybe has destroy", K(ret));
  } else if (OB_ISNULL(ez_buf_) || OB_ISNULL(req_) || OB_ISNULL(req_->get_ez_req())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("easy buffer or req_ is null", KP_(ez_buf), KP_(req), K(ret));
  } else {
    if (proto20_context_.is_proto20_used() && (FILL_PAYLOAD_STEP == proto20_context_.next_step_)) {
      ObProtoEncodeParam param;
      if (OB_FAIL(build_encode_param(param, NULL, is_last))) {
        LOG_ERROR("fail to build encode param", K(ret));
      } else if (OB_FAIL(ObProto20Utils::fill_proto20_header_and_tailer(param))) {
        LOG_ERROR("fail to fill ob20 protocol header and tailer", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObFlushBufferParam flush_param(
          *ez_buf_, *req_->get_ez_req(), comp_context_, conn_valid_, req_has_wokenup_, io_thread_mark_, is_last);
      if (OB_FAIL(ObMySQLRequestUtils::flush_buffer(flush_param))) {
        LOG_WARN("failed to flush_buffer", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (proto20_context_.is_proto20_used()) {
        // after flush, set pos to 0
        proto20_context_.curr_proto20_packet_start_pos_ = 0;
      }
    }
  }
  return ret;
}

int ObMPBase::resize_ezbuf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_) || OB_ISNULL(ez_buf_) || OB_UNLIKELY(size < static_cast<int64_t>(sizeof(easy_buf_t)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input size", K(size), KP_(req), KP_(ez_buf), K(ret));
  } else {
    const int64_t remain_data_size = ez_buf_->last - ez_buf_->pos;
    char* buf = NULL;
    if (size - sizeof(easy_buf_t) < remain_data_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new size is too little", K(remain_data_size), K(size), K(ret));
    } else if (OB_ISNULL(buf = easy_alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory failed", K(size), K(ret));
      disconnect();
    } else {
      easy_buf_t* tmp = reinterpret_cast<easy_buf_t*>(buf);
      init_easy_buf(tmp, reinterpret_cast<char*>(tmp + 1), req_->get_ez_req(), size - sizeof(easy_buf_t));
      if (remain_data_size > 0) {
        // if ezbuf has leave data, we need move it
        MEMCPY(tmp->last, ez_buf_->pos, remain_data_size);
        tmp->last = tmp->pos + remain_data_size;
        ez_buf_->last = ez_buf_->pos;
      }
      if (comp_context_.last_pkt_pos_ == ez_buf_->pos) {
        comp_context_.last_pkt_pos_ = tmp->pos;
      }
      ez_buf_ = tmp;
    }
  }
  return ret;
}

bool ObMPBase::need_flush_buffer() const
{
  bool bret = false;
  // there is data in buf
  if (NULL != ez_buf_ && ez_buf_->pos != ez_buf_->last) {
    if (comp_context_.is_proxy_compress_based() && comp_context_.last_pkt_pos_ == ez_buf_->pos) {
      // the whole buf is proxy last packet, no need flush part
    } else {
      bret = true;
    }
  }
  return bret;
}

inline int ObMPBase::build_encode_param(ObProtoEncodeParam& param, ObMySQLPacket* pkt, const bool is_last)
{
  INIT_SUCC(ret);
  uint32_t conn_id = 0;
  if (OB_FAIL(get_conn_id(conn_id))) {
    LOG_WARN("fail to build param", K(ret));
  } else {
    ObProtoEncodeParam::build_param(param, pkt, *ez_buf_, conn_id, is_last, proto20_context_);
  }
  return ret;
}

int ObMPBase::try_encode_with(ObMySQLPacket& pkt, int64_t current_size, int64_t& seri_size, int64_t try_steps)
{
  int ret = OB_SUCCESS;
  ObProtoEncodeParam param;
  seri_size = 0;
  if (OB_UNLIKELY(!conn_valid_)) {
    // force core dump in debug
    ret = OB_CONNECT_ERROR;
    LOG_ERROR("connection already disconnected", K(ret));
  } else if (OB_UNLIKELY(req_has_wokenup_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("req_has_wokenup, resource maybe has destroy", K(ret));
  } else if (OB_ISNULL(ez_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy buffer is null", K(ret));
  } else if (OB_ISNULL(req_) || OB_ISNULL(req_->get_ez_req())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input", K(ret), KP(req_));
  } else if (OB_FAIL(build_encode_param(param, &pkt, false))) {
    LOG_ERROR("fail to build encode param", K(ret));
  } else if (OB_FAIL(ObProto20Utils::do_packet_encode(param))) {
    LOG_ERROR(" fail to do packet encode", K(param), K(ret));
  } else {
    if (param.need_flush_) {
      // if failed, try flush ---> alloc larger mem ----> continue encoding
      int last_ret = param.encode_ret_;

      if (need_flush_buffer()) {
        // try again with same buf size
        if (OB_FAIL(flush_buffer(false))) {
          LOG_WARN("failed to flush_buffer", K(ret), K(last_ret));
        } else {
          ret = try_encode_with(pkt, current_size, seri_size, try_steps);
        }
      } else {
        if (try_steps >= MAX_TRY_STEPS) {
          ret = last_ret;
          LOG_ERROR("encode MySQL packet fail", "buffer size", ez_buf_->end - ez_buf_->last, K(ret));
        } else {
          // try again with larger buf size
          const int64_t new_alloc_size = TRY_EZ_BUF_SIZES[try_steps++];
          // open later
          //          if (OB_SIZE_OVERFLOW != last_ret) {
          //            ret = last_ret;
          //            LOG_WARN("last_ret is not size overflow, need check code", K(last_ret));
          //          } else
          if (OB_FAIL(resize_ezbuf(new_alloc_size))) {
            LOG_ERROR("fail to resize_ezbuf", K(ret), K(last_ret));
          } else {
            ret = try_encode_with(pkt, new_alloc_size, seri_size, try_steps);
          }
        }
      }
    } else {
      // nothing, continue encoding next
    }
  }
  return ret;
}

int ObMPBase::response_packet(ObMySQLPacket& pkt)
{
  int ret = OB_SUCCESS;
  if (!conn_valid_) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection already disconnected", K(ret));
  } else if (OB_ISNULL(ez_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy buffer is null", K(ret));
  } else {
    int64_t seri_size = 0;
    pkt.set_seq(seq_);
    if (OB_FAIL(try_encode_with(pkt, ez_buf_->end - ez_buf_->pos, seri_size, 0))) {
      LOG_WARN("failed to encode packet", K(ret));
    } else {
      LOG_DEBUG("succ encode packet", K(pkt), K(seri_size));
      seq_ = pkt.get_seq();  // here will point next avail seq
      EVENT_INC(MYSQL_PACKET_OUT);
      EVENT_ADD(MYSQL_PACKET_OUT_BYTES, seri_size);
    }
  }
  return ret;
}

ObSMConnection* ObMPBase::get_conn() const
{
  ObSMConnection* conn = NULL;
  if (OB_ISNULL(req_)) {
    LOG_ERROR("request is null");
  } else if (conn_valid_) {
    conn = reinterpret_cast<ObSMConnection*>(req_->get_session());
  } else {
    // do nothing
  }
  return conn;
}

int ObMPBase::get_conn_id(uint32_t& sessid) const
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get connection fail");
  } else {
    sessid = conn->sessid_;
  }
  return ret;
}

int ObMPBase::send_error_packet(
    int err, const char* errmsg, bool is_partition_hit /* = true */, void* extra_err_info /* = NULL */)
{
  int ret = OB_SUCCESS;
  BACKTRACE(ERROR, (OB_SUCCESS == err), "BUG send error packet but err code is 0");
  LOG_INFO("sending error packet", K(err), "bt", lbt(), K(extra_err_info));
  ObSMConnection* conn = NULL;
  if (OB_SUCCESS == err || (OB_ERR_PROXY_REROUTE == err && OB_ISNULL(extra_err_info))) {
    // @BUG work around
    ret = OB_ERR_UNEXPECTED;
  } else if (!conn_valid_) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection already disconnected", K(ret));
  } else if (OB_ISNULL(conn = get_conn()) || OB_ISNULL(ez_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get connection fail or ez_buf_ is null", K(conn), K_(ez_buf), K(ret));
  } else {
    // error message
    ObString message;
    const int32_t MAX_MSG_BUF_SIZE = 512;
    char msg_buf[MAX_MSG_BUF_SIZE];
    const ObWarningBuffer* wb = common::ob_get_tsi_warning_buffer();
    if (NULL != wb && (wb->get_err_code() == err ||
                          (err >= OB_MIN_RAISE_APPLICATION_ERROR && err <= OB_MAX_RAISE_APPLICATION_ERROR))) {
      message = wb->get_err_msg();
    }
    if (message.length() <= 0) {
      if (OB_LIKELY(NULL != errmsg) && 0 < strlen(errmsg)) {
        message = ObString::make_string(errmsg);
      } else if (err >= OB_MIN_RAISE_APPLICATION_ERROR && err <= OB_MAX_RAISE_APPLICATION_ERROR) {
        // do nothing ...
      } else {
        snprintf(msg_buf, MAX_MSG_BUF_SIZE, "%s", ob_errpkt_strerror(err, lib::is_oracle_mode()));
        message = ObString::make_string(msg_buf);  // default error message
      }
    }

    if (ObServerConfig::get_instance().enable_rich_error_msg) {
      int32_t msg_buf_size = 0;
      const uint64_t* trace_id = ObCurTraceId::get();
      uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
      uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];
      const ObAddr addr = ObCurTraceId::get_addr();

      struct timeval tv;
      struct tm tm;
      char addr_buf[MAX_IP_PORT_LENGTH];
      (void)gettimeofday(&tv, NULL);
      ::localtime_r((const time_t*)&tv.tv_sec, &tm);
      addr.ip_port_to_string(addr_buf, sizeof(addr_buf));

      char tmp_msg_buf[MAX_MSG_BUF_SIZE];
      strncpy(tmp_msg_buf, message.ptr(), message.length());  // msg_buf is overwriten
      msg_buf_size = snprintf(msg_buf,
          MAX_MSG_BUF_SIZE,
          "%.*s\n"
          "[%s] "
          "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
          "[" TRACE_ID_FORMAT "]",
          message.length(),
          tmp_msg_buf,
          addr_buf,
          tm.tm_year + 1900,
          tm.tm_mon + 1,
          tm.tm_mday,
          tm.tm_hour,
          tm.tm_min,
          tm.tm_sec,
          tv.tv_usec,
          trace_id_0,
          trace_id_1);
      (void)msg_buf_size;                        // make compiler happy
      message = ObString::make_string(msg_buf);  // default error message
    }

    OMPKError epacket;
    // TODO Negotiate a err for rerouting sql
    epacket.set_errcode(static_cast<uint16_t>(ob_errpkt_errno(err, lib::is_oracle_mode())));
    ret = epacket.set_sqlstate(ob_sqlstate(err));
    if (OB_SUCC(ret) && OB_SUCC(epacket.set_message(message))) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
      if (OB_FAIL(response_packet(epacket))) {
        RPC_LOG(WARN, "failed to send error packet", K(epacket), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "failed to set error info", K(err), K(errmsg), K(ret));
    }
  }

  //
  // for obproxy or OCJ, followed by another OK packet
  if (conn_valid_) {
    if (OB_FAIL(ret)) {
      disconnect();
    } else if (conn->need_send_extra_ok_packet()) {
      if (conn->is_in_authed_phase()) {
        sql::ObSQLSessionInfo* session = NULL;
        if (OB_FAIL(get_session(session))) {
          LOG_WARN("fail to get session", K(ret));
        } else if (OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql session info is null", K(ret));
        } else {
          ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
          ObOKPParam ok_param;
          if (session->is_track_session_info()) {
            ok_param.is_partition_hit_ = is_partition_hit;
            ok_param.take_trace_id_to_client_ = true;
          }
          if (OB_ERR_PROXY_REROUTE == err) {
            ok_param.reroute_info_ = static_cast<ObFeedbackRerouteInfo*>(extra_err_info);
          }
          if (OB_FAIL(send_ok_packet(*session, ok_param))) {
            LOG_WARN("failed to send ok packet", K(ok_param), K(ret));
          }
        }
        if (OB_LIKELY(NULL != session)) {
          revert_session(session);
        }
      } else {  // just a basic ok packet contain nothing
        OMPKOK okp;
        okp.set_capability(conn->cap_flags_);
        if (OB_FAIL(response_packet(okp))) {
          LOG_WARN("response ok packet fail", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append ok pkt after error pkt, need disconnect", K(ret));
        disconnect();
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObMPBase::send_switch_packet(ObString& auth_name, ObString& auth_data)
{
  int ret = OB_SUCCESS;
  OMPKChangeUser packet;
  packet.set_auth_plugin_name(auth_name);
  packet.set_auth_response(auth_data);
  if (OB_FAIL(response_packet(packet))) {
    LOG_WARN("failed to send switch packet", K(packet), K(ret));
  }
  return ret;
}

int ObMPBase::load_system_variables(const ObSysVariableSchema& sys_variable_schema, ObSQLSessionInfo& session) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema.get_sysvar_count(); ++i) {
    const ObSysVarSchema* sysvar = NULL;
    sysvar = sys_variable_schema.get_sysvar_schema(i);
    if (sysvar != NULL) {
      LOG_DEBUG("load system variable", K(*sysvar));
      if (OB_FAIL(session.load_sys_variable(sysvar->get_name(),
              sysvar->get_data_type(),
              sysvar->get_value(),
              sysvar->get_min_val(),
              sysvar->get_max_val(),
              sysvar->get_flags(),
              true))) {
        LOG_WARN("load sys variable failed", K(ret), K(*sysvar));
      }
    }
  }
  if (OB_SUCC(ret)) {
    session.set_global_vars_version(sys_variable_schema.get_schema_version());
    if (OB_FAIL(session.gen_sys_var_in_pc_str())) {
      LOG_WARN("fail to gen sys var in pc str", K(ret));
    }
  }
  return ret;
}

int ObMPBase::send_ok_packet(ObSQLSessionInfo& session, ObOKPParam& ok_param)
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  OMPKOK okp;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is null", K(ret));
  } else {
    okp.set_affected_rows(ok_param.affected_rows_);
    if (OB_UNLIKELY(NULL != ok_param.message_)) {
      ObString message_str;
      message_str.assign_ptr(ok_param.message_, static_cast<int32_t>(strlen(ok_param.message_)));
      okp.set_message(message_str);
    }

    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    okp.set_capability(session.get_capability());
    okp.set_last_insert_id(ok_param.lii_);
    okp.set_warnings(ok_param.warnings_count_);

    // set is_partition_hit
    if (!ok_param.is_partition_hit_) {
      ret = session.set_partition_hit(false);
    }

    if (OB_SUCC(ret)) {
      if (!ok_param.take_trace_id_to_client_) {
        const int64_t elapsed_time = common::ObTimeUtility::current_time() - get_receive_timestamp();
        bool is_slow = (elapsed_time > GCONF.trace_log_slow_query_watermark);
        ok_param.take_trace_id_to_client_ = is_slow;
      }
      if (ok_param.take_trace_id_to_client_) {
        ObCurTraceId::TraceId* trace_id = ObCurTraceId::get_trace_id();
        if (NULL != trace_id) {
          if (OB_FAIL(session.update_last_trace_id(*trace_id))) {
            LOG_WARN("fail to update last trace id", KPC(trace_id), K(ret));
            ret = OB_SUCCESS;  // ignore ret
          }
        }
      }
    }
  }

  // set is_trans_specfied
  if (OB_SUCC(ret)) {
    ret = session.save_trans_status();
  }

  bool need_track_session_info = false;
  if (OB_SUCC(ret)) {
    if (session.is_track_session_info()) {
      // in hand shake response's ok packet, return all session variable
      if (ok_param.is_on_connect_) {
        need_track_session_info = true;
        if (conn->is_normal_client()) {
          okp.set_use_standard_serialize(true);
          if (OB_FAIL(ObMPUtils::add_nls_format(okp, session))) {
            LOG_WARN("fail to add_nls_format", K(ret));
          }
        } else {
          if (OB_FAIL(ObMPUtils::add_session_info_on_connect(okp, session))) {
            LOG_WARN("fail to add all session system variables", K(ret));
          }
        }
      } else {
        // in proxy or OCJ mode will track session info in the last ok packet
        if (!ok_param.has_more_result_) {
          need_track_session_info = true;
          // if safe weak read support, we should add safe weak read
          ObProxyCapabilityFlags client_cap(conn->proxy_cap_flags_);
          const int64_t snapshot_version = session.get_safe_weak_read_snapshot();
          if (client_cap.is_safe_weak_read_support() && snapshot_version > 0) {
            if (OB_FAIL(session.set_safe_weak_read_snapshot_variable(snapshot_version))) {
              SERVER_LOG(WARN, "fail set safe weak read snapshot ", K(ret));
            } else {
              SERVER_LOG(DEBUG, "succ set safe weak read snapshot", K(snapshot_version));
            }
          }
          // send changed session variables to obproxy
          if (OB_SUCC(ret)) {
            if (conn->is_normal_client()) {
              okp.set_use_standard_serialize(true);
              if (OB_FAIL(ObMPUtils::add_nls_format(okp, session, true))) {
                LOG_WARN("fail to add_nls_format", K(ret));
              }
            } else {
              if (OB_FAIL(ObMPUtils::add_changed_session_info(okp, session))) {
                SERVER_LOG(WARN, "fail to add changed session info", K(ret));
              } else if (ok_param.reroute_info_ != NULL &&
                         OB_FAIL(ObMPUtils::add_client_reroute_info(okp, session, *ok_param.reroute_info_))) {
                LOG_WARN("failed to add reroute info", K(ret));
              } else {
                // do nothing
              }
            }
          }
        }
      }
    } else {
      // use to compatible with older OCJ(< 2.0.9)
      if (ok_param.is_on_connect_ && conn->is_java_client_ && conn->proxy_cap_flags_.is_cap_used()) {
        okp.set_track_session_cap(true);
        if (OB_FAIL(ObMPUtils::add_session_info_on_connect(okp, session))) {
          LOG_WARN("fail to add all session system variables", K(ret));
        } else {
          need_track_session_info = true;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool ac = true;
    if (OB_FAIL(session.get_autocommit(ac))) {
      LOG_WARN("fail to get autocommit", K(ret));
    } else {
      ObServerStatusFlags flags = okp.get_server_status();
      flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = (session.is_server_status_in_transaction() ? 1 : 0);
      flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
      flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = ok_param.has_more_result_;
      if (!conn->is_proxy_) {
        // in java client or others, use slow query bit to indicate partition hit or not
        flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !ok_param.is_partition_hit_;
      }
      if (ok_param.is_on_connect_) {
        flags.status_flags_.OB_SERVER_STATUS_RESERVED_OR_ORACLE_MODE =
            (ORACLE_MODE == session.get_compatibility_mode() ? 1 : 0);
      }
      // todo: set OB_SERVER_STATUS_IN_TRANS_READONLY flag if need?
      okp.set_server_status(flags);
    }
  }
  if (OB_SUCC(ret)) {
    if (NULL != ez_buf_ && !ok_param.has_more_result_) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
    }
    if (OB_FAIL(response_packet(okp))) {
      LOG_WARN("response ok packet fail", K(ret));
    }
  }
  if (need_track_session_info) {
    session.reset_session_changed_info();
  }
  // if somethig wrong during send_ok_paket, disconnect
  if (OB_FAIL(ret)) {
    disconnect();
  }

  return ret;
}

int ObMPBase::send_eof_packet(const ObSQLSessionInfo& session, const ObMySQLResultSet& result)
{
  int ret = OB_SUCCESS;
  OMPKEOF eofp;
  const ObWarningBuffer* warnings_buf = common::ob_get_tsi_warning_buffer();
  uint16_t warning_count = 0;
  bool ac = true;
  if (OB_ISNULL(warnings_buf)) {
    LOG_WARN("can not get thread warnings buffer");
  } else {
    warning_count = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
  }
  eofp.set_warning_count(warning_count);
  ObServerStatusFlags flags = eofp.get_server_status();
  if (session.is_server_status_in_transaction()) {
    flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = 1;
  }
  if (ac) {
    flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = 1;
  }
  flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = result.has_more_result();
  eofp.set_server_status(flags);
  if (OB_FAIL(response_packet(eofp))) {
    LOG_WARN("response packet fail", K(ret));
  }
  return ret;
}

int ObMPBase::create_session(ObSMConnection* conn, ObSQLSessionInfo*& sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get connection fail", K(ret));
  } else if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session manager is null", K(ret));
  } else {
    if (OB_FAIL(gctx_.session_mgr_->create_session(conn, sess_info))) {
      LOG_WARN("create session fail",
          "version",
          conn->version_,
          "sessid",
          conn->sessid_,
          "proxy_sessid",
          conn->proxy_sessid_,
          K(ret));
    } else {
      LOG_DEBUG("create session successfully",
          "version",
          conn->version_,
          "sessid",
          conn->sessid_,
          "proxy_sessid",
          conn->proxy_sessid_);
      conn->is_sess_alloc_ = true;
      sess_info->set_user_session();
      sess_info->set_shadow(false);
      if (req_->get_ssl_st() != NULL) {
        sess_info->set_ssl_cipher(SSL_get_cipher_name((SSL*)req_->get_ssl_st()));
      } else {
        sess_info->set_ssl_cipher("");
      }
    }
  }
  return ret;
}

int ObMPBase::free_session()
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get connection fail", K(ret));
  } else if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session manager is null", K(ret));
  } else {
    ObFreeSessionCtx ctx;
    ctx.tenant_id_ = conn->tenant_id_;
    ctx.version_ = conn->version_;
    ctx.sessid_ = conn->sessid_;
    ctx.proxy_sessid_ = conn->proxy_sessid_;
    ctx.has_inc_active_num_ = conn->has_inc_active_num_;
    if (OB_FAIL(gctx_.session_mgr_->free_session(ctx))) {
      LOG_WARN("fail to free session", K(ctx), K(ret));
    } else {
      LOG_INFO("free session successfully", K(ctx));
      conn->is_sess_free_ = true;
    }
  }
  return ret;
}

int ObMPBase::get_session(ObSQLSessionInfo*& sess_info)
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(conn) || OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn or sessoin mgr is NULL", K(ret), K(conn), K(GCTX.session_mgr_));
  } else if (OB_FAIL(gctx_.session_mgr_->get_session(conn->version_, conn->sessid_, sess_info))) {
    LOG_ERROR("get session fail",
        K(ret),
        "version",
        conn->version_,
        "sessid",
        conn->sessid_,
        "proxy_sessid",
        conn->proxy_sessid_);
  } else {
    NG_TRACE_EXT(session, OB_ID(sid), sess_info->get_sessid(), OB_ID(tenant_id), sess_info->get_priv_tenant_id());
  }
  return ret;
}

int ObMPBase::revert_session(ObSQLSessionInfo* sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.session_mgr_) || OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session mgr or session info is null", K_(gctx_.session_mgr), K(sess_info), K(ret));
  } else {
    if (OB_FAIL(gctx_.session_mgr_->revert_session(sess_info))) {
      LOG_ERROR("revert session fail",
          "sess_id",
          sess_info->get_sessid(),
          "proxy_sessid",
          sess_info->get_proxy_sessid(),
          K(ret));
    }
  }
  return ret;
}

int ObMPBase::init_process_var(sql::ObSqlCtx& ctx, const ObMultiStmtItem& multi_stmt_item,
    sql::ObSQLSessionInfo& session, ObVirtualTableIteratorFactory& vt_iter_fty, bool& use_trace_log) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session.is_use_trace_log(use_trace_log))) {
    LOG_WARN("fail to get use_trace_log", K(ret));
  } else if (OB_ISNULL(req_) || OB_ISNULL(get_conn())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid request", K(ret), K(req_), K(get_conn()));
  } else {
    const int64_t debug_sync_timeout = GCONF.debug_sync_timeout;
    // ignore session debug sync action actions to thread local actions error
    if (debug_sync_timeout > 0) {
      int tmp_ret = GDS.set_thread_local_actions(session.get_debug_sync_actions());
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("set session debug sync actions to thread local actions failed", K(tmp_ret));
      }
    }
    // construct sql context
    ctx.multi_stmt_item_ = multi_stmt_item;
    ctx.session_info_ = &session;
    ctx.sql_proxy_ = gctx_.sql_proxy_;
    ctx.vt_iter_factory_ = &vt_iter_fty;
    ctx.partition_table_operator_ = gctx_.pt_operator_;
    ctx.session_mgr_ = gctx_.session_mgr_;
    ctx.merged_version_ = *(gctx_.merged_version_);
    ctx.part_mgr_ = gctx_.part_mgr_;
    session.set_rpc_tenant_id(THIS_WORKER.get_rpc_tenant());
    const ObMySQLRawPacket& pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());

    if (0 == multi_stmt_item.get_seq_num() && !session.is_in_transaction()) {
      ctx.can_reroute_sql_ = (pkt.can_reroute_pkt() && get_conn()->is_support_proxy_reroute());
    }
    LOG_TRACE("recorded sql reroute flag", K(ctx.can_reroute_sql_));
  }
  return ret;
}

int ObMPBase::do_after_process(
    sql::ObSQLSessionInfo& session, bool use_session_trace, sql::ObSqlCtx& ctx, bool async_resp_used) const
{
  int ret = OB_SUCCESS;
  session.set_session_data(SESSION_SLEEP, obmysql::OB_MYSQL_COM_SLEEP, 0);
  // reset warning buffers
  if (!async_resp_used) {
    session.reset_warnings_buf();
  }
  // clear tsi warning buffer
  ob_setup_tsi_warning_buffer(NULL);
  // trace end
  if (lib::is_diagnose_info_enabled()) {
    NG_TRACE(query_end);

    if (use_session_trace) {
      if (false == ctx.is_show_trace_stmt_) {
        if (NULL != session.get_trace_buf()) {
          if (nullptr != THE_TRACE) {
            (void)session.get_trace_buf()->assign(*THE_TRACE);
          }
        }
      }
      FORCE_PRINT_TRACE(THE_TRACE, "[show trace]");
    } else if (ctx.force_print_trace_) {
      // query with TRACE_LOG hint can also use SHOW TRACE after its execution
      if (NULL != session.get_trace_buf()) {
        if (nullptr != THE_TRACE) {
          (void)session.get_trace_buf()->assign(*THE_TRACE);
        }
      }
      FORCE_PRINT_TRACE(THE_TRACE, "[trace hint]");
    }
  }

  return ret;
}

int ObMPBase::set_session_active(const ObString& sql, ObSQLSessionInfo& session, const int64_t last_active_time_ts,
    obmysql::ObMySQLCmd cmd /* obmysql::OB_MYSQL_COM_QUERY */) const
{
  return session.set_session_active(sql, get_receive_timestamp(), last_active_time_ts, cmd);
}

int ObMPBase::setup_user_resource_group(ObSMConnection& conn, const uint64_t tenant_id, const uint64_t user_id)
{
  int ret = OB_SUCCESS;
  uint64_t group_id = 0;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tenant", K(tenant_id), K(ret));
  } else if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_user(tenant_id, user_id, group_id))) {
    LOG_WARN("fail get group id by user", K(user_id), K(tenant_id), K(ret));
  } else {
    conn.group_id_ = group_id;
  }
  LOG_DEBUG("setup user resource group", K(user_id), K(tenant_id), K(ret));
  return ret;
}

// force refresh schema if local schema version < last schema version
int ObMPBase::check_and_refresh_schema(
    uint64_t login_tenant_id, uint64_t effective_tenant_id, ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  int64_t local_version = 0;
  int64_t last_version = 0;

  if (login_tenant_id != effective_tenant_id) {
    // do nothing
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null schema service", K(ret), K(gctx_));
  } else {
    bool need_revert_session = false;
    if (NULL == session_info) {
      if (OB_FAIL(get_session(session_info))) {
        LOG_WARN("get session failed");
      } else if (OB_ISNULL(session_info)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid session info", K(ret), K(session_info));
      } else {
        need_revert_session = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(effective_tenant_id, local_version))) {
        LOG_WARN("fail to get tenant refreshed schema version", K(ret));
      } else if (OB_FAIL(session_info->get_ob_last_schema_version(last_version))) {
        LOG_WARN("failed to get_sys_variable", K(OB_SV_LAST_SCHEMA_VERSION));
      } else if (local_version >= last_version) {
        // skip
      } else if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(effective_tenant_id, last_version))) {
        LOG_WARN("failed to refresh schema", K(ret), K(effective_tenant_id), K(last_version));
      }
      if (need_revert_session && OB_LIKELY(NULL != session_info)) {
        revert_session(session_info);
      }
    }
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
