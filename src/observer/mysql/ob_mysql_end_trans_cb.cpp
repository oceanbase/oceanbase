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

#include "ob_mysql_end_trans_cb.h"
#include "lib/allocator/ob_malloc.h"
#include "common/data_buffer.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_error.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/mysql/obmp_base.h"
#include "observer/mysql/obmp_utils.h"
#include "observer/mysql/obsm_struct.h"
#include "observer/mysql/ob_mysql_end_trans_cb.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
namespace oceanbase {
namespace observer {

ObSqlEndTransCb::ObSqlEndTransCb()
{
  reset();
}

ObSqlEndTransCb::~ObSqlEndTransCb()
{
  destroy();
}

void ObSqlEndTransCb::set_seq(uint8_t value)
{
  seq_ = value;
}

void ObSqlEndTransCb::set_conn_valid(bool value)
{
  conn_valid_ = value;
}

int ObSqlEndTransCb::set_packet_param(const sql::ObEndTransCbPacketParam& pkt_param)
{
  int ret = OB_SUCCESS;
  if (!pkt_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid copy", K(ret));
  } else {
    pkt_param_ = pkt_param;
  }
  return ret;
}

int ObSqlEndTransCb::init(rpc::ObRequest* req, sql::ObSQLSessionInfo* sess_info, uint8_t packet_seq, bool conn_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req) || OB_ISNULL(sess_info)) {
    ret = OB_ERR_NULL_VALUE;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(req), K(conn_status), K(sess_info), K(packet_seq));
  } else {
    // memory for ok eof error packet
    char* buf = static_cast<char*>(req->easy_reusable_alloc(OB_ONE_RESPONSE_BUF_SIZE + sizeof(easy_buf_t)));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      mysql_request_ = req;
      sess_info_ = sess_info;
      seq_ = packet_seq;
      conn_valid_ = conn_status;
      data_ = buf;
      ez_buf_ = reinterpret_cast<easy_buf_t*>(data_);
      init_easy_buf(ez_buf_, data_ + sizeof(easy_buf_t), req->get_ez_req(), OB_ONE_RESPONSE_BUF_SIZE);
    }
  }

  if (OB_SUCC(ret)) {
    ObSMConnection* conn = NULL;
    if (OB_FAIL(get_conn(req, conn))) {
      SERVER_LOG(WARN, "failed to get conn", K(ret));
    } else {
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
  }
  return ret;
}

int ObSqlEndTransCb::revert_session(ObSQLSessionInfo* sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info)) {
    LOG_WARN("sess_info is NULL", K(ret));
  } else {
    uint32_t sess_id = sess_info->get_sessid();
    uint64_t proxy_sessid = sess_info->get_proxy_sessid();
    if (OB_FAIL(GCTX.session_mgr_->revert_session(sess_info))) {
      LOG_ERROR("revert session fail", K(sess_id), K(proxy_sessid), K(ret));
    }
  }
  return ret;
}

void ObSqlEndTransCb::callback(int cb_param, const transaction::ObTransID& trans_id)
{
  UNUSED(trans_id);
  callback(cb_param);
}

// cb_param : the error code from SQL engine
void ObSqlEndTransCb::callback(int cb_param)
{
  int tmp_ret = OB_SUCCESS;
  uint32_t sessid = 0;
  uint64_t proxy_sessid = 0;
  sql::ObSQLSessionInfo* session_info = sess_info_;
  if (OB_ISNULL(session_info)) {
    tmp_ret = OB_ERR_NULL_VALUE;
    SERVER_LOG(ERROR, "session info is NULL", "ret", tmp_ret, K(session_info));
  } else {
    sql::ObSQLSessionInfo::LockGuard lock_guard(session_info->get_query_lock());
    session_info->reset_tx_variable();
    sessid = session_info->get_sessid();
    proxy_sessid = session_info->get_proxy_sessid();
    if (OB_UNLIKELY(!pkt_param_.is_valid())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "pkt_param_ is invalid", K(tmp_ret), K(pkt_param_));
    } else if (FALSE_IT(ObCurTraceId::set(pkt_param_.get_trace_id()))) {
      // do nothing
    } else if (!conn_valid_) {
      // network problem, callback will still be called
      tmp_ret = OB_CONNECT_ERROR;
      SERVER_LOG(INFO, "connection is invalid", "ret", tmp_ret);
    } else if (OB_ISNULL(mysql_request_) || OB_ISNULL(mysql_request_->get_ez_req()) || OB_ISNULL(ez_buf_) ||
               OB_ISNULL(session_info)) {
      tmp_ret = OB_ERR_NULL_VALUE;
      SERVER_LOG(ERROR, "invalid pointer", KP_(mysql_request), KP(session_info), KP_(ez_buf));
    } else {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
      session_info->set_show_warnings_buf(cb_param);
      if (OB_SUCCESS == cb_param) {
        // ok pakcet
        ObOKPParam ok_param;
        ok_param.message_ = const_cast<char*>(pkt_param_.get_message());
        ok_param.affected_rows_ = pkt_param_.get_affected_rows();
        ok_param.lii_ = pkt_param_.get_last_insert_id_to_client();
        ok_param.warnings_count_ =
            static_cast<uint16_t>(session_info->get_warnings_buffer().get_readable_warning_count());
        ok_param.is_partition_hit_ = pkt_param_.get_is_partition_hit();
        if (OB_SUCCESS != (tmp_ret = encode_ok_packet(mysql_request_, ez_buf_, ok_param, *session_info, seq_++))) {
          SERVER_LOG(WARN, "encode ok packet fail", K(ok_param), "ret", tmp_ret);
        }
      } else {
        // error + possible ok packet
        const char* error_msg = session_info->get_warnings_buffer().get_err_msg();
        if (OB_SUCCESS != (tmp_ret = encode_error_packet(mysql_request_,
                               ez_buf_,
                               *session_info,
                               seq_++,
                               cb_param,
                               error_msg,
                               pkt_param_.get_is_partition_hit()))) {
          SERVER_LOG(WARN, "encode error packet fail", "ret", tmp_ret);
        } else {
          observer::ObSMConnection* conn = NULL;
          if (OB_SUCCESS != (tmp_ret = get_conn(mysql_request_, conn))) {
            SERVER_LOG(WARN, "failed to get conn", "ret", tmp_ret);
          } else {
            if (conn->need_send_extra_ok_packet()) {
              // for obproxy or OCJ, followed by another OK packet
              ObOKPParam ok_param;
              if (OB_SUCCESS !=
                  (tmp_ret = encode_ok_packet(mysql_request_, ez_buf_, ok_param, *session_info, seq_++))) {
                SERVER_LOG(WARN, "encode ok packet fail", K(ok_param), "ret", tmp_ret);
              }
            }
          }
        }
      }
      // succ or not reset warning buffer
      session_info->reset_warnings_buf();
    }

    if (OB_SUCCESS == tmp_ret) {
      // after flush mysql_request_ cannot be visited
      if (conn_valid_) {
        if (need_disconnect_) {
          // needs activly disconnect
          mysql_request_->disconnect();
          SERVER_LOG(WARN,
              "server close connection",
              "ret",
              tmp_ret,
              K(sessid),
              K(proxy_sessid),
              K(cb_param),
              K(session_info->get_trans_desc()));
        }
        if (NULL != ez_buf_) {
          const bool is_last = true;
          bool req_has_wokenup = false;
          bool io_thread_mark = false;
          ObFlushBufferParam flush_param(*ez_buf_,
              *(mysql_request_->get_ez_req()),
              comp_context_,
              conn_valid_,
              req_has_wokenup,
              io_thread_mark,
              is_last);
          if (OB_SUCCESS != (tmp_ret = ObMySQLRequestUtils::flush_buffer(flush_param))) {
            SERVER_LOG(
                WARN, "failed to flush buffer in callback function", K(tmp_ret), K_(need_disconnect), K_(conn_valid));
            if (conn_valid_) {
              need_disconnect_ = false;
              conn_valid_ = false;
            }
          }
        } else {
          SERVER_LOG(WARN, "ez_buf_ is NULL", K_(conn_valid), KP_(mysql_request));
        }
      } else {
        SERVER_LOG(WARN, "connection is invalid", K_(conn_valid), KP_(ez_buf));
      }
    } else {
      if (conn_valid_ && NULL != mysql_request_ && NULL != mysql_request_->get_ez_req()) {
        mysql_request_->disconnect();
        bool req_has_wokenup = false;
        ObMySQLRequestUtils::wakeup_easy_request(*mysql_request_->get_ez_req(), req_has_wokenup);
        SERVER_LOG(WARN, "cannot response packet, close connection", "ret", tmp_ret);
      } else {
        SERVER_LOG(WARN, "connection is invalid", K_(conn_valid), KP_(mysql_request), "ret", tmp_ret);
      }
    }

    conn_valid_ = false;
    ob_setup_tsi_warning_buffer(NULL);
    pkt_param_.reset();
    need_disconnect_ = false;
    mysql_request_ = NULL;
    ez_buf_ = NULL;
    sess_info_ = NULL;
    destroy();
  } /* end query_lock protection */

  if (NULL != session_info) {
    MEM_BARRIER();
    int sret = revert_session(session_info);
    if (OB_SUCCESS != sret) {
      SERVER_LOG(ERROR, "revert session fail", K(sessid), K(proxy_sessid), K(sret), "ret", tmp_ret, K(lbt()));
    }
  }
}

void ObSqlEndTransCb::destroy()
{
  data_ = NULL;  // mem from easy
  ez_buf_ = NULL;
}

void ObSqlEndTransCb::reset()
{
  data_ = NULL;
  mysql_request_ = NULL;
  sess_info_ = NULL;
  pkt_param_.reset();
  warning_count_ = 0;
  seq_ = 0;
  comp_context_.reset();
  proto20_context_.reset();
  ez_buf_ = NULL;
  conn_valid_ = true;
  need_disconnect_ = false;
  sessid_ = 0;
}

int ObSqlEndTransCb::encode_ok_packet(rpc::ObRequest* req, easy_buf_t* ez_buf, observer::ObOKPParam& ok_param,
    sql::ObSQLSessionInfo& sess_info, uint8_t packet_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == req || NULL == ez_buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(req), K(ez_buf), K(ok_param), K(packet_seq));
  } else if (OB_UNLIKELY(!conn_valid_)) {
    ret = OB_CONNECT_ERROR;
    SERVER_LOG(WARN, "conn is invalid", K(ret));
  } else {
    ObSMConnection* conn = reinterpret_cast<ObSMConnection*>(req->get_session());
    // make an okpacket
    OMPKOK okp;
    okp.set_affected_rows(ok_param.affected_rows_);
    if (NULL != ok_param.message_) {
      ObString message_str;
      message_str.assign_ptr(ok_param.message_, static_cast<int32_t>(strlen(ok_param.message_)));
      okp.set_message(message_str);
    }
    okp.set_capability(sess_info.get_capability());
    okp.set_last_insert_id(ok_param.lii_);
    okp.set_warnings(ok_param.warnings_count_);
    // set is_partition_hit
    if (!ok_param.is_partition_hit_) {
      ret = sess_info.set_partition_hit(false);
    }

    // set is_trans_specfied
    if (OB_SUCC(ret)) {
      ret = sess_info.save_trans_status();
    }
    if (OB_SUCC(ret)) {
      if (sess_info.is_track_session_info()) {
        if (conn->is_normal_client()) {
          okp.set_use_standard_serialize(true);
          if (OB_FAIL(ObMPUtils::add_nls_format(okp, sess_info, !ok_param.is_on_connect_))) {
            LOG_WARN("fail to add_nls_format", K(ret));
          }
        } else {
          // in hand shake response's ok packet, return all session variable
          if (ok_param.is_on_connect_) {
            if (OB_FAIL(ObMPUtils::add_session_info_on_connect(okp, sess_info))) {
              SERVER_LOG(WARN, "fail to add all session system variables", K(ret));
            }
          } else {
            if (OB_FAIL(ObMPUtils::add_changed_session_info(okp, sess_info))) {
              SERVER_LOG(WARN, "fail to add changed session info", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool ac = true;
      if (OB_FAIL(sess_info.get_autocommit(ac))) {
        LOG_WARN("fail to get autocommit", K(ret));
      } else {
        ObServerStatusFlags flags = okp.get_server_status();
        flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = (sess_info.is_server_status_in_transaction() ? 1 : 0);
        flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
        if (!sess_info.is_obproxy_mode()) {
          // in java client or others, use slow query bit to indicate partition hit or not
          flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !ok_param.is_partition_hit_;
        }
        // todo: set OB_SERVER_STATUS_IN_TRANS_READONLY flag if need?
        okp.set_server_status(flags);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(encode_packet(okp, packet_seq, true))) {
        SERVER_LOG(WARN, "encode ok packet fail", K(ret));
      }
    }
    if (sess_info.is_track_session_info()) {
      sess_info.reset_session_changed_info();
    }
  }
  return ret;
}

int ObSqlEndTransCb::encode_error_packet(rpc::ObRequest* req, easy_buf_t* ez_buf, sql::ObSQLSessionInfo& sess_info,
    uint8_t packet_seq, int err, const char* errmsg, bool is_partition_hit)
{
  int ret = OB_SUCCESS;
  UNUSED(is_partition_hit);
  UNUSED(sess_info);
  if (NULL == req || NULL == ez_buf || OB_SUCCESS == err) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(req), K(ez_buf), K(packet_seq), K(err));
  } else if (!conn_valid_) {
    ret = OB_CONNECT_ERROR;
    SERVER_LOG(WARN, "connection already disconnected", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObString message;
    const int32_t MAX_MSG_BUF_SIZE = 512;
    char msg_buf[MAX_MSG_BUF_SIZE];
    if (NULL != errmsg && 0 < strlen(errmsg)) {
      message = ObString::make_string(errmsg);
    } else {
      snprintf(msg_buf, MAX_MSG_BUF_SIZE, "%s", ob_errpkt_strerror(err, lib::is_oracle_mode()));
      message = ObString::make_string(msg_buf);  // default error message
    }

    OMPKError epacket;
    epacket.set_errcode(static_cast<uint16_t>(ob_errpkt_errno(err, lib::is_oracle_mode())));
    ret = epacket.set_sqlstate(ob_sqlstate(err));
    if (OB_SUCC(ret) && OB_SUCC(epacket.set_message(message))) {
      // error packet must follow by an ok packet
      if (OB_FAIL(encode_packet(epacket, packet_seq, false))) {
        SERVER_LOG(WARN, "failed to encdoe error packet", K(ret), K(epacket));
      }
    } else {
      SERVER_LOG(WARN, "failed to set error info", K(ret), K(err), K(errmsg));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int ObSqlEndTransCb::get_conn(rpc::ObRequest* req, ObSMConnection*& conn) const
{
  int ret = OB_SUCCESS;
  if (NULL == req || NULL == req->get_session()) {
    ret = OB_ERR_NULL_VALUE;
    conn = NULL;
    SERVER_LOG(WARN, "invalid null value", K(ret));
  } else if (!conn_valid_) {
    ret = OB_CONNECT_ERROR;
    conn = NULL;
    SERVER_LOG(WARN, "invalid conn info", K_(conn_valid), K(ret));
  } else {
    conn = reinterpret_cast<ObSMConnection*>(req->get_session());
  }
  return ret;
}

inline int ObSqlEndTransCb::encode_packet(ObMySQLPacket& pkt, const uint8_t packet_seq, const bool is_last)
{
  INIT_SUCC(ret);

  // packet_seq is critical, when set wrong, client may disconnect
  pkt.set_seq(packet_seq);

  ObProtoEncodeParam param;
  if (FALSE_IT(ObProtoEncodeParam::build_param(param, &pkt, *ez_buf_, sessid_, is_last, proto20_context_))) {
    // impossible
  } else if (OB_FAIL(ObProto20Utils::do_packet_encode(param))) {
    LOG_WARN("fail to do packet encode", K(param), K(ret));
  } else if (param.need_flush_) {  // this means buf not enough
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer is no enough", K(ret));
  } else if (OB_FAIL(param.encode_ret_)) {
    ret = param.encode_ret_;
    LOG_WARN("fail to do packet encode", K(ret));
  } else {
    if (is_last) {  // last mysql packet, fill proto20 header and tailer
      if (proto20_context_.is_proto20_used() && (FILL_PAYLOAD_STEP == proto20_context_.next_step_)) {
        ObProtoEncodeParam param2;
        if (FALSE_IT(ObProtoEncodeParam::build_param(param2, NULL, *ez_buf_, sessid_, is_last, proto20_context_))) {
          LOG_ERROR("fail to build encode param", K(ret));
        } else if (OB_FAIL(ObProto20Utils::fill_proto20_header_and_tailer(param))) {
          LOG_ERROR("fail to fill ob20 protocol header and tailer", K(ret));
        }
      }
    } else {
      // continue to encode
    }
  }

  return ret;
}

}  // namespace observer
}  // end of namespace oceanbase
