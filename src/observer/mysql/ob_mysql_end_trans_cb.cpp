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
#include "rpc/obmysql/obsm_struct.h"
#include "observer/mysql/ob_mysql_end_trans_cb.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "obmp_stmt_send_piece_data.h"
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
namespace oceanbase
{
namespace observer
{

ObSqlEndTransCb::ObSqlEndTransCb()
{
  reset();
}

ObSqlEndTransCb::~ObSqlEndTransCb()
{
  destroy();
}

int ObSqlEndTransCb::set_packet_param(const sql::ObEndTransCbPacketParam &pkt_param)
{
  int ret = OB_SUCCESS;
  if (!pkt_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid copy", K(ret));
  } else {
    pkt_param_ = pkt_param; //! 拷贝语义
  }
  return ret;
}

int ObSqlEndTransCb::init(ObMPPacketSender& packet_sender, 
                          sql::ObSQLSessionInfo *sess_info, 
                          int32_t stmt_id,
                          uint64_t params_num,
                          int64_t com_offset)
{
  sess_info_ = sess_info;
  stmt_id_ = stmt_id;
  params_num_ = params_num;
  return packet_sender_.clone_from(packet_sender, com_offset);
}

void ObSqlEndTransCb::callback(int cb_param, const transaction::ObTransID &trans_id)
{
  UNUSED(trans_id);
  callback(cb_param);
}

//cb_param : the error code from SQL engine
void ObSqlEndTransCb::callback(int cb_param)
{
  int ret = OB_SUCCESS;
  uint32_t sessid = 0;
  uint64_t proxy_sessid = 0;
  sql::ObSQLSessionInfo *session_info = sess_info_;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_NULL_VALUE;
    SERVER_LOG(ERROR, "session info is NULL", "ret", ret, K(session_info));
  } else {
    sql::ObSQLSessionInfo::LockGuard lock_guard(session_info->get_query_lock());
    bool reuse_tx = OB_SUCCESS == cb_param
      || OB_TRANS_COMMITED == cb_param
      || OB_TRANS_ROLLBACKED == cb_param;
      ObSqlTransControl::reset_session_tx_state(session_info, reuse_tx);
    sessid = session_info->get_sessid();
    proxy_sessid = session_info->get_proxy_sessid();
    // 临界区内检查这些变量，预防并发callback造成的不良影响
    if (OB_UNLIKELY(!pkt_param_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "pkt_param_ is invalid", K(ret), K(pkt_param_));
    } else if (FALSE_IT(ObCurTraceId::set(pkt_param_.get_trace_id()))) { // 尽早设置trace_id
      //do nothing
    } else if (!packet_sender_.is_conn_valid()) {
      //network problem, callback will still be called
      ret = OB_CONNECT_ERROR;
      SERVER_LOG(INFO, "connection is invalid", "ret", ret);
    } else if (OB_SUCCESS != packet_sender_.alloc_ezbuf()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc easy buf");
    } else if (OB_SUCCESS != packet_sender_.update_last_pkt_pos()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to update last packet pos");
    } else {
      session_info->set_show_warnings_buf(cb_param);
      if (OB_SUCCESS == cb_param) {
        //ok pakcet
        ObOKPParam ok_param;
        ok_param.message_ = const_cast<char*>(pkt_param_.get_message());
        ok_param.affected_rows_ = pkt_param_.get_affected_rows();
        ok_param.lii_ = pkt_param_.get_last_insert_id_to_client();
        ok_param.warnings_count_ = static_cast<uint16_t>(session_info->get_warnings_buffer().get_readable_warning_count());
        ok_param.is_partition_hit_ = pkt_param_.get_is_partition_hit();
        if (OB_SUCCESS != (ret = packet_sender_.send_ok_packet(*session_info, ok_param))) {
          SERVER_LOG(WARN, "encode ok packet fail", K(ok_param), "ret", ret);
        }
      } else {
        //error + possible ok packet
        const char *error_msg = session_info->get_warnings_buffer().get_err_msg();
        if (OB_SUCCESS != (ret = packet_sender_.send_error_packet(cb_param, error_msg, pkt_param_.get_is_partition_hit()))) {
          SERVER_LOG(WARN, "encode error packet fail", "ret", ret);
        }
      }
      //succ or not reset warning buffer
      session_info->reset_warnings_buf();
    }

    ObPieceCache *piece_cache = session_info->get_piece_cache();
    if (OB_ISNULL(piece_cache)) {
      // do nothing
      // piece_cache not be null in piece data protocol
    } else {
      int piece_ret = OB_SUCCESS;
      for (uint64_t i = 0; OB_SUCCESS == piece_ret && i < params_num_; i++) {
        piece_ret = piece_cache->remove_piece(
                            piece_cache->get_piece_key(stmt_id_, i),
                            *session_info);
        if (OB_SUCCESS != piece_ret) {
          if (OB_HASH_NOT_EXIST == piece_ret) {
            piece_ret = OB_SUCCESS;
          } else {
            LOG_WARN("remove piece fail", K(stmt_id_), K(i), K(piece_ret));
          }
        }
      }
    }

    ObActiveSessionGuard::get_stat().in_sql_execution_ = false;
    ObActiveSessionGuard::setup_default_ash();
    session_info->set_session_sleep();
    if (OB_SUCCESS == ret) {
      if (need_disconnect_) {
        packet_sender_.force_disconnect();
      }
      const bool is_last = true;
      packet_sender_.flush_buffer(is_last);
    } else {
      packet_sender_.force_disconnect();
      packet_sender_.finish_sql_request();
    }


    ob_setup_tsi_warning_buffer(NULL);
    pkt_param_.reset(); // 过期作废，再次调callback的时候必须重新设置参数
    need_disconnect_ = false;
    sess_info_ = NULL;
    packet_sender_.reset();
    destroy();
  } /* end query_lock protection */

  if (NULL != session_info) {
    MEM_BARRIER();
    int sret = packet_sender_.revert_session(session_info);
    if (OB_SUCCESS != sret) {
      SERVER_LOG_RET(ERROR, sret, "revert session fail", K(sessid), K(proxy_sessid), K(sret), "ret", ret, K(lbt()));
    }
  }
}

void ObSqlEndTransCb::destroy()
{
}

void ObSqlEndTransCb::reset()
{
  packet_sender_.reset();
  sess_info_ = NULL;
  pkt_param_.reset();
  need_disconnect_ = false;
  stmt_id_ = 0;
  params_num_ = 0;
}

} // end of namespace obmysql
} // end of namespace oceanbase
