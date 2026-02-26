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
#include "obmp_stmt_send_piece_data.h"
#include "pl/ob_pl.h"
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
  need_disconnect_ = false;
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
    sessid = session_info->get_server_sid();
    proxy_sessid = session_info->get_proxy_sessid();
    send_response_packet(cb_param, session_info);
    clear_session_state(session_info);
    reset();
  } /* end query_lock protection */
  if (NULL != session_info) {
    MEM_BARRIER();
    int sret = packet_sender_.revert_session(session_info);
    if (OB_SUCCESS != sret) {
      SERVER_LOG_RET(ERROR, sret, "revert session fail", K(sessid), K(proxy_sessid), K(sret), "ret", ret, K(lbt()));
    }
  }
}

int ObSqlEndTransCb::clear_session_state(sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_NULL_VALUE;
    SERVER_LOG(ERROR, "session_info is NULL", K(ret), K(session_info));
  } else {
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

    GET_DIAGNOSTIC_INFO->get_ash_stat().in_sql_execution_ = false;
    session_info->reset_cur_sql_id();
    session_info->reset_current_plan_hash();
    session_info->reset_current_plan_id();
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
  }
  return ret;
}

int ObSqlEndTransCb::send_response_packet(int cb_param, sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  //需要在session_info->get_query_lock()临界区内检查这些变量，预防并发callback造成的不良影响 ！！！
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
      ret = send_ok_packet(session_info);
    } else {
      ret = send_error_packet(cb_param, session_info);
    }
    //succ or not reset warning buffer
    session_info->reset_warnings_buf();
  }
  return ret;
}

int ObSqlEndTransCb::send_ok_packet(sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObOKPParam ok_param;
  ok_param.message_ = const_cast<char*>(pkt_param_.get_message());
  ok_param.affected_rows_ = pkt_param_.get_affected_rows();
  ok_param.lii_ = pkt_param_.get_last_insert_id_to_client();
  ok_param.warnings_count_ = static_cast<uint16_t>(session_info->get_warnings_buffer().get_readable_warning_count());
  ok_param.is_partition_hit_ = pkt_param_.get_is_partition_hit();

  if (OB_SUCCESS != (ret = packet_sender_.send_ok_packet(*session_info, ok_param))) {
    SERVER_LOG(WARN, "encode ok packet fail", K(ok_param), "ret", ret);
  }
  return ret;
}

int ObSqlEndTransCb::send_error_packet(int cb_param, sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  const char *error_msg = session_info->get_warnings_buffer().get_err_msg();
  if (OB_SUCCESS != (ret = packet_sender_.send_error_packet(cb_param, error_msg, pkt_param_.get_is_partition_hit()))) {
    SERVER_LOG(WARN, "encode error packet fail", "ret", ret);
  }
  return ret;
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

void ObPLEndTransCb::callback(int cb_param)
{
  int ret = OB_SUCCESS;
  transaction::ObTxDesc *tx_desc = get_tx_desc();
  sql::ObSQLSessionInfo *session_info = sess_info_;
  int64_t tx_id = 0;
  set_err(cb_param);
  bool need_response_packet = get_need_response_packet();
  if (OB_ISNULL(tx_desc) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PL_ASYNC_COMMIT] tx_desc or session_info is NULL!");
  } else {
    tx_id = tx_desc->get_tx_id();
    uint64_t effect_tid = session_info->get_effective_tenant_id();
    MTL_SWITCH(effect_tid) {
      transaction::ObTransService *txs = NULL;
      if (OB_ISNULL(txs = MTL_WITH_CHECK_TENANT(transaction::ObTransService*, effect_tid))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get_tx_service", K(ret), K(effect_tid), K(MTL_ID()));
      }
      OZ (txs->release_tx(*tx_desc), tx_id);
    }
    set_tx_desc(NULL);
  }
  if (OB_SUCCESS != ret) {
    if (OB_SUCCESS == cb_param) {
      set_err(ret);
    }
    LOG_WARN("[PL_ASYNC_COMMIT] Failed to release tx_desc", K(ret));
  }
  if (need_response_packet) {
    sql::ObSQLSessionInfo::LockGuard lock_guard(session_info->get_query_lock());
    send_response_packet(get_err(), session_info);
    clear_session_state(session_info);
    destroy();
  }
  LOG_TRACE("[PL_ASYNC_COMMIT] async callback completed", K(ret), K(tx_id), K(cb_param), K(need_response_packet));
  if (NULL != session_info) {
    MEM_BARRIER();
    int sret = packet_sender_.revert_session(session_info);
    if (OB_SUCCESS != sret) {
      SERVER_LOG_RET(ERROR, sret, "[PL_ASYNC_COMMIT] Revert session fail", K(ret), K(sret), K(lbt()));
    }
  }
}

void ObPLEndTransCb::callback(int cb_param, const transaction::ObTransID &trans_id)
{
  UNUSED(trans_id);
  callback(cb_param);
}

int ObPLEndTransCb::init(ObMPPacketSender& packet_sender,
                         sql::ObSQLSessionInfo *sess_info,
                         int32_t stmt_id,
                         uint64_t params_num,
                         int64_t com_offset)
{
  int ret = OB_SUCCESS;
  tx_desc_ = NULL;
  err_ = OB_SUCCESS;
  need_response_packet_ = false;
  OZ (ObSqlEndTransCb::init(packet_sender, sess_info, stmt_id, params_num, com_offset));
  return ret;
}

int ObPLEndTransCb::wait_tx_end(sql::ObPhysicalPlanCtx *plan_ctx, bool force_wait)
{
  int ret = OB_SUCCESS;
  int64_t elapsed_us = 0;
  int wait_us = 3;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(get_tx_desc())) {
    OZ (get_err());
    reset();
  } else if (OB_NOT_NULL(get_tx_desc())) {
    int64_t timeout_us = 0;
    OZ (sess_info_->get_tx_timeout(timeout_us));
    while (OB_NOT_NULL(get_tx_desc())) {
      if (force_wait) {
        // do nothing
      } else {
        elapsed_us = ObTimeUtility::current_time() - start_time;
        if (elapsed_us >= timeout_us) {
          ret = OB_TRANS_TIMEOUT;
          LOG_WARN("[PL_ASYNC_COMMIT] Wait pl async commit timeout", K(ret), K(elapsed_us), K(timeout_us));
          break;
        }
      }
      ob_usleep(wait_us);
    }
    ObSpinLockGuard lock_guard(lock_);
    elapsed_us = ObTimeUtility::current_time() - start_time;
    if (OB_SUCC(ret) && OB_NOT_NULL(plan_ctx)) {
      int64_t current_timeout_ts = plan_ctx->get_timeout_timestamp();
      int64_t modified_timeout_ts = current_timeout_ts + elapsed_us;
      plan_ctx->set_timeout_timestamp(modified_timeout_ts);
    }
    OZ (get_err());
    reset();
  }
  return ret;
}

int ObPLEndTransCb::check_dependency_has_modified_tables(const sql::ObPhysicalPlan *plan, sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  bool need_wait = false;
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PL_ASYNC_COMMIT] Plan is NULL", K(ret));
  } else {
    ObSpinLockGuard lock_guard(lock_);
    if (OB_NOT_NULL(get_tx_desc())) {
      if (plan->has_uncertain_local_operator()) {
        need_wait = true;
      } else {
        transaction::ObTxDesc *tx_desc = get_tx_desc();
        const DependenyTableStore &tables = plan->get_dependency_table();
        ARRAY_FOREACH(tables, i) {
          if (tables.at(i).is_base_table()) {
            if (tx_desc->has_modify_table((uint64_t)tables.at(i).get_object_id())) {
              LOG_TRACE("[PL_ASYNC_COMMIT] Dependency has modified table with preceding transaction", K(tables.at(i).get_object_id()));
              need_wait = true;
              break;
            }
          }
        }
      }
    } else {
      // do nothing
    }
  }
  if (need_wait) {
    OZ (wait_tx_end());
  }
  return ret;
}

void ObPLEndTransCb::destroy()
{
  reset();
  ObSqlEndTransCb::reset();
}

void ObPLEndTransCb::reset()
{
  tx_desc_ = NULL;
  err_ = OB_SUCCESS;
  need_response_packet_ = false;
  need_disconnect_ = false;
}

} // end of namespace obmysql
} // end of namespace oceanbase
