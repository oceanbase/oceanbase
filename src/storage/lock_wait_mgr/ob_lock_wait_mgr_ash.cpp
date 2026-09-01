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

#include "ob_lock_wait_mgr.h"

#include "lib/stat/ob_diagnostic_info.h"
#include "rpc/ob_sql_request_operator.h"
#include "rpc/obmysql/obsm_struct.h"

#define USING_LOG_PREFIX TRANS
#include "share/ash/ob_active_sess_hist_list.h"

namespace oceanbase
{

using namespace common;
using namespace storage;

namespace lockwaitmgr
{

void ObLockWaitMgr::begin_row_lock_wait_event(const Node * const node)
{
  rpc::ObRequest* req = CONTAINER_OF((const rpc::ObLockWaitNode *)node, rpc::ObRequest, lock_wait_node_);
  if (OB_NOT_NULL(req)) {
    ObDiagnosticInfo *di = req->get_type() == rpc::ObRequest::OB_MYSQL
      ? reinterpret_cast<observer::ObSMConnection *>(SQL_REQ_OP.get_sql_session(req))->get_diagnostic_info()
      : req->get_diagnostic_info();
    if (OB_NOT_NULL(di)) {
      ObActiveSessionStat &ash_stat = di->get_ash_stat();
      ash_stat.begin_row_lock_wait_event();
      ash_stat.block_sessid_ = node->holder_sessid_;
    }
  }
}

void ObLockWaitMgr::end_row_lock_wait_event(const Node * const node)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    rpc::ObRequest* req = CONTAINER_OF((const rpc::ObLockWaitNode *)node, rpc::ObRequest, lock_wait_node_);
    if (OB_NOT_NULL(req)) {
      ObDiagnosticInfo *di = req->get_type() == rpc::ObRequest::OB_MYSQL
        ? reinterpret_cast<observer::ObSMConnection *>(SQL_REQ_OP.get_sql_session(req))->get_diagnostic_info()
        : req->get_diagnostic_info();
      if (OB_NOT_NULL(di)) {
        ObActiveSessionStat &ash_stat = di->get_ash_stat();
        ash_stat.end_row_lock_wait_event();
        ash_stat.block_sessid_ = 0;
      }
    }
  }
}

void ObLockWaitMgr::set_ash_rowlock_diag_info(const ObRowConflictInfo &cflict_info)
{
  int ret = OB_SUCCESS;
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(holder_tx_id_, cflict_info.conflict_tx_id_);
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(holder_data_seq_num_, cflict_info.conflict_tx_hold_seq_.get_seq());
  int64_t holder_lock_timestamp = calc_holder_tx_lock_timestamp(cflict_info.holder_tx_start_time_, cflict_info.conflict_tx_hold_seq_.get_seq());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(holder_lock_timestamp_, holder_lock_timestamp);
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObDiagnosticInfo *di = common::ObLocalDiagnosticInfo::get();
    uint64_t my_seq = 0;
    const char *diag_warn_msg = nullptr;
    if (OB_ISNULL(di)) {
      diag_warn_msg = "diagnostic info is null when setting row lock detail";
    } else {
      ObActiveSessionStat &ash_stat = di->get_ash_stat();
      share::ObLockWaitDetailBuffer &buf =
          share::ObActiveSessHistList::get_instance().get_lock_wait_detail_buffer();
      if (!buf.is_inited()) {
        diag_warn_msg = "lock wait detail buffer not inited";
      } else {
        my_seq = buf.alloc_detail_seq();
        share::ObLockWaitDetail *det = buf.get_entry(my_seq);
        if (OB_ISNULL(det)) {
          diag_warn_msg = "failed to alloc row lock wait detail";
        } else {
          ATOMIC_STORE(&det->alloc_seq_, 0);
          ATOMIC_STORE_REL(&det->holder_filled_seq_, 0);
          det->holder_sql_id_[0] = '\0';
          det->holder_query_sql_[0] = '\0';
          det->last_filled_holder_seq_ = INT64_MAX;
          const common::ObString rowkey = cflict_info.conflict_row_key_str_.get_ob_string();
          det->rowkey_len_ = static_cast<int32_t>(MIN(rowkey.length(), share::LOCK_DIAG_ROWKEY_MAX_LEN));
          if (det->rowkey_len_ > 0 && OB_NOT_NULL(rowkey.ptr())) {
            MEMCPY(det->rowkey_, rowkey.ptr(), det->rowkey_len_);
          } else {
            det->rowkey_len_ = 0;
          }
          ATOMIC_STORE_REL(&det->alloc_seq_, my_seq);
          ATOMIC_STORE(&ash_stat.lock_wait_detail_seq_, my_seq);
          LOG_TRACE("[LOCK_DIAG] alloc row lock wait detail", K(my_seq));
        }
      }
    }
    if (OB_NOT_NULL(diag_warn_msg)) {
      LOG_WARN("[LOCK_DIAG] set row lock wait detail failed",
               "msg", diag_warn_msg,
               K(my_seq),
               "holder_tx_id", cflict_info.conflict_tx_id_,
               "holder_tx_seq", cflict_info.conflict_tx_hold_seq_,
               "holder_seq", cflict_info.conflict_tx_hold_seq_.get_seq(),
               "waiter_tx_id", cflict_info.self_tx_id_);
    }
  }
  GET_DIAGNOSTIC_INFO->get_ash_stat().block_sessid_ = cflict_info.holder_sess_id_;
}

}; // end namespace lockwaitmgr
}; // end namespace oceanbase
