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

#define USING_LOG_PREFIX SQL

#include "ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#ifdef OB_HOTSPOT_GROUP_COMMIT
#include "sql/ob_sql_group_commit_struct.h"
#endif


using namespace oceanbase::transaction;
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
ObSharedEndTransCallback::ObSharedEndTransCallback()
{
}

ObSharedEndTransCallback::~ObSharedEndTransCallback()
{
}

ObExclusiveEndTransCallback::ObExclusiveEndTransCallback()
{
  reset();
}

ObExclusiveEndTransCallback::~ObExclusiveEndTransCallback()
{
}

/////////////////  Async Callback Impl /////////////

ObEndTransAsyncCallback::ObEndTransAsyncCallback() :
    ObExclusiveEndTransCallback(),
    mysql_end_trans_cb_(),
    diagnostic_info_(nullptr)
{
}

ObEndTransAsyncCallback::~ObEndTransAsyncCallback()
{
  reset_diagnostic_info();
}

void ObEndTransAsyncCallback::callback(int cb_param, const transaction::ObTransID &trans_id)
{
  UNUSED(trans_id);
  callback(cb_param);
}

void ObEndTransAsyncCallback::callback(int cb_param)
{
  // Add ASH flags to async commit of transactions
  // In the start of async commit in func named ` ObSqlTransControl::do_end_trans_() `,
  // set the ash flag named  `in_committing_` to true.
  ObDiagnosticInfoSwitchGuard g(diagnostic_info_);
  if (OB_NOT_NULL(diagnostic_info_)) {
    common::ObDiagnosticInfo *di = diagnostic_info_;
    reset_diagnostic_info();
    di->get_ash_stat().in_committing_ = false;
    di->get_ash_stat().in_sql_execution_ = true;
    di->end_wait_event(ObWaitEventIds::ASYNC_COMMITTING_WAIT, false);
  }
  bool need_disconnect = false;
  if (OB_UNLIKELY(!has_set_need_rollback_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "is_need_rollback_ has not been set",
              K(has_set_need_rollback_),
              K(is_need_rollback_));
  } else if (OB_UNLIKELY(ObExclusiveEndTransCallback::END_TRANS_TYPE_INVALID == end_trans_type_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "end trans type is invalid", K(cb_param), K(end_trans_type_));
  } else {
    ObSQLUtils::check_if_need_disconnect_after_end_trans(
        cb_param, is_need_rollback_,
        ObExclusiveEndTransCallback::END_TRANS_TYPE_EXPLICIT == end_trans_type_,
        need_disconnect);
  }
  mysql_end_trans_cb_.set_need_disconnect(need_disconnect);
  this->handin();
  CHECK_BALANCE("[async callback]");

  if (OB_SUCCESS == this->last_err_) {
    mysql_end_trans_cb_.callback(cb_param);
  } else {
    cb_param = this->last_err_;
    mysql_end_trans_cb_.callback(cb_param);
  }
}

void ObEndTransAsyncCallback::set_diagnostic_info(common::ObDiagnosticInfo *diagnostic_info)
{
  if (nullptr == diagnostic_info_) {
    diagnostic_info_ = diagnostic_info;
    common::ObLocalDiagnosticInfo::inc_ref(diagnostic_info_);
  }

};
void ObEndTransAsyncCallback::reset_diagnostic_info()
{
  if (nullptr != diagnostic_info_) {
    common::ObLocalDiagnosticInfo::dec_ref(diagnostic_info_);
    diagnostic_info_ = nullptr;
  }
}

int ObEndTransAsyncCallback::inc_session_ref(ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session mgr is null", K(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->inc_session_ref(sess_info))) {
    LOG_WARN("fail to inc session ref", K(ret));
#ifdef OB_HOTSPOT_GROUP_COMMIT
  } else if (mysql_end_trans_cb_.get_packet_sender().get_req()->has_group_commit_agg_info()) {
    ObGroupCommitAggInfo *agg_info = mysql_end_trans_cb_.get_packet_sender().get_req()->get_group_commit_agg_info();
    ObGroupCommitSubRequest *head = reinterpret_cast<ObGroupCommitSubRequest *>(agg_info->get_head_sub_request()->next_);
    ObGroupCommitSubRequest *cur = head;
    while (OB_SUCC(ret) && cur != nullptr) {
      // If a sub-transaction has not started, its end transaction will be skipped,
      // and the callback for the sub-transaction will be invoked within the primary transaction's callback.
      // Therefore, we need to increase the session reference for the sub-transaction here,
      // to prevent its session from being released too early.
      if (!cur->is_trans_started_ && OB_FAIL(GCTX.session_mgr_->inc_session_ref(cur->sess_))) {
        LOG_WARN("fail to inc session ref", K(ret));
      } else {
        cur = reinterpret_cast<ObGroupCommitSubRequest *>(cur->next_);
      }
    }

    if (OB_FAIL(ret)) {
      ObGroupCommitSubRequest *revert_cur = head;
      while (revert_cur != cur) {
        if (!revert_cur->is_trans_started_) {
          GCTX.session_mgr_->revert_session(revert_cur->sess_);
        }
        revert_cur = reinterpret_cast<ObGroupCommitSubRequest *>(revert_cur->next_);
      }
    }
#endif
  }
  return ret;
}

int ObEndTransAsyncCallback::dec_session_ref(ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session mgr is null", K(ret));
  } else {
    GCTX.session_mgr_->revert_session(sess_info);
#ifdef OB_HOTSPOT_GROUP_COMMIT
    if (mysql_end_trans_cb_.get_packet_sender().get_req()->has_group_commit_agg_info()) {
      ObGroupCommitAggInfo *agg_info = mysql_end_trans_cb_.get_packet_sender().get_req()->get_group_commit_agg_info();
      ObGroupCommitSubRequest *cur = reinterpret_cast<ObGroupCommitSubRequest *>(agg_info->get_head_sub_request()->next_);
      while (nullptr != cur) {
        // see comments in inc_session_ref()
        if (!cur->is_trans_started_) {
          GCTX.session_mgr_->revert_session(cur->sess_);
        }
        cur = reinterpret_cast<ObGroupCommitSubRequest *>(cur->next_);
      }
    }
#endif
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
