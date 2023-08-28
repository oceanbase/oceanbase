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
#include "lib/ob_name_id_def.h"
#include "lib/profile/ob_perf_event.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
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
    mysql_end_trans_cb_()
{
}

ObEndTransAsyncCallback::~ObEndTransAsyncCallback()
{
}

void ObEndTransAsyncCallback::callback(int cb_param, const transaction::ObTransID &trans_id)
{
  UNUSED(trans_id);
  callback(cb_param);
}

void ObEndTransAsyncCallback::callback(int cb_param)
{
  sql::ObSQLSessionInfo *session_info = mysql_end_trans_cb_.get_sess_info_ptr();
  // Add ASH flags to async commit of transactions
  // In the start of async commit in func named ` ObSqlTransControl::do_end_trans_() `,
  // set the ash flag named  `in_committing_` to true.
  if (NULL != session_info) {
    ObActiveSessionGuard::setup_ash(session_info->get_ash_stat());
    ObActiveSessionGuard::get_stat().in_committing_ = false;
    ObActiveSessionGuard::get_stat().in_sql_execution_ = true;
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

}/* ns sql*/
}/* ns oceanbase */
