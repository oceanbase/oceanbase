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
#include "storage/transaction/ob_trans_part_ctx.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::transaction;
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {
ObSharedEndTransCallback::ObSharedEndTransCallback()
{}

ObSharedEndTransCallback::~ObSharedEndTransCallback()
{}

ObExclusiveEndTransCallback::ObExclusiveEndTransCallback()
{
  reset();
}

ObExclusiveEndTransCallback::~ObExclusiveEndTransCallback()
{}

ObEndTransSyncCallback::ObEndTransSyncCallback() : ObExclusiveEndTransCallback(), queue_(), trans_desc_(NULL)
{}

ObEndTransSyncCallback::~ObEndTransSyncCallback()
{}

int ObEndTransSyncCallback::wait()
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  int retry = 0;

  if (NULL == trans_desc_) {
    SQL_LOG(ERROR, "transaction desc is null, unexpected error", KP_(trans_desc));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t trans_expired_time = trans_desc_->get_trans_expired_time();
    do {
      ret = queue_.wait(PUSH_POP_TIMEOUT, result);
      if (ObTimeUtility::current_time() > trans_expired_time + 1000 * 1000) {
        ++retry;
        // log every 1000s
        if (4 == retry || 0 == retry % 100) {
          SQL_LOG(ERROR,
              "ObEndTransSyncCallback wait() takes too long",
              K(retry),
              "trans_desc",
              *trans_desc_,
              K(lbt()),
              K(result),
              K_(call_counter),
              K_(callback_counter),
              K(ret));
        }
      }
    } while (OB_TIMEOUT == ret);

    if (OB_LIKELY(OB_SUCCESS == this->last_err_)) {
      ret = (OB_SUCCESS == ret) ? result : ret;
    } else {
      ret = this->last_err_;
    }

    // cleanup after transaction is committed
    if (NULL != session_ && trans_desc_->is_trx_level_temporary_table_involved()) {
      int temp_ret = session_->drop_temp_tables(false);
      if (OB_SUCCESS != temp_ret) {
        LOG_WARN("trx level temporary table clean failed", KR(temp_ret));
      }
    }
  }
  return ret;
}

void ObEndTransSyncCallback::callback(int cb_param, const transaction::ObTransID& trans_id)
{
  UNUSED(trans_id);
  callback(cb_param);
}

void ObEndTransSyncCallback::callback(int cb_param)
{
  this->handin();
  CHECK_BALANCE("[sync callback]");

  queue_.notify(cb_param);
}

int ObEndTransSyncCallback::init(const transaction::ObTransDesc* trans_desc, ObSQLSessionInfo* session)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_desc)) {
    SQL_LOG(WARN, "invalid argument", KP(trans_desc));
    ret = OB_INVALID_ARGUMENT;
  } else {
    queue_.reset();
    trans_desc_ = trans_desc;
    session_ = session;
  }
  return ret;
}

void ObEndTransSyncCallback::reset()
{
  queue_.reset();
  trans_desc_ = NULL;
  last_err_ = OB_SUCCESS;
}

/////////////////  Async Callback Impl /////////////

ObEndTransAsyncCallback::ObEndTransAsyncCallback() : ObExclusiveEndTransCallback(), mysql_end_trans_cb_()
{}

ObEndTransAsyncCallback::~ObEndTransAsyncCallback()
{}

void ObEndTransAsyncCallback::callback(int cb_param, const transaction::ObTransID& trans_id)
{
  UNUSED(trans_id);
  callback(cb_param);
}

void ObEndTransAsyncCallback::callback(int cb_param)
{
  bool need_disconnect = false;
  if (OB_UNLIKELY(!has_set_need_rollback_)) {
    LOG_ERROR("is_need_rollback_ has not been set", K(has_set_need_rollback_), K(is_need_rollback_));
  } else if (OB_UNLIKELY(ObExclusiveEndTransCallback::END_TRANS_TYPE_INVALID == end_trans_type_)) {
    LOG_ERROR("end trans type is invalid", K(cb_param), K(end_trans_type_));
  } else if (!is_txs_end_trans_called_) {
    // connection disconnect
    need_disconnect = true;
    LOG_WARN("fail before trans service end trans, disconnct", K(cb_param));
    if (OB_UNLIKELY(OB_SUCCESS == cb_param)) {
      LOG_ERROR("callback before trans service end trans, but ret is OB_SUCCESS, it is BUG!!!",
          K(cb_param),
          K(end_trans_type_),
          K(need_disconnect));
    }
  } else {
    ObSQLUtils::check_if_need_disconnect_after_end_trans(cb_param,
        is_need_rollback_,
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

int ObNullEndTransCallback::init(const transaction::ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (!trans_id.is_valid()) {
    SQL_LOG(WARN, "invalid argument", K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_id_ = trans_id;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
