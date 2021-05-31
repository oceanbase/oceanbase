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

#include "ob_trans_end_trans_callback.h"
#include "ob_trans_event.h"
#include <stdint.h>
#include <stdarg.h>
#include <algorithm>
#include "lib/profile/ob_perf_event.h"
#include "lib/utility/utility.h"

namespace oceanbase {

using namespace common;
using namespace sql;

namespace transaction {
int ObEndTransCallback::init(const uint64_t tenant_id, sql::ObIEndTransCallback* cb)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cb)) {
    TRANS_LOG(WARN, "invalid argument", KP(cb));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != cb_) {
    ret = OB_INIT_TWICE;
  } else {
    cb_ = cb;
    cb_type_ = cb->get_type();
    tenant_id_ = tenant_id;
  }

  return ret;
}

void ObEndTransCallback::reset()
{
  callback_count_ = 0;
  cb_ = NULL;
  cb_type_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObEndTransCallback::destroy()
{
  reset();
}

int ObEndTransCallback::callback(const int cb_param)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();

  if (NULL == cb_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "end trans callback is null", KR(ret), KP_(cb));
  } else if (callback_count_ >= 1) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "more callback will be called", KR(ret), K_(callback_count));
  } else {
    ++callback_count_;
    cb_->callback(cb_param);
    cb_ = NULL;
  }
  const int64_t end_us = ObTimeUtility::current_time();
  ObTransStatistic::get_instance().add_trans_callback_sql_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_trans_callback_sql_time(tenant_id_, end_us - start_us);

  return ret;
}

int64_t ObEndTransCallback::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf && buf_len > 0) {
    databuff_printf(buf,
        buf_len,
        pos,
        "[cb_count=%ld, cb=%p, cb_type=%s, tenant_id_=%ld]",
        callback_count_,
        cb_,
        cb_type_,
        tenant_id_);
  }
  return pos;
}

void EndTransCallbackTask::reset()
{
  ObTransTask::reset();
  end_trans_cb_.reset();
  param_ = 0;
  trans_need_wait_wrap_.reset();
}

int EndTransCallbackTask::make(const int64_t task_type, const ObEndTransCallback& end_trans_cb, const int param,
    const MonotonicTs receive_gts_ts, const int64_t need_wait_interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTransTask::make(task_type))) {
    TRANS_LOG(WARN, "ObTransTask make error", KR(ret), K(task_type));
  } else {
    end_trans_cb_ = end_trans_cb;
    param_ = param;
    trans_need_wait_wrap_.set_trans_need_wait_wrap(receive_gts_ts, need_wait_interval_us);
  }

  return ret;
}

int EndTransCallbackTask::callback(bool& has_cb)
{
  int ret = OB_SUCCESS;

  if (trans_need_wait_wrap_.need_wait()) {
    has_cb = false;
  } else {
    if (OB_FAIL(end_trans_cb_.callback(param_))) {
      TRANS_LOG(WARN, "callback error", KR(ret), K_(param));
    }
    has_cb = true;
  }

  return ret;
}

int64_t EndTransCallbackTask::get_need_wait_us() const
{
  int64_t ret = 0;
  int64_t remain_us = trans_need_wait_wrap_.get_remaining_wait_interval_us();
  if (remain_us > MAX_NEED_WAIT_US) {
    ret = MAX_NEED_WAIT_US;
  } else {
    ret = remain_us;
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
