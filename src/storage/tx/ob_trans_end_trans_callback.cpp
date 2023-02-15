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
#include "lib/utility/utility.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{

using namespace common;

namespace transaction
{
void ObTxCommitCallback::reset()
{
  enable_ = false;
  inited_ = false;
  callback_count_ = 0;
  txs_ = NULL;
  tx_id_.reset();
  ret_ = OB_ERR_UNEXPECTED;
  commit_version_.reset();
}

int ObTxCommitCallback::callback()
{
  int ret = OB_SUCCESS;
  if (NULL == txs_ || !tx_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "", KR(ret), KPC(this));
  } else if (callback_count_ >= 1) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "more callback will be called", KPC(this));
  } else {
    ++callback_count_;
    txs_->handle_tx_commit_result(tx_id_, ret_, commit_version_);
  }
  return ret;
}

int ObTxCommitCallbackTask::make(const int64_t task_type,
                               const ObTxCommitCallback &cb,
                               const MonotonicTs receive_gts_ts,
                               const int64_t need_wait_interval_us)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransTask::make(task_type))) {
    TRANS_LOG(WARN, "ObTransTask make error", KR(ret), K(task_type));
  } else {
    cb_ = cb;
    trans_need_wait_wrap_.set_trans_need_wait_wrap(receive_gts_ts,
                                                   need_wait_interval_us);
  }
  return ret;
}

int ObTxCommitCallbackTask::callback(bool &has_cb)
{
  int ret = OB_SUCCESS;

  if (trans_need_wait_wrap_.need_wait()) {
    has_cb = false;
  } else {
    if (OB_FAIL(cb_.callback())) {
      TRANS_LOG(WARN, "callback error", KR(ret), K_(cb));
    }
    has_cb = true;
  }

  return ret;
}

void ObTxCommitCallbackTask::reset()
{
  cb_.reset();
  trans_need_wait_wrap_.reset();
}

int64_t ObTxCommitCallbackTask::get_need_wait_us() const
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

} // transaction
} // oceanbase
