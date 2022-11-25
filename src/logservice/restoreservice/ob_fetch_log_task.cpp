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

#include "ob_fetch_log_task.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_ls_id.h"
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
ObFetchLogTask::ObFetchLogTask(const share::ObLSID &id,
    const int64_t pre_log_ts,
    const palf::LSN &lsn,
    const int64_t size,
    const int64_t proposal_id) :
  id_(id),
  proposal_id_(proposal_id),
  pre_log_ts_(pre_log_ts),
  start_lsn_(lsn),
  cur_lsn_(lsn),
  end_lsn_(lsn + size),
  max_fetch_log_ts_(OB_INVALID_TIMESTAMP),
  max_submit_log_ts_(OB_INVALID_TIMESTAMP),
  status_(Status::NORMAL)
{}

bool ObFetchLogTask::is_valid() const
{
  return id_.is_valid()
    && proposal_id_ > 0
    && OB_INVALID_TIMESTAMP != pre_log_ts_
    && start_lsn_.is_valid()
    && cur_lsn_.is_valid()
    && end_lsn_.is_valid()
    && end_lsn_ > start_lsn_;
}

int ObFetchLogTask::update_cur_lsn_ts(const palf::LSN &lsn, const int64_t max_submit_ts, const int64_t max_fetch_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! lsn.is_valid() || OB_INVALID_TIMESTAMP == max_fetch_ts)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    cur_lsn_ = lsn;
    max_submit_log_ts_ = std::max(max_submit_ts, max_submit_log_ts_);
    max_fetch_log_ts_ = std::max(max_fetch_ts, max_fetch_log_ts_);
    if (cur_lsn_ >= end_lsn_) {
      status_ = Status::FINISH;
    }
    if (max_submit_ts != max_fetch_ts) {
      status_ = Status::TO_END;
    }
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
