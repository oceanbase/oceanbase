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
using namespace palf;
namespace logservice
{
ObFetchLogTask::ObFetchLogTask(const share::ObLSID &id,
    const SCN &pre_scn,
    const palf::LSN &lsn,
    const int64_t size,
    const int64_t proposal_id) :
  id_(id),
  proposal_id_(proposal_id),
  start_lsn_(lsn),
  cur_lsn_(lsn),
  end_lsn_(lsn + size),
  max_fetch_scn_(),
  max_submit_scn_(),
  status_(Status::NORMAL)
{
  pre_scn_ = pre_scn;
}

bool ObFetchLogTask::is_valid() const
{
  return id_.is_valid()
    && proposal_id_ > 0
    && pre_scn_.is_valid()
    && start_lsn_.is_valid()
    && cur_lsn_.is_valid()
    && end_lsn_.is_valid()
    && end_lsn_ > start_lsn_;
}

int ObFetchLogTask::update_cur_lsn_scn(const palf::LSN &lsn, const SCN &max_submit_scn, const SCN &max_fetch_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! lsn.is_valid() || ! max_fetch_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    cur_lsn_ = lsn;
    max_submit_scn_ = max_submit_scn > max_submit_scn_ ? max_submit_scn : max_submit_scn_;
    max_fetch_scn_ = max_fetch_scn > max_fetch_scn_ ? max_fetch_scn: max_fetch_scn_;
    if (cur_lsn_ >= end_lsn_) {
      status_ = Status::FINISH;
    }
    if (max_submit_scn != max_fetch_scn) {
      status_ = Status::TO_END;
    }
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
