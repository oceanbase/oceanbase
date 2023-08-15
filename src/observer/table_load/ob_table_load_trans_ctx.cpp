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

#include "observer/table_load/ob_table_load_trans_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace table;

ObTableLoadTransCtx::ObTableLoadTransCtx(ObTableLoadTableCtx *ctx,
                                         const ObTableLoadTransId &trans_id)
  : ctx_(ctx),
    trans_id_(trans_id),
    allocator_("TLD_TCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx->param_.tenant_id_),
    trans_status_(ObTableLoadTransStatusType::NONE),
    error_code_(OB_SUCCESS)
{
}

int ObTableLoadTransCtx::advance_trans_status(ObTableLoadTransStatusType trans_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLoadTransStatusType::ERROR == trans_status ||
                  ObTableLoadTransStatusType::ABORT == trans_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(trans_status));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    if (OB_UNLIKELY(ObTableLoadTransStatusType::ERROR == trans_status_)) {
      ret = error_code_;
      LOG_WARN("trans has error", KR(ret));
    } else if (OB_UNLIKELY(ObTableLoadTransStatusType::ABORT == trans_status_)) {
      ret = OB_TRANS_KILLED;
      LOG_WARN("trans is abort", KR(ret));
    }
    // 正常运行阶段, 状态是一步步推进的
    else if (OB_UNLIKELY(static_cast<int64_t>(trans_status) !=
                         static_cast<int64_t>(trans_status_) + 1)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("unexpected trans status", KR(ret), K(trans_status), K(trans_status_));
    } else {
      trans_status_ = trans_status;
    }
  }
  return ret;
}

int ObTableLoadTransCtx::set_trans_status_error(int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS == error_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(error_code));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    if (OB_UNLIKELY(trans_status_ == ObTableLoadTransStatusType::ABORT)) {
      ret = OB_TRANS_KILLED;
    } else if (trans_status_ != ObTableLoadTransStatusType::ERROR) {
      trans_status_ = ObTableLoadTransStatusType::ERROR;
      error_code_ = error_code;
    }
  }
  return ret;
}

int ObTableLoadTransCtx::set_trans_status_abort()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(rwlock_);
  trans_status_ = ObTableLoadTransStatusType::ABORT;
  return ret;
}

int ObTableLoadTransCtx::check_trans_status(ObTableLoadTransStatusType trans_status) const
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(trans_status != trans_status_)) {
    if (ObTableLoadTransStatusType::ERROR == trans_status_) {
      ret = error_code_;
    } else if (ObTableLoadTransStatusType::ABORT == trans_status_) {
      ret = OB_TRANS_KILLED;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
