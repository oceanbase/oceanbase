/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OB_HOTSPOT_GROUP_COMMIT

#include "storage/tx/ob_tx_hotspot_helper.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_msg.h"

namespace oceanbase
{
namespace transaction
{

void ObPartTransCtx::clean_hotspot_redo_cache()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hotspot_redo_cache_.reuse())) {
    TRANS_LOG(WARN, "CE hotspot cleanup failed", K(ret), K(trans_id_), K(ls_id_));
  }
}

void ObPartTransCtx::reset_hotspot_redo_status()
{
  CtxLockGuard guard(lock_);
  reset_hotspot_redo_status_();
}

void ObPartTransCtx::reset_hotspot_redo_status_()
{
  if (has_completed_lifecycle(redo_flush_status_)) {
    TRANS_LOG(DEBUG, "ignore reset on completed redo status in CE",
              "redo_flush_status", to_cstr(redo_flush_status_), KPC(this));
  } else if (is_normal_status(redo_flush_status_)
             || redo_flush_status_ == TxRedoFlushStatus::PRIMARY_PREPARING
             || redo_flush_status_ == TxRedoFlushStatus::SECONDARY_PREPARING) {
    redo_flush_status_ = TxRedoFlushStatus::NORMAL_START;
    TRANS_LOG(DEBUG, "reset hotspot redo status in CE",
              "redo_flush_status", to_cstr(redo_flush_status_), KPC(this));
  } else {
    TRANS_LOG(DEBUG, "ignore reset on unexpected aggregation status in CE",
              "redo_flush_status", to_cstr(redo_flush_status_), KPC(this));
  }
}

int ObPartTransCtx::transit_redo_status_as_normal_(TxRedoFlushStatus to)
{
  OB_ASSERT(lock_.is_locked_by_self());
  int ret = OB_SUCCESS;
  if (!is_normal_status(redo_flush_status_)) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "role mismatch: current status is not normal",
              "from", to_cstr(redo_flush_status_), "to", to_cstr(to),
              K(trans_id_), K(ls_id_));
  } else if (!is_normal_status(to)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "target is not a normal status", "to", to_cstr(to),
              K(trans_id_), K(ls_id_));
  } else if (OB_FAIL(is_valid_transition(redo_flush_status_, to))) {
    TRANS_LOG(WARN, "invalid normal transition",
              K(ret), "from", to_cstr(redo_flush_status_), "to", to_cstr(to),
              K(trans_id_), K(ls_id_));
  } else {
    redo_flush_status_ = to;
  }
  return ret;
}

int ObPartTransCtx::wait_hotspot_redo_frozen_flushed(const uint32_t freeze_clock,
                                                     bool &submitted_primary_log)
{
  UNUSED(freeze_clock);
  submitted_primary_log = false;
  return OB_SUCCESS;
}

int ObPartTransCtx::apply_others_hotspot_redo_(ObTxLogCb *invoke_log_cb, const bool sync_result)
{
  UNUSED(invoke_log_cb);
  UNUSED(sync_result);
  return OB_NOT_SUPPORTED;
}

int ObPartTransCtx::release_hotspot_redo_cb_(ObTxLogCb *apply_log_cb, const bool sync_result)
{
  UNUSED(apply_log_cb);
  UNUSED(sync_result);
  return OB_NOT_SUPPORTED;
}

bool ObPartTransCtx::is_in_active_aggregation_() const
{
  return false;
}

bool ObPartTransCtx::can_not_submit_log_for_hotspot_() const
{
  int ret = OB_SUCCESS;
  bool can_not_submit = false;
  if (!is_normal_status(redo_flush_status_)) {
    TRANS_LOG(WARN, "unexpected hotspot redo status in CE, do not block log submit",
              "redo_flush_status", to_cstr(redo_flush_status_), K(trans_id_), K(ls_id_));
  }
  return can_not_submit;
}

int ObPartTransCtx::handle_primary_aggregation_for_forcedly_(bool &secondaries_aborted)
{
  secondaries_aborted = false;
  return OB_SUCCESS;
}

int ObPartTransCtx::wait_for_primary_aggregation_gracefully_()
{
  return OB_SUCCESS;
}

} // namespace transaction
} // namespace oceanbase

#endif // OB_HOTSPOT_GROUP_COMMIT
