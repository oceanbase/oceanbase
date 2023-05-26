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

#include "ob_tx_elr_handler.h"
#include "common/ob_clock_generator.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_service.h"

namespace oceanbase
{
namespace transaction
{

void ObTxELRHandler::reset()
{
  elr_prepared_state_ = TxELRState::ELR_INIT;
  mt_ctx_ = NULL;
}

int ObTxELRHandler::check_and_early_lock_release(bool has_row_updated, ObPartTransCtx *ctx)
{
  int ret = OB_SUCCESS;

  if (!ctx->is_can_elr()) {
    // do nothing
  } else {
    ctx->trans_service_->get_tx_version_mgr().update_max_commit_ts(ctx->ctx_tx_data_.get_commit_version(), true);
    if (has_row_updated) {
      if (OB_FAIL(ctx->acquire_ctx_ref())) {
        TRANS_LOG(WARN, "get trans ctx error", K(ret), K(*this));
      } else {
        set_elr_prepared();
      }
    } else {
      // no need to release lock after submit log
      mt_ctx_ = NULL;
    }
  }
  return ret;
}

} //transaction
} //oceanbase
