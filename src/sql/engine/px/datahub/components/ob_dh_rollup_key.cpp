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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

OB_SERIALIZE_MEMBER(ObRollupNDVInfo, ndv_, n_keys_, dop_, max_keys_);
OB_SERIALIZE_MEMBER((ObRollupKeyPieceMsg, ObDatahubPieceMsg), rollup_ndv_);
OB_SERIALIZE_MEMBER((ObRollupKeyWholeMsg, ObDatahubWholeMsg), rollup_ndv_);

int ObRollupKeyPieceMsgListener::on_message(
    ObRollupKeyPieceMsgCtx &ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObRollupKeyPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  if (pkt.op_id_ != ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(pkt), K(ctx));
  } else if (ctx.received_ >= ctx.task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not receive any more pkt. already get all pkt expected",
             K(pkt), K(ctx));
  } else if (OB_FAIL(ctx.received_msgs_.push_back(pkt))) {
    LOG_WARN("failed to push back pkt", K(pkt), K(ret));
  }
  if (OB_SUCC(ret)) {
     ctx.received_++;
    LOG_TRACE("got a win buf picece msg", "all_got", ctx.received_, "expected", ctx.task_cnt_);
  }
  if (OB_SUCC(ret) && ctx.received_ == ctx.task_cnt_) {
    if (OB_FAIL(ctx.process_ndv())) {
      LOG_WARN("failed to process ndv", K(ret));
    } else if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
      LOG_WARN("fail to send whole msg", K(ret));
    }
    IGNORE_RETURN ctx.reset_resource();
  }
  return ret;
}

// find keys that ndv >> dop
int ObRollupKeyPieceMsgCtx::process_ndv()
{
  int ret = OB_SUCCESS;
  // analyze all rollup keys and get optimal keys that make the data evenly distributed
  int64_t dop = 0;
  ObRollupNDVInfo optimal_rollup_ndv;
  ObRollupNDVInfo max_rollup_ndv;
  optimal_rollup_ndv.n_keys_ = INT64_MAX;
  max_rollup_ndv.n_keys_ = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < received_msgs_.count(); ++i) {
    ObRollupNDVInfo &rollup_ndv = received_msgs_.at(i).rollup_ndv_;
    if (0 == dop) {
      dop = rollup_ndv.dop_;
    } else if (dop != rollup_ndv.dop_) {
      LOG_WARN("unexpected status: dop is not match", K(dop), K(rollup_ndv.dop_));
    }
    if (rollup_ndv.ndv_ >= rollup_ndv.dop_ * FAR_GREATER_THAN_RATIO &&
        optimal_rollup_ndv.n_keys_ > rollup_ndv.n_keys_) {
      optimal_rollup_ndv.n_keys_ = rollup_ndv.n_keys_;
      optimal_rollup_ndv.ndv_ = rollup_ndv.ndv_;
      optimal_rollup_ndv.dop_ = rollup_ndv.dop_;
      optimal_rollup_ndv.max_keys_ = rollup_ndv.max_keys_;
    }
    // set max
    if (max_rollup_ndv.n_keys_ < rollup_ndv.n_keys_) {
      max_rollup_ndv.n_keys_ = rollup_ndv.n_keys_;
    }
    if (max_rollup_ndv.ndv_ < rollup_ndv.ndv_) {
      max_rollup_ndv.ndv_ = rollup_ndv.ndv_;
    }
    if (max_rollup_ndv.dop_ < rollup_ndv.dop_) {
      max_rollup_ndv.dop_ = rollup_ndv.dop_;
    }
    if (max_rollup_ndv.max_keys_ < rollup_ndv.max_keys_) {
      max_rollup_ndv.max_keys_ = rollup_ndv.max_keys_;
    }
  }
  if (INT64_MAX == optimal_rollup_ndv.n_keys_) {
    // can't found ndv that ndv >> dop
    optimal_rollup_ndv = max_rollup_ndv;
  }
  if (0 == optimal_rollup_ndv.n_keys_) {
    // it may has no data
    optimal_rollup_ndv = max_rollup_ndv;
  }
  whole_msg_.rollup_ndv_ = optimal_rollup_ndv;
  if (OB_SUCC(ret)) {
    // set partial rollup keys
    ret = OB_E(EventTable::EN_ROLLUP_ADAPTIVE_KEY_NUM) ret;
    if (OB_FAIL(ret)) {
      whole_msg_.rollup_ndv_.n_keys_ = (-ret);
    }
    ret = OB_SUCCESS;
  }
  // FIXME: now use max_keys
  // three stage only use max_keys
  if (0 < max_rollup_ndv.max_keys_) {
    whole_msg_.rollup_ndv_.n_keys_ = max_rollup_ndv.max_keys_;
  }
  return ret;
}

int ObRollupKeyPieceMsgCtx::alloc_piece_msg_ctx(const ObRollupKeyPieceMsg &pkt,
                                                ObPxCoordInfo &,
                                                ObExecContext &ctx,
                                                int64_t task_cnt,
                                                ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_my_session()) ||
      OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null or physical plan ctx is null", K(ret));
  } else {
    void *buf = ctx.get_allocator().alloc(sizeof(ObRollupKeyPieceMsgCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      msg_ctx = new (buf) ObRollupKeyPieceMsgCtx(pkt.op_id_, task_cnt,
          ctx.get_physical_plan_ctx()->get_timeout_timestamp(),
          ctx.get_my_session()->get_effective_tenant_id());
    }
  }
  return ret;
}

int ObRollupKeyPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  // all piece msg has been received
  whole_msg_.op_id_ = op_id_;
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    dtl::ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expected", K(ret));
    } else if (OB_FAIL(ch->send(whole_msg_, timeout_ts_))) {
      LOG_WARN("fail push data to channel", K(ret));
    } else if (OB_FAIL(ch->flush(true, false))) {
      LOG_WARN("fail flush dtl data", K(ret));
    } else {
      LOG_DEBUG("dispatched winbuf whole msg",
                K(idx), K(cnt), K(whole_msg_), K(*ch));
    }
    if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
      LOG_WARN("failed to wait response", K(ret));
    }
  }
  return ret;
}

void ObRollupKeyPieceMsgCtx::reset_resource()
{
  received_ = 0;
}

int ObRollupKeyWholeMsg::assign(const ObRollupKeyWholeMsg &other)
{
  int ret = OB_SUCCESS;
  rollup_ndv_ = other.rollup_ndv_;
  return ret;
}
