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

using namespace oceanbase::sql;
using namespace oceanbase::common;

OB_SERIALIZE_MEMBER((ObBarrierPieceMsg, ObDatahubPieceMsg));
OB_SERIALIZE_MEMBER((ObBarrierWholeMsg, ObDatahubWholeMsg), ready_state_);

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObBarrierPieceMsgListener::on_message(
    ObBarrierPieceMsgCtx &ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObBarrierPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  if (pkt.op_id_ != ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(pkt), K(ctx));
  } else if (ctx.received_ >= ctx.task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not receive any more pkt. already get all pkt expected",
             K(pkt), K(ctx));
  } else {
    ctx.received_++;
    LOG_DEBUG("got a barrier picece msg", "all_got", ctx.received_, "expected", ctx.task_cnt_);
  }

  // 已经收到所有 piece，发送 sqc  个 whole
  // 各个 sqc 广播给各自 task
  if (OB_SUCC(ret) && ctx.received_ == ctx.task_cnt_) {
    if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
      LOG_WARN("fail to send whole msg", K(ret));
    }
    IGNORE_RETURN ctx.reset_resource();
  }
  return ret;
}

int ObBarrierPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  ObBarrierWholeMsg whole;
  whole.op_id_ = op_id_;
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    dtl::ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expected", K(ret));
    } else if (OB_FAIL(ch->send(whole, timeout_ts_))) {
      LOG_WARN("fail push data to channel", K(ret));
    } else if (OB_FAIL(ch->flush(true, false))) {
      LOG_WARN("fail flush dtl data", K(ret));
    } else {
      LOG_DEBUG("dispatched barrier whole msg",
                K(idx), K(cnt), K(whole), K(*ch));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
    LOG_WARN("failed to wait response", K(ret));
  }
  return ret;
}

void ObBarrierPieceMsgCtx::reset_resource()
{
  received_ = 0;
}

int ObBarrierPieceMsgCtx::alloc_piece_msg_ctx(const ObBarrierPieceMsg &pkt,
                                              ObPxCoordInfo &,
                                              ObExecContext &ctx,
                                              int64_t task_cnt,
                                              ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  void *buf = ctx.get_allocator().alloc(sizeof(ObBarrierPieceMsgCtx));
  if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    msg_ctx = new (buf) ObBarrierPieceMsgCtx(pkt.op_id_, task_cnt,
        ctx.get_physical_plan_ctx()->get_timeout_timestamp());
  }
  return ret;
}
