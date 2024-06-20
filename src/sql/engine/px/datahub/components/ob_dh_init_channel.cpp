/**
 * Copyright (c) 2023 OceanBase
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
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
int ObInitChannelPieceMsgListener::on_message(
    ObInitChannelPieceMsgCtx &ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObInitChannelPieceMsg &pkt)
{
  UNUSED(sqcs);
  int ret = OB_SUCCESS;
  if (pkt.op_id_ != ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(pkt), K(ctx));
  } else if (ctx.received_ >= ctx.task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not receive any more pkt. already get all pkt expected",
             K(pkt), K(ctx));
  }
  if (OB_SUCC(ret)) {
    ctx.received_ += pkt.piece_count_;
    LOG_TRACE("got a init channel picece msg", K(ctx.received_), K(ctx.task_cnt_), K(pkt), K(pkt.piece_count_));
  }
  // have received all piece from px receive
  // send whole msg to px transmit
  if (OB_SUCC(ret) && ctx.received_ == ctx.task_cnt_) {
    if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
      LOG_WARN("fail to send whole msg", K(ret));
    }
    IGNORE_RETURN ctx.reset_resource();
  }
  return ret;
}
int ObInitChannelPieceMsgCtx::alloc_piece_msg_ctx(const ObInitChannelPieceMsg &pkt,
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
    void *buf = ctx.get_allocator().alloc(sizeof(ObInitChannelPieceMsgCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      msg_ctx = new (buf) ObInitChannelPieceMsgCtx(pkt.op_id_, task_cnt,
          ctx.get_physical_plan_ctx()->get_timeout_timestamp(),
          ctx.get_my_session()->get_effective_tenant_id());
    }
  }
  return ret;
}
OB_SERIALIZE_MEMBER((ObInitChannelPieceMsg, ObDatahubPieceMsg), piece_count_);
OB_SERIALIZE_MEMBER((ObInitChannelWholeMsg, ObDatahubWholeMsg), ready_state_);
int ObInitChannelWholeMsg::assign(const ObInitChannelWholeMsg &other, common::ObIAllocator *allocator)
 {
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  ready_state_ = other.ready_state_;
  return ret;
}


int ObInitChannelPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  // get child transmit op's id.
  // in current impl. the paired transmit op id = receive op id + 1
  //TODO :
  whole_msg_.op_id_ = op_id_ + 1;
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
  }
  if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
    LOG_WARN("failed to wait response", K(ret));
  }
  return ret;
}

void ObInitChannelPieceMsgCtx::reset_resource()
{
  whole_msg_.reset();
  received_ = 0;
}
