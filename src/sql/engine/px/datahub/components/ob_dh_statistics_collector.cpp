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
#include "sql/engine/px/datahub/components/ob_dh_statistics_collector.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

OB_SERIALIZE_MEMBER((ObStatisticsCollectorPieceMsg, ObDatahubPieceMsg), use_hash_join_);
OB_SERIALIZE_MEMBER((ObStatisticsCollectorWholeMsg, ObDatahubWholeMsg), use_hash_join_);

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObStatisticsCollectorPieceMsgListener::on_message(
    ObStatisticsCollectorPieceMsgCtx &ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObStatisticsCollectorPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  if (pkt.op_id_ != ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(pkt), K(ctx));
  } else {
    if (pkt.use_hash_join_) {
      ctx.use_hash_join_ = pkt.use_hash_join_;
    }
    ++ctx.received_cnt_;
    LOG_DEBUG("got a statistics collector picece msg",
              K(pkt.use_hash_join_), K(ctx.use_hash_join_),
              K(ctx.received_cnt_));
    if (ctx.received_cnt_ == ctx.task_cnt_) {
      if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
        LOG_WARN("fail to send whole msg", K(ret));
      }
      IGNORE_RETURN ctx.reset_resource();
    }
  }
  return ret;
}

int ObStatisticsCollectorPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  ObStatisticsCollectorWholeMsg whole;
  whole.op_id_ = op_id_;
  whole.use_hash_join_ = use_hash_join_;
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

void ObStatisticsCollectorPieceMsgCtx::reset_resource()
{
  received_cnt_ = 0;
  use_hash_join_ = false;
}

int ObStatisticsCollectorPieceMsgCtx::alloc_piece_msg_ctx(const ObStatisticsCollectorPieceMsg &pkt,
                                              ObPxCoordInfo &,
                                              ObExecContext &ctx,
                                              int64_t task_cnt,
                                              ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  void *buf = ctx.get_allocator().alloc(sizeof(ObStatisticsCollectorPieceMsgCtx));
  if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("buf is null", K(ret));
  } else {
    msg_ctx = new (buf) ObStatisticsCollectorPieceMsgCtx(pkt.op_id_, task_cnt,
        ctx.get_physical_plan_ctx()->get_timeout_timestamp());
  }
  return ret;
}
