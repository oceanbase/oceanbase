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
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/px/datahub/components/ob_dh_second_stage_reporting_wf.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

#define UNLIMITED_MEM 0


// ObReportingWFPieceMsg functions begin
OB_DEF_SERIALIZE_SIZE(ObReportingWFPieceMsg)
{
  int64_t len = ObReportingWFPieceMsgBase::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, pby_hash_value_array_);
  return len;
}

OB_DEF_SERIALIZE(ObReportingWFPieceMsg)
{
  int ret = ObReportingWFPieceMsgBase::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, pby_hash_value_array_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObReportingWFPieceMsg)
{
  int ret = ObReportingWFPieceMsgBase::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, pby_hash_value_array_);
  }
  return ret;
}
// ObReportingWFPieceMsg functions end


// ObReportingWFWholeMsg functions begin
int ObReportingWFWholeMsg::assign(const ObReportingWFWholeMsg &other)
 {
  int ret = OB_SUCCESS;
  if (OB_FAIL(pby_hash_value_array_.reserve(other.pby_hash_value_array_.count()))) {
    LOG_WARN("reserve pby_hash_value_array failed", K(ret), K(other.pby_hash_value_array_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.pby_hash_value_array_.count(); ++i) {
    if (OB_FAIL(pby_hash_value_array_.push_back(other.pby_hash_value_array_.at(i)))) {
      LOG_WARN("push back partition range failed", K(ret), K(other.pby_hash_value_array_.count()),
               K(i), K(other.pby_hash_value_array_.at(i)));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObReportingWFWholeMsg)
{
  int64_t len = ObReportingWFWholeMsgBase::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, pby_hash_value_array_);
  return len;
}

OB_DEF_SERIALIZE(ObReportingWFWholeMsg)
{
  int ret = ObReportingWFWholeMsgBase::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, pby_hash_value_array_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObReportingWFWholeMsg)
{
  int ret = ObReportingWFWholeMsgBase::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, pby_hash_value_array_);
  }
  return ret;
}
// ObReportingWFWholeMsg functions end


int ObReportingWFPieceMsgCtx::alloc_piece_msg_ctx(const ObReportingWFPieceMsg &pkt,
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
    void *buf = ctx.get_allocator().alloc(sizeof(ObReportingWFPieceMsgCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      msg_ctx = new (buf) ObReportingWFPieceMsgCtx(pkt.op_id_, task_cnt,
          ctx.get_physical_plan_ctx()->get_timeout_timestamp(),
          ctx.get_my_session()->get_effective_tenant_id());
    }
  }
  return ret;
}

int ObReportingWFPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  // datahub already received all pieces, will send whole msg to sqc
  whole_msg_.op_id_ = op_id_;
  // no need to sort here, will use pby_hash_value_array_ to build hash map later
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    dtl::ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expected", K(ret));
    } else if (OB_FAIL(ch->send(whole_msg_, timeout_ts_))) {
      LOG_WARN("fail push data to channel", K(ret));
    } else if (OB_FAIL(ch->flush(true /* wait */, false /* wait response */))) {
      LOG_WARN("fail flush dtl data", K(ret));
    } else {
      LOG_DEBUG("dispatched winbuf whole msg", K(idx), K(cnt), K(whole_msg_), K(*ch));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
    LOG_WARN("failed to wait response", K(ret));
  }
  return ret;
}

void ObReportingWFPieceMsgCtx::reset_resource()
{
  whole_msg_.reset();
  received_ = 0;
}

int ObReportingWFPieceMsgListener::on_message(
    ObReportingWFPieceMsgCtx &ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObReportingWFPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  if (pkt.op_id_ != ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(pkt), K(ctx));
  } else if (ctx.received_ >= ctx.task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not receive any more pkt. already get all pkt expected",
             K(ret), K(pkt), K(ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkt.pby_hash_value_array_.count(); ++i) {
      if (OB_FAIL(ctx.whole_msg_.pby_hash_value_array_.push_back(
                  pkt.pby_hash_value_array_.at(i)))) {
        LOG_WARN("push back pby_hash_value_array_ failed", K(ret),
                 K(pkt.pby_hash_value_array_.count()), K(i), K(pkt.pby_hash_value_array_.at(i)));
      }
    }
  }
  if (OB_SUCC(ret)) {
     ++ctx.received_;
    LOG_TRACE("got a win buf picece msg", "all_got", ctx.received_, "expected", ctx.task_cnt_);
  }
  if (OB_SUCC(ret) && ctx.received_ == ctx.task_cnt_) {
    if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
      LOG_WARN("fail to send whole msg", K(ret));
    }
    IGNORE_RETURN ctx.reset_resource();
  }
  return ret;
}
// ObWinbufPieceMsgListener functions end
